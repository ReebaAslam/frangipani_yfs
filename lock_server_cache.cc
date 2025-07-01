// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
  revoke_cv = new pthread_cond_t;
  pthread_cond_init(revoke_cv, NULL);
  retry_cv = new pthread_cond_t;
  pthread_cond_init(retry_cv, NULL);
  pthread_mutex_init(&lock_mutex, NULL);
  pthread_mutex_init(&retry_mutex, NULL);
  pthread_mutex_init(&revoke_mutex, NULL);
  pthread_mutex_init(&connections_mutex, NULL);

}

void
lock_server_cache::revoker()
{
  while (true) {
    pthread_mutex_lock(&revoke_mutex);

    while (revokes.empty()) {
      printf("revoker thread waiting for revokes\n");
      pthread_cond_wait(revoke_cv, &revoke_mutex);
    }
    printf("revoker thread woke up\n");
    auto it = revokes.begin();
    while (it != revokes.end()) {
      lock_protocol::lockid_t lid = it->first;
      std::string clt = it->second;
      rpcc* cl;
      get_client_connection(clt, cl);
      // Unlock before doing RPC
      pthread_mutex_unlock(&revoke_mutex);
      int r;
      printf("server is sending revoke signal to client %s for lock %016llx\n", clt.c_str(), lid);
      int ret = cl->call(rlock_protocol::revoke, lid, r);
      // Re-lock to update shared state
      pthread_mutex_lock(&revoke_mutex);
      if (ret == rlock_protocol::OK) {
        printf("revoke signal sent to client %s for lock %016llx successfully\n", clt.c_str(), lid);
        it = revokes.erase(it);
      } else {
        printf("failed to send revoke signal to client %s for lock %016llx, keeping it in revokes list\n", clt.c_str(), lid);
        ++it;  // Keep it for retry
      }
    }
    pthread_mutex_unlock(&revoke_mutex);
  }
}

void lock_server_cache::get_client_connection(std::string &clt, rpcc *&cl)
{
  pthread_mutex_lock(&connections_mutex);
  if (client_connections.count(clt) == 0)
  {
    sockaddr_in dstsock;
    make_sockaddr(clt.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    cl->bind(); // Optional, some implementations auto-bind on call()
    client_connections[clt] = cl;
  }
  else
  {
    cl = client_connections[clt];
  }
  pthread_mutex_unlock(&connections_mutex);
}

void
lock_server_cache::retryer()
{
  while (true) {
    // printf("retryer thread started\n");
    // iterate over the locks and for the ones that are free check if there are any waiting clients
    // signal the first one in the list to send an acquire RPC
    pthread_mutex_lock(&retry_mutex);
    while (free_locks.empty()) {
      pthread_cond_wait(retry_cv, &retry_mutex);
    }

    // printf("retryer thread woke up\n");
    auto it = free_locks.begin();
    while (it != free_locks.end()) {
      lock_protocol::lockid_t lid = *it;
      lock_info &li = locks[lid];

      // printf("retryer thread checking lock %016llx, state: %s, retryer_sent_to: %s\n", lid, li.state == ACQUIRED ? "ACQUIRED" : "FREE", li.retryer_sent_to.c_str());
      if (!li.waiting_clients.empty()) {
        std::string clt = li.waiting_clients.front();        
        rpcc* cl;
        get_client_connection(clt, cl);
        pthread_mutex_unlock(&retry_mutex);
        int r;
        printf("server is sending retry signal to client %s for lock %016llx\n", clt.c_str(), lid);
        int ret = cl -> call(rlock_protocol::retry, lid, r);
        if (ret == lock_protocol::OK) {
          printf("server sent retry signal to client %s for lock %016llx\n", clt.c_str(), lid);
          pthread_mutex_lock(&retry_mutex);
          free_locks.remove(lid); // remove from free locks list if no more waiting clients
        } else {
          printf("server failed to send retry signal to client %s for lock %016llx, keeping it in free locks list\n", clt.c_str(), lid);
        }
        ++it;
      }
    }
    pthread_mutex_unlock(&retry_mutex);
  }
}


lock_protocol::status
lock_server_cache::acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&lock_mutex);
  printf("client %s is requesting to acquire lock %016llx\n", clt_id.c_str(), lid);

  // if the lock is not in the map, create a new lock_info object and grant it to the client
  lock_info &li = locks[lid];
  if (li.state == FREE){
    printf("lock %016llx is free, granting to client %s\n", lid, clt_id.c_str());
    li.state = ACQUIRED;
    li.clt_id = clt_id;
    li.seq_num = seq_num;
    //remove from waiting clients if exists
    li.waiting_clients.remove(clt_id);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::OK; // Already owned by the same client

  }
  else{
    if (li.clt_id == clt_id) {
      printf("client %s is trying to acquire lock %016llx but it already owns it\n", clt_id.c_str(), lid);
      pthread_mutex_unlock(&lock_mutex);
      return lock_protocol::OK; // Already owned by the same client
    }
    else{
      printf("lock %016llx is already acquired by client %s, adding client %s to waiting list and adding to revoke queue\n", lid, li.clt_id.c_str(), clt_id.c_str());
      li.waiting_clients.push_back(clt_id);
      li.seq_num = seq_num; // update the sequence number for the waiting client
      pthread_mutex_lock(&revoke_mutex);
      revokes.push_back(std::make_pair(lid, li.clt_id)); // add to revokes queue
      pthread_mutex_unlock(&revoke_mutex);
      pthread_cond_signal(revoke_cv); // signal the revoker thread
    }
  }
  pthread_mutex_unlock(&lock_mutex);
  r = lock_protocol::RETRY;
  return lock_protocol::RETRY;
}

lock_protocol::status
lock_server_cache::release(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &)
{
  printf("client %s is requesting to release lock %016llx\n", clt_id.c_str(), lid);
  pthread_mutex_lock(&lock_mutex);
  lock_info &li = locks[lid];
  if (li.state == FREE) {
    printf("lock %016llx is already free, nothing to release\n", lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Lock is already free
  }

  if (li.state != ACQUIRED) {
    printf("lock %016llx is not acquired, cannot release\n", lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Lock is not acquired
  }

  if (li.clt_id != clt_id) {
    printf("client %s is trying to release lock %016llx but it is owned by client %s\n", clt_id.c_str(), lid, li.clt_id.c_str());
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Not the owner
  }

  if (li.seq_num != seq_num) {
    printf("client %s is trying to release lock %016llx with wrong sequence number %d, expected %d\n", clt_id.c_str(), lid, seq_num, li.seq_num);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Wrong sequence number
  }

  // Mark the lock as free, but do not assign it yet
  li.state = FREE;
  li.clt_id = "";
  li.waiting_clients.remove(clt_id);
  printf("lock %llu is now free\n", lid);
  printf("client %s is releasing lock %016llx\n", clt_id.c_str(), lid);
  
  pthread_mutex_lock(&retry_mutex);
  free_locks.push_back(lid); // Add to free locks list
  pthread_mutex_unlock(&retry_mutex);
  pthread_cond_signal(retry_cv);
  
  pthread_mutex_unlock(&lock_mutex);
  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &)
{
  return lock_protocol::OK;
}

