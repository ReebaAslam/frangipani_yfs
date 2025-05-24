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

}

void
lock_server_cache::revoker()
{
  while (true) {
    pthread_mutex_lock(&lock_mutex);
    while (revokes.empty()) {
      pthread_cond_wait(revoke_cv, &lock_mutex);
    }
    printf("revoker thread woke up\n");
    auto it = revokes.begin();
    while (it != revokes.end()) {
      lock_protocol::lockid_t lid = it->first;
      std::string clt = it->second;

      // Unlock before doing RPC
      pthread_mutex_unlock(&lock_mutex);

      sockaddr_in dstsock;
      make_sockaddr(clt.c_str(), &dstsock);
      rpcc cl(dstsock);
      if (cl.bind() < 0) {
        printf("[server] bind failed to client %s\n", clt.c_str());
      }
      // Send revoke signal to the client
      int r;
      printf("server is sending revoke signal to client %s for lock %016llx\n", clt.c_str(), lid);
      int ret = cl.call(rlock_protocol::revoke, lid, r);

      // Re-lock to update shared state
      pthread_mutex_lock(&lock_mutex);
      if (ret == lock_protocol::OK) {
        it = revokes.erase(it);
      } else {
        ++it;  // Keep it for retry
      }
    }

    pthread_mutex_unlock(&lock_mutex);
  }
}


void
lock_server_cache::retryer()
{
  while (true) {
    pthread_mutex_lock(&lock_mutex);
    // printf("retryer thread started\n");
    // iterate over the locks and for the ones that are free check if there are any waiting clients
    // signal the first one in the list to send an acquire RPC
    while (free_locks.empty()) {
      pthread_cond_wait(retry_cv, &lock_mutex);
    }

    // printf("retryer thread woke up\n");
    auto it = free_locks.begin();
    while (it != free_locks.end()) {
      lock_protocol::lockid_t lid = *it;
      lock_info &li = locks[lid];

      printf("retryer thread checking lock %016llx, state: %s, retryer_sent_to: %s\n", lid, li.state == ACQUIRED ? "ACQUIRED" : "FREE", li.retryer_sent_to.c_str());
      if (!li.waiting_clients.empty() && li.retryer_sent_to.empty()) {
        std::string clt = li.waiting_clients.front();
        li.waiting_clients.pop_front();

        sockaddr_in dstsock;
        make_sockaddr(clt.c_str(), &dstsock);
        pthread_mutex_unlock(&lock_mutex);

        rpcc cl(dstsock);
        if (cl.bind() < 0) {
        printf("[server] bind failed to client %s\n", clt.c_str());
        }
        int r;
        printf("server is sending retry signal to client %s for lock %016llx\n", clt.c_str(), lid);
        int ret = cl.call(rlock_protocol::retry, lid, r);

        pthread_mutex_lock(&lock_mutex);
        li.retryer_sent_to = clt; // update the client id for the retryer
        if (!li.waiting_clients.empty()) {
          // if there are still waiting clients, add back to revokes
          if (std::find(revokes.begin(), revokes.end(), std::make_pair(lid, clt)) == revokes.end()) {
            revokes.push_back(std::make_pair(lid, clt));
            printf("client %s request for retrying lock %016llx is sending revoke signal to the same client because of waiting list\n", clt.c_str(), lid);
            pthread_cond_signal(revoke_cv);
          }
        }

        if (ret == rlock_protocol::OK) {
          it = free_locks.erase(it);  
        } else {
          ++it;
        }
      } else {
        ++it;
      }
    }
    pthread_mutex_unlock(&lock_mutex);
  }
}


lock_protocol::status
lock_server_cache::acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&lock_mutex);
  printf("client %s is requesting to acquire lock %016llx\n", clt_id.c_str(), lid);

  // if the lock is not in the map, create a new lock_info object and grant it to the client
  lock_info &li = locks[lid];

  // if lock is already held by another client:
  // and add the current client to the waiting list
  // add lock and current client id to revokes
  // return lock_protocol::RETRY;
  // do not block the client
  

  if (li.retryer_sent_to != "" && li.retryer_sent_to != clt_id) {
    printf("retry for lock %016llx is already sent to client %s, current client is %s\n", lid, li.retryer_sent_to.c_str(), clt_id.c_str());
    // add client to the waiting list if not already there
    if (std::find(li.waiting_clients.begin(), li.waiting_clients.end(), clt_id) == li.waiting_clients.end()) {
      printf("adding client %s to waiting list for lock %016llx\n", clt_id.c_str(), lid);
      li.waiting_clients.push_back(clt_id); // add to waitlist
    }
    // send revoke signal to the current retryer if not already sent
    if (std::find(revokes.begin(), revokes.end(), std::make_pair(lid, li.clt_id)) == revokes.end()) {
      revokes.push_back(std::make_pair(lid, li.clt_id));
      printf("lock %llu state is %s \n", lid, li.state == ACQUIRED ? "ACQUIRED" : "FREE");
      printf("client %s request for acquiring lock %016llx is sending revoke signal to client %s\n", clt_id.c_str(), lid, li.clt_id.c_str());
      pthread_cond_signal(revoke_cv);
    }
    pthread_mutex_unlock(&lock_mutex);
    r = lock_protocol::RETRY;
    return lock_protocol::RETRY;
  }

  if (li.state == ACQUIRED){
    if (li.retryer_sent_to == clt_id) {
      printf("client %s is the retryer for lock %016llx but lock was already acquired, this seems buggy\n", clt_id.c_str(), lid);
    }
    if (std::find(li.waiting_clients.begin(), li.waiting_clients.end(), clt_id) == li.waiting_clients.end()) {
      printf("adding client %s to waiting list for lock %016llx\n", clt_id.c_str(), lid);
      li.waiting_clients.push_back(clt_id); // add to waitlist
    }

    if (std::find(revokes.begin(), revokes.end(), std::make_pair(lid, li.clt_id)) == revokes.end()) {
      revokes.push_back(std::make_pair(lid, li.clt_id));
      printf("lock %llu state is %s \n", lid, li.state == ACQUIRED ? "ACQUIRED" : "FREE");
      printf("client %s request for acquiring lock %016llx is sending revoke signal to client %s\n", clt_id.c_str(), lid, li.clt_id.c_str());
      pthread_cond_signal(revoke_cv);
    }
    pthread_mutex_unlock(&lock_mutex);
    r = lock_protocol::RETRY;
    return lock_protocol::RETRY;
  }

  // printf("lock %llu is free, granting it to client %d\n", lid, clt);
  li.clt_id = clt_id;
  li.state = ACQUIRED;
  li.retryer_sent_to.clear(); // reset the retryer sent to client id
  if ((std::find(li.waiting_clients.begin(), li.waiting_clients.end(), clt_id) != li.waiting_clients.end())) {
    li.waiting_clients.remove(clt_id); // remove from waiting list if it exists
  }
  printf("client %s is granted lock %016llx\n", clt_id.c_str(), lid);

  pthread_mutex_unlock(&lock_mutex);
  r = lock_protocol::OK;
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::release(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &)
{
  printf("client %s is requesting to release lock %016llx\n", clt_id.c_str(), lid);
  pthread_mutex_lock(&lock_mutex);
  lock_info &li = locks[lid];

  if (li.clt_id != clt_id && li.retryer_sent_to != clt_id) {
    printf("client %s is trying to release lock %016llx but it is not the owner\n", clt_id.c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR;
  }

  // Mark the lock as free, but do not assign it yet
  li.state = FREE;
  li.clt_id = "";
  free_locks.push_back(lid);
  li.retryer_sent_to.clear(); // reset the retryer sent to client id
  printf("lock %llu is now free\n", lid);
  printf("client %s is releasing lock %016llx\n", clt_id.c_str(), lid);
  pthread_cond_signal(retry_cv);
  
  pthread_mutex_unlock(&lock_mutex);
  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &)
{
  return lock_protocol::OK;
}


