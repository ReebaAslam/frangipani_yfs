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

lock_server_cache::lock_server_cache(class rsm *_rsm) 
  : rsm (_rsm)
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
  pthread_mutex_init(&revoke_mutex, NULL);
  pthread_mutex_init(&free_locks_mutex, NULL);
  pthread_mutex_init(&client_connections_mutex, NULL);

}

lock_protocol::status
lock_server_cache::acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &r){
    pthread_mutex_lock(&lock_mutex);
    lock_info &li = locks[lid]; // creates if not already present
    client_info clt_info;
    clt_info.clt_id = clt_id;
    clt_info.seq_num = seq_num;

    if (li.state == FREE){
        printf("lock_server_cache::acquire: lock %llu is free, client %s acquiring it with sequence number %d\n", lid, clt_id.c_str(), seq_num);
        li.state = ACQUIRED;
        li.clt_info = clt_info;
        //search waiting clients for this client id and remove it
        auto it = std::remove_if(li.waiting_clients.begin(), li.waiting_clients.end(),
                                 [&clt_info](const client_info &c) {
            return c.clt_id == clt_info.clt_id;
        });
        if (it != li.waiting_clients.end())
            printf("lock_server_cache::acquire: removing client %s from waiting clients for lock %llu\n", clt_id.c_str(), lid);
            li.waiting_clients.erase(it, li.waiting_clients.end()); // Remove all matching clients
        printf("lock_server_cache::acquire: client %s acquired lock %llu\n", clt_id.c_str(), lid);
        r = lock_protocol::OK;
    } else {
        // queue to wait for the lock
        printf("lock_server_cache::acquire: lock %llu is not free, client %s waiting with sequence number %d\n", lid, clt_id.c_str(), seq_num);
        queue_waiting_client(li, clt_info);
        printf("lock_server_cache::acquire: client %s queued for lock %llu\n", clt_id.c_str(), lid);
        // add to revokes list if not already present
        add_lock_to_revokes(lid);
        printf("lock_server_cache::acquire: client %s requesting revoke for lock %llu by adding to queue and signalling thread\n", clt_id.c_str(), lid);
        pthread_cond_signal(revoke_cv);
        r = lock_protocol::RETRY;
    }
    pthread_mutex_unlock(&lock_mutex);
    return r;
}

void lock_server_cache::add_lock_to_revokes(lock_protocol::lockid_t &lid)
{
    pthread_mutex_lock(&revoke_mutex);
    revokes.push_back(lid);
    pthread_mutex_unlock(&revoke_mutex);
}

void lock_server_cache::queue_waiting_client(lock_server_cache::lock_info &li, client_info &clt_info)
{
    // if client is already waiting for this lock, then update sequence number else add to waiting clients
    auto it = std::find_if(li.waiting_clients.begin(), li.waiting_clients.end(),
                           [&clt_info](const client_info &c) {
        return c.clt_id == clt_info.clt_id;
    });
    if (it != li.waiting_clients.end()) {
        // Client is already waiting, update sequence number
        printf("lock_server_cache::queue_waiting_client: client %s is already waiting for lock %llu, updating sequence number from %d to %d\n",
            clt_info.clt_id.c_str(), it->seq_num, clt_info.seq_num);
        it->seq_num = clt_info.seq_num; // Update sequence number
    } else {
        // Client is not waiting, add to waiting clients
        printf("lock_server_cache::queue_waiting_client: client %s is not waiting for lock %llu, adding to waiting clients\n",
            clt_info.clt_id.c_str(), li.clt_info.seq_num);
        li.waiting_clients.push_back(clt_info); // Add to waiting clients
    }
}

void
lock_server_cache::revoker()
{
  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while(1){
    pthread_mutex_lock(&revoke_mutex);
    while (revokes.empty()) {
      printf("lock_server_cache::revoker: thread going to sleep, waiting for revokes\n");
      pthread_cond_wait(revoke_cv, &revoke_mutex);
      printf("lock_server_cache::revoker: thread woke up\n");
    }
    lock_protocol::lockid_t lock_id = revokes.front();
    pthread_mutex_lock(&lock_mutex);
    lock_info &li = locks[lock_id];
    client_info clt_info = li.clt_info;
    lock_state state = li.state;
    bool revoke_sent = li.revoke_sent;
    pthread_mutex_unlock(&lock_mutex);
    if (revoke_sent || state == FREE) {

    //   printf("lock_server_cache::revoker: lock %llu state is %d or revoke already sent, skipping revoke for client %s with sequence number %d\n", 
            //  lock_id, state, clt_info.clt_id.c_str(), clt_info.seq_num);
      pthread_mutex_unlock(&revoke_mutex);
      continue; // Skip if revoke already sent
    }
    printf("lock_server_cache::revoker: processing revoke for lock %llu held by client %s with sequence number %d\n", 
           lock_id, clt_info.clt_id.c_str(), clt_info.seq_num);
    revokes.pop_front();
    li.revoke_sent = true; // Mark revoke as sent
    pthread_mutex_unlock(&revoke_mutex);

    if (rsm->amiprimary()){
        printf("lock_server_cache::revoker: sending revoke signal for lock %llu to client %s with sequence number %d\n", 
            lock_id, clt_info.clt_id.c_str(), clt_info.seq_num);
        rpcc* cl = get_client_connection(clt_info.clt_id);
        int r;  
        int ret = cl->call(rlock_protocol::revoke, lock_id, clt_info.seq_num, r);
        if (ret != rlock_protocol::OK) {
        fprintf(stderr, "lock_server_cache::revoker: failed to send revoke to %s for lock %llu\n", clt_info.clt_id.c_str(), lock_id);
        } else {
        printf("lock_server_cache::revoker: sent revoke to %s for lock %llu\n", clt_info.clt_id.c_str(), lock_id);
        }
    }
  }
}


lock_protocol::status 
lock_server_cache::release(std::string clt_id, int seq_num, lock_protocol::lockid_t lid, int &r){
    pthread_mutex_lock(&lock_mutex);
    printf("lock_server_cache::release: client %s requesting to release lock %llu\n", clt_id.c_str(), lid);
    lock_info &li = locks[lid]; // creates if not already present
    // Check if the client is the one holding the lock
    if (li.clt_info.clt_id != clt_id) {
        printf("lock_server_cache::release: client %s is not the owner of lock %llu\n", clt_id.c_str(), lid);
        pthread_mutex_unlock(&lock_mutex);
        return lock_protocol::RPCERR; // Not the owner
    }
    if (li.clt_info.seq_num != seq_num) {
        printf("lock_server_cache::release: sequence number mismatch for client %s on lock %llu, expected %d, got %d\n", 
               clt_id.c_str(), lid, li.clt_info.seq_num, seq_num);
        pthread_mutex_unlock(&lock_mutex);
        return lock_protocol::RPCERR; // Sequence number mismatch
    }
    if (li.state != ACQUIRED) {
        printf("lock_server_cache::release: lock %llu is not in ACQUIRED state, current state: %d\n", lid, li.state);
        pthread_mutex_unlock(&lock_mutex);
        return lock_protocol::RPCERR; // Lock not in ACQUIRED state
    }

    // Release the lock
    // Notify the waiting clients
    li.state = FREE;
    li.clt_info = client_info(); // Reset client info
    li.revoke_sent = false; // Reset revoke sent flag
    printf("lock_server_cache::release: released lock %llu held by client %s\n", lid, clt_id.c_str());
    
    add_lock_to_free_locks(lid); // Add to free locks list
    pthread_cond_signal(retry_cv); // Wake up any waiting clients

    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::OK;; // Return OK status
}


void lock_server_cache::add_lock_to_free_locks(lock_protocol::lockid_t &lid)
{
    pthread_mutex_lock(&free_locks_mutex);
    bool found = (std::find(free_locks.begin(), free_locks.end(), lid) != free_locks.end());
    if (!found)
    {
        printf("lock_server_cache::add_lock_to_free_locks: adding lock %llu to free locks\n", lid);
        free_locks.push_back(lid);
    }
    pthread_mutex_unlock(&free_locks_mutex);
}


void
lock_server_cache::retryer()
{
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  while(1){
    pthread_mutex_lock(&free_locks_mutex);
    while (free_locks.empty()) {
      pthread_cond_wait(retry_cv, &free_locks_mutex);
        printf("lock_server_cache::retryer: thread woke up\n");
    }
    lock_protocol::lockid_t lock_id = free_locks.front();
    free_locks.pop_front();
    pthread_mutex_unlock(&free_locks_mutex);

    pthread_mutex_lock(&lock_mutex);
    lock_info &li = locks[lock_id];
    std::list<client_info> waiting_clients = li.waiting_clients;
    pthread_mutex_unlock(&lock_mutex);
    if (waiting_clients.empty()) {
        printf("lock_server_cache::retryer: no waiting clients for lock %llu\n", lock_id);
        continue; // No clients waiting for this lock
    }
    
    else if (rsm->amiprimary()) {
        // Notify the first waiting client
        client_info clt_info = waiting_clients.front();
        waiting_clients.pop_front();
        printf("lock_server_cache::retryer: notifying client %s to retry lock %llu\n", clt_info.clt_id.c_str(), lock_id);
        rpcc* cl = get_client_connection(clt_info.clt_id);
        int r;
        int ret = cl->call(rlock_protocol::retry, lock_id, clt_info.seq_num, r);
        if (ret != rlock_protocol::OK) {
            fprintf(stderr, "lock_server_cache::retryer: failed to send retry to %s for lock %llu\n", clt_info.clt_id.c_str(), lock_id);
        }
        else {
            printf("lock_server_cache::retryer: sent retry to %s for lock %llu\n", clt_info.clt_id.c_str(), lock_id);
        }        
    }
  }
}


rpcc* lock_server_cache::get_client_connection(const std::string &clt_id)
{
    pthread_mutex_lock(&client_connections_mutex);
    rpcc* cl;
    if (client_connections.count(clt_id) == 0) {
        sockaddr_in dstsock;
        make_sockaddr(clt_id.c_str(), &dstsock);
        cl = new rpcc(dstsock);
        cl->bind(); 
        client_connections[clt_id] = cl;
    } else {
        cl = client_connections[clt_id];
    }
    pthread_mutex_unlock(&client_connections_mutex);
    return cl;
}
