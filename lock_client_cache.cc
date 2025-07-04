// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
  releaser_cv = new pthread_cond_t;
  pthread_cond_init(releaser_cv, NULL);
  pthread_mutex_init(&lock_cache_mutex, NULL);
  pthread_mutex_init(&releaser_queue_mutex, NULL);

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    printf("[client] %s thread %ld requesting lock %016llx\n", 
           this->get_id().c_str(), pthread_self(), lid);
    int ret = lock_protocol::OK;
    pthread_mutex_lock(&lock_cache_mutex);
    while(1){
        lock_info &li = lock_cache[lid];
        if (li.state == NONE) {
            // ask the server for the lock
            li.state = ACQUIRING;
            li.sequence_number++;
            pthread_mutex_unlock(&lock_cache_mutex);
            printf("[client] %s thread %ld trying to acquire lock %016llx, sequence number %d\n", 
                     this->get_id().c_str(), pthread_self(), lid, li.sequence_number);
            int r;
            int ret = cl->call(lock_protocol::acquire, this->id, li.sequence_number, lid, r);
            pthread_mutex_lock(&lock_cache_mutex);
            if (ret==lock_protocol::OK) {
                li.state = LOCKED;
                printf("[client] %s thread %ld successfully acquired lock %016llx\n", 
                       this->get_id().c_str(), pthread_self(), lid);
                break;
            } else {
                li.state = NONE; // Set to NONE to allow retrying
                printf("[client] %s thread %ld failed to acquire lock %016llx, retrying...\n", 
                       this->get_id().c_str(), pthread_self(), lid);
            }
        }
        else if (li.state == FREE){
            li.state = LOCKED;
            printf("[client] %s lock %016llx is free, thread %ld acquiring it\n", 
                   this->get_id().c_str(), lid, pthread_self());
            break;
        }
        else{
            printf("[client] %s lock %016llx is not available\n", this->get_id().c_str(), lid);
        }
        printf("[client] %s putting thread %ld to wait for lock %016llx\n", this->get_id().c_str(), pthread_self(), lid);
        if (li.waiting_threads.empty() || li.waiting_threads.back() != pthread_self()) {
            // Only add the thread if it's not already waiting
            li.waiting_threads.push_back(pthread_self());
        }
        pthread_cond_wait(li.cond, &lock_cache_mutex);
        printf("[client] %s thread %ld woke up, checking lock %016llx state\n", this->get_id().c_str(), pthread_self(), lid);
    }
    pthread_mutex_unlock(&lock_cache_mutex);
    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  printf("[client] %s request to release lock %016llx\n", this->get_id().c_str(), lid);
  pthread_mutex_lock(&lock_cache_mutex);
  lock_info &li = lock_cache[lid];
  // Check if the lock is in a releasable state
  if (li.state != LOCKED) {
    return lock_protocol::NOENT; // Cannot release a lock that is not locked
  }
  li.state = FREE; // Set the lock state to FREE
  // trigger releaser thread to release the lock if needed
  printf("[client] %s lock state set to free %016llx\n", this->get_id().c_str(), lid);
  
  pthread_mutex_lock(&releaser_queue_mutex);
  bool found = (std::find(releaser_queue.begin(), releaser_queue.end(), lid) != releaser_queue.end());
  pthread_mutex_unlock(&releaser_queue_mutex);

  // if lid in releaser queue, signal releaser cv, otherwise broadcast
  if (!found) {
    printf("[client] %s lock %016llx is not in releaser queue, broadcasting condition\n", this->get_id().c_str(), lid);
    pthread_cond_broadcast(li.cond);
  } 
  else{
    printf("[client] %s lock %016llx is in releaser queue, signalling releaser\n", this->get_id().c_str(), lid);
    pthread_cond_signal(releaser_cv); // Notify the releaser thread
  }
  pthread_mutex_unlock(&lock_cache_mutex);
  return lock_protocol::OK;
}


void lock_client_cache::releaser()
{
    while(1){
        pthread_mutex_lock(&releaser_queue_mutex);
        while (releaser_queue.empty()) {
            printf("[client] %s releaser thread going to sleep\n", this->id.c_str());
            pthread_cond_wait(releaser_cv, &releaser_queue_mutex);
            printf("[client] %s releaser thread woke up, checking for locks to release\n", this->id.c_str());
        }
        lock_protocol::lockid_t lid = releaser_queue.front();
        releaser_queue.pop_front();
        // printf("[client] %s processing lock %016llx from releaser queue\n", this->id.c_str(), lid);
        pthread_mutex_unlock(&releaser_queue_mutex);

        pthread_mutex_lock(&lock_cache_mutex);
        lock_info &li = lock_cache[lid];
        if (li.state == FREE){
            printf("[client] %s lock %016llx is free, proceeding to release\n", this->id.c_str(), lid);
            li.state = RELEASING;
            pthread_mutex_unlock(&lock_cache_mutex);
            printf("[client] %s releasing lock %016llx, lock state = %s\n", 
                   this->id.c_str(), lid, get_state(li.state).c_str());
            int r;  
            int ret = cl -> call(lock_protocol::release, this->id, li.sequence_number, lid, r);
            pthread_mutex_lock(&lock_cache_mutex);
            if (ret != lock_protocol::OK) {
                fprintf(stderr, "[client] %s failed to release lock %016llx\n", this->id.c_str(), lid);
                li.state = FREE;
            } else {
                li.state = NONE; // Set the lock state to NONE after release
                sleep(1); // Simulate some delay for the release operation
                pthread_cond_broadcast(li.cond);
                printf("[client] %s successfully released lock %016llx and broadcasting to waiting threads\n", this->id.c_str(), lid);
            }
        }
        else {
            // printf("[client] %s lock %016llx is not in a releasable state, current state: %s\n", 
            //        this->id.c_str(), lid, get_state(li.state).c_str());
            add_to_releaser_queue(lid); // Re-add to the releaser queue if not in a releasable state
        }
        pthread_mutex_unlock(&lock_cache_mutex);
    }
}



rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int seq_num, int &r)
{
  printf("[client] %s revoke lock %016llx\n", this->id.c_str(), lid);
  pthread_mutex_lock(&lock_cache_mutex);
  lock_info &li = lock_cache[lid];
  if (li.sequence_number != seq_num) {
    printf("[client] %s revoke lock %016llx sequence number mismatch, expected %d, got %d\n", 
           this->id.c_str(), lid, li.sequence_number, seq_num);
    pthread_mutex_unlock(&lock_cache_mutex);
    return rlock_protocol::RPCERR; // Sequence number mismatch
  }
  printf("[client] %s lock %016llx is being queued for release\n", this->id.c_str(), lid);
  add_to_releaser_queue(lid);

  pthread_cond_signal(releaser_cv); // Notify the releaser thread

  pthread_mutex_unlock(&lock_cache_mutex);
  r = rlock_protocol::OK;
  return r;
}

void lock_client_cache::add_to_releaser_queue(lock_protocol::lockid_t &lid)
{
    pthread_mutex_lock(&releaser_queue_mutex);
    // add to releaser queue if not already present
    bool found = (std::find(releaser_queue.begin(), releaser_queue.end(), lid) != releaser_queue.end());
    if (!found)
    {
        // printf("[client] %s lock %016llx already in releaser queue\n", this->id.c_str(), lid);
        releaser_queue.push_back(lid);
    }
    pthread_mutex_unlock(&releaser_queue_mutex);
}

rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int seq_num, int &r)
{
  printf("[client] %s retry lock %016llx\n", this->id.c_str(), lid);
  pthread_mutex_lock(&lock_cache_mutex);
  lock_info &li = lock_cache[lid];
  std::list<pthread_t> waiting_threads = li.waiting_threads;
  //print waiting threads
  pthread_mutex_unlock(&lock_cache_mutex);

  if (waiting_threads.empty()) {
    // create a new thread and call acquire
    }

  printf("[client] %s broadcasting condition for lock %016llx\n", this->id.c_str(), lid);
  pthread_cond_broadcast(li.cond); // Notify all waiting threads on this lock
  r = rlock_protocol::OK;
  return rlock_protocol::OK;
}


std::string lock_client_cache::get_state(int state) {
  switch (state) {
    case NONE:
      return "NONE";
    case FREE:
      return "FREE";
    case LOCKED:
      return "LOCKED";
    case ACQUIRING:
      return "ACQUIRING";
    case RELEASING:
      return "RELEASING";
    default:
      return "UNKNOWN";
  }
} 