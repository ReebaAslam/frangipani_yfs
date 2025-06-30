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
  pthread_mutex_init(&lock_mutex, NULL);

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret;
  pthread_mutex_lock(&lock_mutex);
  printf("[client] %s request to acquire lock %016llx\n", this-> get_id().c_str(), lid);
  lock_info &li = lock_cache[lid]; // creates if not already present
  pthread_t tid = pthread_self();

  // is pthread the topmost thread?
  while (true){
    if(!li.waiting_threads.empty() || li.waiting_threads.front() != tid){
    // If not, add to waiting threads and wait
    printf("[client] %s is not the topmost thread for lock %016llx, adding to waiting list\n", this->get_id().c_str(), lid);
    add_pthread_to_waiting_threads(li);
    pthread_cond_wait(li.cond, &lock_mutex);
    continue; // retry acquiring the lock
    printf("[client] %s woke up from waiting for lock %016llx\n", this->get_id().c_str(), lid);
    }
    if (li.state == NONE){
      li.sequence_number++;
      li.state = ACQUIRING;
      pthread_mutex_unlock(&lock_mutex);
      printf("[client] %s sending acquire RPC for lock %016llx\n", this->get_id().c_str(), lid);
      int r;
      ret = cl->call(lock_protocol::acquire, this->id, li.sequence_number, lid, r);
      pthread_mutex_lock(&lock_mutex);
      if (ret == lock_protocol::OK) {
        printf("[client] %s acquired lock %016llx\n", this->get_id().c_str(), lid);
        // if lock in release queue, set state to FREE and trigger releaser thread
        if (is_lock_in_release_queue(lid)) {
          li.state = FREE;
          printf("[client] %s lock %016llx is in release queue, setting state to FREE\n", this->get_id().c_str(), lid);
          pthread_cond_signal(releaser_cv); // signal releaser thread
        } 
        else {
          printf("[client] %s lock %016llx is not in release queue, setting state to LOCKED\n", this->get_id().c_str(), lid);
          li.state = LOCKED;
          li.owner_thread = tid; // set owner thread
          remove_pthread_from_waiting_threads(li); // remove this thread from waiting list
        }
        break;
      }
      else {
        printf("[client] %s failed to acquire lock %016llx, server sent RETRY\n", this->get_id().c_str(), lid);
        li.state = NONE; // reset state to NONE
        pthread_cond_wait(li.cond, &lock_mutex); // wait for next signal
        continue; // retry acquiring the lock
      }
    }
    else if(li.state == FREE) {
      printf("[client] %s lock %016llx is free, setting state to LOCKED\n", this->get_id().c_str(), lid);
      // If lock is free, set state to LOCKED and return
      li.state = LOCKED;
      li.owner_thread = tid; // set owner thread
      remove_pthread_from_waiting_threads(li); // remove this thread from waiting list
      ret = lock_protocol::OK;
      break;
    }
    else{
      if (li.state == LOCKED) {
      // If lock is already locked, wait for it to be released
      printf("[client] %s lock %016llx is already locked, waiting\n", this->get_id().c_str(), lid);
      pthread_cond_wait(li.cond, &lock_mutex);
      continue; // retry acquiring the lock
      }
      else if (li.state == ACQUIRING ) {
        // If lock is being acquired or released by another thread, wait
        printf("[client] %s lock %016llx is being acquired by another thread, should not have reached this point\n", this->get_id().c_str(), lid);

      }
      else{
        // If lock is in RELEASING state, wait for it to be released
        printf("[client] %s lock %016llx is being released by another thread, waiting\n", this->get_id().c_str(), lid);
      }

      pthread_cond_wait(li.cond, &lock_mutex);
      continue; // retry acquiring the lock
    }
  }
  pthread_mutex_unlock(&lock_mutex);
  return ret;
}


bool lock_client_cache::is_lock_in_release_queue(lock_protocol::lockid_t lid)
{
  return std::find(release_queue.begin(), release_queue.end(), lid) != release_queue.end();
}

void lock_client_cache::remove_pthread_from_waiting_threads(lock_client_cache::lock_info &li)
{
  li.waiting_threads.remove(pthread_self());
}

void lock_client_cache::add_pthread_to_waiting_threads(lock_client_cache::lock_info &li)
{
  // Lock is not available yet
  if (std::find(li.waiting_threads.begin(), li.waiting_threads.end(), pthread_self()) == li.waiting_threads.end())
  {
    li.waiting_threads.push_back(pthread_self());
  }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  printf("[client] %s request to release lock %016llx\n", this->get_id().c_str(), lid);
  pthread_mutex_lock(&lock_mutex);
  lock_info &li = lock_cache[lid];
  pthread_t tid = pthread_self();
  if (li.owner_thread != tid) {
    printf("[client] %s cannot release lock %016llx, it is not the owner\n", this->get_id().c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Not the owner
  }
  if (li.state == NONE) {
    printf("[client] %s lock %016llx was never acquired, cannot release\n", this->get_id().c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Lock was never acquired
  }
  if (li.state == FREE) {
    printf("[client] %s lock %016llx is already free, cannot release\n", this->get_id().c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    return lock_protocol::RPCERR; // Lock is already free
  }
  if (li.state == LOCKED){
    printf("[client] %s releasing lock %016llx\n", this->get_id().c_str(), lid);
    li.state = FREE;
    // Remove this thread from the waiting list
    remove_pthread_from_waiting_threads(li);

    if (is_lock_in_release_queue(lid)) {
      // If this lock is in the release queue, remove it
      printf("[client] %s lock %016llx is in release queue, triggering releaser thread \n", this->get_id().c_str(), lid);
      pthread_cond_signal(releaser_cv);
    } else {
      // If not in release queue, add it to the release queue
      printf("[client] %s lock %016llx is not in release queue, waking up other threads waiting for lock\n", this->get_id().c_str(), lid);
        // Wake up other waiting threads
      pthread_cond_broadcast(li.cond);
    }
  }

  pthread_mutex_unlock(&lock_mutex);
  return lock_protocol::OK;
}


void lock_client_cache::releaser()
{
  printf("[client] %s releaser thread started\n", this->id.c_str());
  pthread_mutex_lock(&lock_mutex);
  while (true) {
    while (release_queue.empty()) {
      pthread_cond_wait(releaser_cv, &lock_mutex);
    }
    lock_protocol::lockid_t lid = release_queue.front();
    lock_info &li = lock_cache[lid];
    if (li.state == NONE) {
      printf("[client] %s lock %016llx was never acquired, cannot release\n", this->id.c_str(), lid);
      release_queue.pop_front();
      continue; // Skip to next iteration
    }
    else if(li.state == FREE){
      li.state = RELEASING;
      pthread_mutex_unlock(&lock_mutex);
      lu -> dorelease(lid); // Notify the lock release user
      int r;  
      int ret = cl->call(lock_protocol::release, this->id, li.sequence_number, lid, r);
      pthread_mutex_lock(&lock_mutex);
      if (ret == lock_protocol::OK) {
        release_queue.pop_front(); // Remove from release queue
        li.state = NONE; // Reset state to NONE
      }
      else{
        li.state = FREE; // Reset state to FREE if release failed
        continue; // Skip to next iteration
      }
    }
  }
}

rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int &r)
{
  printf("[client] %s revoke lock %016llx\n", this->id.c_str(), lid);
  pthread_mutex_lock(&lock_mutex);
  lock_info &li = lock_cache[lid];
  if (li.state == NONE) {
    printf("[client] %s lock %016llx is already free, nothing to revoke\n", this->id.c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    r = rlock_protocol::OK;
    return rlock_protocol::OK; // Lock is already free
  }
  else{
    // add to release queue if not already in it
    if (!is_lock_in_release_queue(lid)) {
      printf("[client] %s lock %016llx is not in release queue, adding it\n", this->id.c_str(), lid);
      release_queue.push_back(lid);
    }
    if (li.state == FREE){
      printf("[client] %s lock %016llx is free, signalling releaser thread\n", this->id.c_str(), lid);
      pthread_cond_signal(releaser_cv); // Signal the releaser thread
    }
  }

  printf("[client] %s lock %016llx is free, signalling releaser\n", this-> id.c_str(), lid);
  pthread_cond_signal(releaser_cv);

  pthread_mutex_unlock(&lock_mutex);
  r = rlock_protocol::OK;
  return rlock_protocol::OK;
}



rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int &r)
{
  printf("[client] %s retry lock %016llx\n", this->id.c_str(), lid);
  pthread_mutex_lock(&lock_mutex);

  lock_info &li = lock_cache[lid];

  if (li.state==NONE){
    if (!li.waiting_threads.empty()) {
      // If there are waiting threads, wake them up
      printf("[client] %s lock %016llx is NONE, waking up waiting threads\n", this->id.c_str(), lid);
      pthread_cond_broadcast(li.cond);
      pthread_mutex_unlock(&lock_mutex);
    } else {
      pthread_mutex_unlock(&lock_mutex);
      acquire(lid); // Try to acquire the lock again
    }
  }
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