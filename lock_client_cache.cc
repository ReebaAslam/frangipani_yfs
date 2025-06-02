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
  pthread_mutex_lock(&lock_mutex);
  printf("[client] %s request to acquire lock %016llx\n", this-> get_id().c_str(), lid);
  lock_info &li = lock_cache[lid]; // creates if not already present
  pthread_t tid = pthread_self();

  // Add current thread to the waiting queue if not already in it
  if (std::find(li.waiting_threads.begin(), li.waiting_threads.end(), tid) == li.waiting_threads.end()) {
    li.waiting_threads.push_back(tid);
  }

  while (true) {
    if (li.to_be_revoked){
      printf("[client] %s lock %016llx is to be revoked, not acquiring lock and thread %lu going to sleep\n", this->get_id().c_str(), lid, tid);
      pthread_cond_wait(li.cond, &lock_mutex);
      continue;
    }
    if (li.waiting_threads.front() != tid){
      printf("[client] %s lock %016llx thread not the first one, not acquiring lock and thread %lu going to sleep\n", this->get_id().c_str(), lid, tid);
      pthread_cond_wait(li.cond, &lock_mutex);
      continue;
    }
    else {
      if (li.state != NONE && li.state != FREE) {
        printf("[client] %s lock %016llx state is %s which is neither FREE nor NONE, not acquiring lock and thread %lu going to sleep\n", this->get_id().c_str(), lid, this->get_state(li.state).c_str(), tid);
        pthread_cond_wait(li.cond, &lock_mutex);
        continue;
      }
      if (li.state == NONE) {
        printf("[client] %s lock %016llx state is none, thread %lu ,acquiring lock from server\n", this->get_id().c_str(), lid, tid);
        li.state = ACQUIRING;
        li.retry_received = false;
        pthread_mutex_unlock(&lock_mutex);
        int r;
        int ret = cl->call(lock_protocol::acquire, this->id, li.sequence_number++, lid, r);
        pthread_mutex_lock(&lock_mutex);
        if (ret == lock_protocol::OK) {
          printf("[client] %s lock %016llx acquired by thread %lu\n", this->get_id().c_str(), lid, tid);
          li.state = LOCKED;
          li.waiting_threads.pop_front();
          pthread_mutex_unlock(&lock_mutex);
          return lock_protocol::OK;
        }
        else if (ret == lock_protocol::RETRY){
          li.state = NONE;
          printf("[client] %s lock %016llx retry received by thread %lu\n", this->get_id().c_str(), lid, tid);
          while (!li.retry_received){
            pthread_cond_wait(li.cond, &lock_mutex);
            continue;
          }
          printf("[client] retry received for lock %016llx by thread %lu\n", lid, tid);
        }
      }
      else if (li.state == FREE){
        printf("[client] %s lock %016llx state is free, thread %lu is acquiring lock\n", this->get_id().c_str(), lid, tid);
        li.state = LOCKED;
        li.waiting_threads.pop_front();
        pthread_mutex_unlock(&lock_mutex);
        return lock_protocol::OK;
      }
    }
  }
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

  li.state = FREE;

  // Remove this thread from the waiting list
  remove_pthread_from_waiting_threads(li);

  // Wake up other waiting threads
  pthread_cond_broadcast(li.cond);

  // If this lock was revoked, notify releaser thread
  if (li.to_be_revoked) {
    printf("[client] %s lock %016llx is to be revoked, notifying releaser\n", this->get_id().c_str(), lid);
    pthread_cond_signal(releaser_cv);
  }

  pthread_mutex_unlock(&lock_mutex);
  printf("[client] %s released lock %016llx\n", this->get_id().c_str(), lid);

  return lock_protocol::OK;
}


void lock_client_cache::releaser()
{
  printf("[client] %s releaser thread started\n", this->id.c_str());
  while (true) {
    pthread_mutex_lock(&lock_mutex);
    while (revoke_queue.empty()) {
      pthread_cond_wait(releaser_cv, &lock_mutex);
    }
    printf("[client] %s releaser thread awake\n", this->id.c_str());

    std::list<lock_protocol::lockid_t>::iterator it = revoke_queue.begin();
    while (it != revoke_queue.end()) {
      lock_protocol::lockid_t lid = *it;
      lock_info &li = lock_cache[lid];

      printf("[client] %s checking lock %016llx to release to server\n", this->id.c_str(), lid);
      printf("[client] %s lock %016llx state: %s, to_be_revoked: %d\n", this->id.c_str(), lid, this->get_state(li.state).c_str(), li.to_be_revoked);
      if (li.state != LOCKED) {
        // Can safely release this lock
        printf("[client] %s releasing lock %016llx to server\n", this->id.c_str(), lid);
        li.state = RELEASING;
        pthread_mutex_unlock(&lock_mutex);

        lu -> dorelease(lid); // Notify the user that the lock is being released
        int r;
        int ret = cl->call(lock_protocol::release, this->id, li.sequence_number++, lid, r);

        pthread_mutex_lock(&lock_mutex);
        if (ret == lock_protocol::OK) {
          printf("[client] %s released lock %016llx to server\n", this->id.c_str(), lid);

          li.to_be_revoked = false;
          li.state = NONE;
          it = revoke_queue.erase(it);  // remove from queue
          pthread_cond_broadcast(li.cond); // wake up any waiting threads
          continue;
        } else {
          std::cerr << "Error releasing lock " << lid << std::endl;
        }
      }

      ++it;  // Move on to next lock if not FREE or failed
    }

    pthread_mutex_unlock(&lock_mutex);
    sleep(1); // optional: yield CPU and avoid tight loop
  }
}



rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int &r)
{
  printf("[client] %s revoke lock %016llx\n", this->id.c_str(), lid);
  pthread_mutex_lock(&lock_mutex);
  lock_info &li = lock_cache[lid];
  li.to_be_revoked = true;
  if (std::find(revoke_queue.begin(), revoke_queue.end(), lid) == revoke_queue.end()) {
    revoke_queue.push_back(lid);
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
  if (lock_cache.find(lid) == lock_cache.end()) {
    printf("[client] %s lock %016llx not found\n", this->id.c_str(), lid);
    pthread_mutex_unlock(&lock_mutex);
    return rlock_protocol::RPCERR;
  }

  lock_info &li = lock_cache[lid];
  li.retry_received = true;
  pthread_cond_broadcast(li.cond);
  pthread_mutex_unlock(&lock_mutex);
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