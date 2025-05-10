// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&lock_mutex, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}



lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r){

  pthread_mutex_lock(&lock_mutex);

  if (locks.find(lid) == locks.end()){
    printf("creating new lock %llu\n", lid);
    locks[lid] = lock_server::FREE;
    
    pthread_cond_t* cv = new pthread_cond_t;
    pthread_cond_init(cv, NULL);
    lock_conds[lid] = cv;
  }
  
  while(locks[lid] == LOCKED){
    // lock is already held by another client, put it in a queue and grant it to him when the lock is released if it is on top of the queue
    // printf("lock %llu is already held by another client\n", lid);
    pthread_cond_wait(lock_conds[lid], &lock_mutex); // Wait for the lock to become free
  }

  printf("lock %llu is free, granting it to client %d\n", lid, clt);
  locks[lid] = lock_server::LOCKED;
  nacquire++; 
  r = nacquire;
  pthread_mutex_unlock(&lock_mutex); // Unlock the mutex before returning
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r){
  pthread_mutex_lock(&lock_mutex); // Lock the mutex

  if (locks.find(lid) == locks.end() || locks[lid] == FREE){
    // lock is not held by any client, return error
    printf("lock %llu is not held by any client\n", lid);
    pthread_mutex_unlock(&lock_mutex); // Unlock the mutex before returning
    return lock_protocol::NOENT;
  }
  printf("lock %llu is being released by client %d\n", lid, clt);
  locks[lid] = FREE; // set the lock to free state
  nacquire--; 
  r = nacquire;
  // Signal waiting threads that the lock is now free
  pthread_cond_signal(lock_conds[lid]);

  pthread_mutex_unlock(&lock_mutex); // Unlock the mutex before returning
  return lock_protocol::OK;
}

