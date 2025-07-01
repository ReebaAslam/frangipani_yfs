// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


// SUGGESTED LOCK CACHING IMPLEMENTATION PLAN:
//
// to work correctly for lab 7,  all the requests on the server run till 
// completion and threads wait on condition variables on the client to
// wait for a lock.  this allows the server to be replicated using the
// replicated state machine approach.
//
// On the client a lock can be in several states:
//  - free: client owns the lock and no thread has it
//  - locked: client owns the lock and a thread has it
//  - acquiring: the client is acquiring ownership
//  - releasing: the client is releasing ownership
//
// in the state acquiring and locked there may be several threads
// waiting for the lock, but the first thread in the list interacts
// with the server and wakes up the threads when its done (released
// the lock).  a thread in the list is identified by its thread id
// (tid).
//
// a thread is in charge of getting a lock: if the server cannot grant
// it the lock, the thread will receive a retry reply.  at some point
// later, the server sends the thread a retry RPC, encouraging the client
// thread to ask for the lock again.
//
// once a thread has acquired a lock, its client obtains ownership of
// the lock. the client can grant the lock to other threads on the client 
// without interacting with the server. 
//
// the server must send the client a revoke request to get the lock back. this
// request tells the client to send the lock back to the
// server when the lock is released or right now if no thread on the
// client is holding the lock.  when receiving a revoke request, the
// client adds it to a list and wakes up a releaser thread, which returns
// the lock the server as soon it is free.
//
// the releasing is done in a separate a thread to avoid
// deadlocks and to ensure that revoke and retry RPCs from the server
// run to completion (i.e., the revoke RPC cannot do the release when
// the lock is free.
//
// a challenge in the implementation is that retry and revoke requests
// can be out of order with the acquire and release requests.  that
// is, a client may receive a revoke request before it has received
// the positive acknowledgement on its acquire request.  similarly, a
// client may receive a retry before it has received a response on its
// initial acquire request.  a flag field is used to record if a retry
// has been received.
//


class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  enum lock_state {NONE, FREE, LOCKED, ACQUIRING, RELEASING};
  struct lock_info {
    lock_state state;
    pthread_cond_t *cond;
    int sequence_number;
    pthread_t owner_thread; // thread that currently owns the lock
    std::list<pthread_t> waiting_threads;
    bool to_be_revoked;
    bool retry_received;
    lock_info(){
      state = NONE;
      cond = new pthread_cond_t;
      pthread_cond_init(cond, NULL);
      sequence_number = 0;
      to_be_revoked = false;
      retry_received = false;
    }
  };
  // A map to keep track of the locks and their states
  std::map<lock_protocol::lockid_t, lock_info> lock_cache;
  pthread_mutex_t lock_mutex;
  pthread_cond_t *releaser_cv;
  std::list<lock_protocol::lockid_t> release_queue;


 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  bool is_lock_in_release_queue(lock_protocol::lockid_t lid);
  void remove_pthread_from_waiting_threads(lock_client_cache::lock_info &li);
  void add_pthread_to_waiting_threads(lock_client_cache::lock_info &li);
  virtual lock_protocol::status release(lock_protocol::lockid_t);
  void releaser();
  rlock_protocol::status revoke(lock_protocol::lockid_t lid, int &);
  rlock_protocol::status retry(lock_protocol::lockid_t lid, int &);
  std::string get_id() { return id; }
  std::string get_state(int state);
};
#endif



/*
acquire(lid){
- check if thread is the topmost thread in the waiting list
- if not, then add to waiting list if not already there else wait on condition variable
- if yes, get lock status against lid
- if lock is None:
  - new seq num = old seq num + 1
  - set lock state to Acquiring
  - send RPC to server to acquire lock
  - if RPC returns OK:
    - if lock is in the release queue:
      - set lock state to FREE
      - trigger releaser thread
    - else
      - set lock state to Locked
      - remove thread from waiting list
  - if RPC returns RETRY:
    - remove lock from cache so that status remains None
- elif lock is Free:
  - then grant the thread the lock
  - set lock state to Locked
- elif lock is Locked:
  - put the thread to waiting
- elif lock is Acquiring:
  - this seems like an error, can't get to this point as thread can't be topmost
- elif lock is Releasing:
  - put the thread to waiting
}

relase(lid){
- check if thread is the one that has the lock
- if not then return error
- if yes, get lock status against lid
- if lock is None:
  - return error "Lock was never acquired by client"
- if lock is Free:
  - return error "Lock is already free"
- if lock is Locked:
  - set lock state to Free
  - remove thread from waiting list
  - if lock is in the relase queue:
    - trigger releaser thread
  - else
    - wake up any waiting threads
- if lock is Acquiring:
  - this should not be happening as thread can't be topmost
  - return error "Lock is being acquired by another thread"
- if lock is Releasing:
  - return error "Lock is being released by another thread"
}

revoke(lid){
- get lock status against lid
if lock is None:
  - return error "Lock was never acquired by client"
- if lock is Free:
  - trigger releaser thread, add lock to the release queue
- if lock is Locked:
  - wait for the lock to be released
  - add lock to the release queue
- if lock is acquiring:
  - wait for the lock to be released
  - add lock to the release queue
- if lock is releasing:
  - do nothing, wait for the releaser thread to release the lock
}

retry(lid){
- get lock status against lid
- if lock is None:
  - call acquire(lid) to try to acquire the lock again
- else if lock is Free:
  - return error "Lock is already free"
- else if lock is Locked:
  - return error "Lock is already locked by another thread"
- else if lock is Acquiring:
  - 
- if lock is any other status:
  - return error "Something went wrong cache already knows about lock"
}

releaser(lid){
- while release queue is not empty:
  - get the first lock from the release queue
  - get lock status against lid
  - if lock is None:
    - return error "Lock was never acquired by client"
  - if lock is Free:
    - set lock state to Releasing
    - send RPC to server to release the lock
    - if RPC returns OK:
      - what to do with waiting threads?
      - remove lock from cache
      - remove lock from release queue
    - else:
      - set lock state to Free
      - try again
  - if lock is Locked:
     - try again
  - if lock is Acquiring:
    - try again
  - if lock is Releasing:
    - this seems like an error, can't get to this point as thread can't be topmost
}
*/