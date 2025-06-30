#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"



class lock_server_cache {
 private:
  enum lock_state {FREE, ACQUIRED};
  struct lock_info {
    std::string clt_id;
    int seq_num;
    pthread_cond_t *cv;
    lock_state state;
    std::list <std::string> waiting_clients;
    std::string retryer_sent_to;
    lock_info() {
      clt_id = "";
      seq_num = 0;
      cv = new pthread_cond_t;
      pthread_cond_init(cv, NULL);
      state = FREE;
      retryer_sent_to = "";
    }
  };
  std::map<lock_protocol::lockid_t, lock_info> locks;
  pthread_mutex_t lock_mutex;
  pthread_cond_t *revoke_cv;
  std::list< std::pair <lock_protocol::lockid_t, std::string>> revokes;
  std::list< lock_protocol::lockid_t > free_locks;
  pthread_cond_t *retry_cv;
  std::map<std::string, rpcc*> client_connections;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  lock_protocol::status release(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
};

#endif


/*
acquire(lid){
if lock does not exist:
  create new lock_info object
  set state to LOCKED
  set clt_id to current client id
}
else{
if lock is FREE:
  set state to LOCKED
  set clt_id to current client id
}
else if lock is LOCKED:
  if clt_id is the owner:
    return error "Lock is already locked by this client"
  else:
    add clt_id to waiting list
    add lock to revoke queue
    trigger revoke thread
    return RETRY
}
release(lid){
if lock does not exist:
  return error "Lock was never acquired by client"
}
else{
  if lock is FREE:
    return error "Lock is already free"
  }
  else if lock is LOCKED:
  {
    if clt_id is the owner:
      set state to FREE
      remove clt_id from waiting list
      add lock to retry queue
      trigger retry thread
    else:
      return error "Lock is locked by another client"
    }
  }
}

revoker(){
if revoke queue is empty:
  wait for revoke signal
}
else{
  get lock from revoke queue
  if lock is FREE:
    remove lock from revoke queue
    continue
  }
  else if lock is LOCKED:
  {
    if clt_id is the owner:
      set state to FREE
      remove clt_id from waiting list
      continue
    else:
      return error "Lock is locked by another client"
    }
  }
}

retryer(){
if retry queue is empty:
  wait for retry signal
}
else{
  get lock from retry queue
  send retry request to top most waiting client;
  }
}

*/