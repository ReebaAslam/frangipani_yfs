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
    lock_info() {
      clt_id = "";
      seq_num = 0;
      cv = new pthread_cond_t;
      pthread_cond_init(cv, NULL);
      state = FREE;
    }
    
  };
  std::map<lock_protocol::lockid_t, lock_info> locks;
  pthread_mutex_t lock_mutex;
  pthread_cond_t *revoke_cv;
  std::list< std::pair <lock_protocol::lockid_t, std::string>> revokes;
  std::list< lock_protocol::lockid_t > free_locks;
  pthread_cond_t *retry_cv;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  lock_protocol::status release(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
};

#endif
