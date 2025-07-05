#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

#include "rsm.h"


class lock_server_cache {
 private:
  class rsm *rsm;
  enum lock_state {FREE, ACQUIRED};
  struct client_info {
    std::string clt_id;
    int seq_num;
    client_info() : clt_id(""), seq_num(0) {}
    bool operator==(const client_info& other) const {
      return clt_id == other.clt_id && seq_num == other.seq_num;
    }
  };
  struct lock_info {
    client_info clt_info;
    pthread_cond_t *cv;
    lock_state state;
    std::list <client_info> waiting_clients;
    bool revoke_sent;
    lock_info() {
      cv = new pthread_cond_t;
      pthread_cond_init(cv, NULL);
      state = FREE;
      revoke_sent = false;
    }
  };
  std::map<lock_protocol::lockid_t, lock_info> locks;
  pthread_mutex_t lock_mutex;
  pthread_mutex_t revoke_mutex;
  pthread_cond_t *revoke_cv;
  std::list<lock_protocol::lockid_t> revokes;
  std::list< lock_protocol::lockid_t > free_locks;
  pthread_mutex_t free_locks_mutex;
  pthread_cond_t *retry_cv;
  std::map<std::string, rpcc*> client_connections;
  pthread_mutex_t client_connections_mutex;

 public:
  lock_server_cache(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  void add_lock_to_revokes(lock_protocol::lockid_t &lid);
  void add_lock_to_free_locks(lock_protocol::lockid_t &lid);
  void queue_waiting_client(lock_server_cache::lock_info &li, client_info &clt_info);
  lock_protocol::status release(std::string clt_id, int seq_num, lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
  rpcc* get_client_connection(const std::string &clt_id);
};

#endif
