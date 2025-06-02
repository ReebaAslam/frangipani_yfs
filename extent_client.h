// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"
#include "rpc.h"

class extent_client: public extent_server {

 private:
  rpcc *cl;
  std::list<extent_protocol::extentid_t> modified_extents; // to keep track of extent ids
  std::list<extent_protocol::extentid_t> removed_extents; // to keep track of removed extent ids
  std::list<extent_protocol::extentid_t> modified_extent_attributes; // to keep track of modified extent attributes
  pthread_mutex_t extent_mutex; // mutex to protect extent operations
  pthread_cond_t *extent_cond; // condition variable for extent operations
  std::map<extent_protocol::extentid_t, std::list <pthread_t>> waiting_threads; // list of threads waiting for extent operations

 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid,
                              std::string &buf);
  extent_protocol::status get_and_cache_extent_from_server(extent_protocol::extentid_t &eid,
     std::string &buf, extent_protocol::attr &attr);
  void put_thread_to_wait_if_needed(extent_protocol::extentid_t &eid);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
};

#endif 

