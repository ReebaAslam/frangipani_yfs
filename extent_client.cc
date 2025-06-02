// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  pthread_mutex_init(&extent_mutex, NULL);
  extent_cond = new pthread_cond_t;
  pthread_cond_init(extent_cond, NULL);
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&extent_mutex);
  extent_protocol::status ret = extent_protocol::OK;
  put_thread_to_wait_if_needed(eid);

  printf("extent_client::get called for extent id %016llx\n", eid);

  // check if my extent has eid, call extent_server::get
  if (extents.find(eid) != extents.end()) {
    printf("extent_client::get found extent id %016llx in cache\n", eid);
    ret = extent_server::get(eid, buf);
  }
  else{
    printf("extent_client::get extent id %016llx not found in cache, fetching from server\n", eid);
    extent_protocol::attr attr;
    ret = get_and_cache_extent_from_server(eid, buf, attr);
  }

  pthread_mutex_unlock(&extent_mutex);
  pthread_cond_broadcast(extent_cond);
  return ret;
}

extent_protocol::status 
extent_client::get_and_cache_extent_from_server(extent_protocol::extentid_t &eid, std::string &buf, extent_protocol::attr &attr)
{
  extent_protocol::status ret;
  ret = cl->call(extent_protocol::get, eid, buf);
  if (ret == extent_protocol::OK)
  {
    printf("extent_client::get_and_cache_extent_from_server fetched extent %016llx from server\n", eid);
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if (ret == extent_protocol::OK)
    {
      printf("extent_client::get_and_cache_extent_from_server fetched attributes for extent %016llx from server\n", eid);
      extents[eid] = buf;
      extent_attrs[eid] = attr; // store the attributes if getattr was successful
      modified_extent_attributes.push_back(eid); // keep track of modified extent attributes
    }
  }
  return ret;
} 

void extent_client::put_thread_to_wait_if_needed(extent_protocol::extentid_t & eid)
{
  // if thread is not already in waiting_threads for this extent, then add it
  std::list<pthread_t> extent_waiting_threads = waiting_threads[eid];
  pthread_t tid = pthread_self();
  if (std::find(extent_waiting_threads.begin(), extent_waiting_threads.end(), tid) == extent_waiting_threads.end())
  {
    waiting_threads[eid].push_back(tid);
  }
  // if thread is not at the front of the waiting_threads for this extent, then wait
  while (waiting_threads[eid].front() != tid)
  {
    printf("extent_client::put_thread_to_wait_if_needed thread %lu waiting for extent id %016llx\n", tid, eid);
    pthread_cond_wait(extent_cond, &extent_mutex);
  }
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  pthread_mutex_lock(&extent_mutex);
  put_thread_to_wait_if_needed(eid);
  extent_protocol::status ret = extent_protocol::OK;
  // check if my extent has eid, call extent_server::getattr
  if (extent_attrs.find(eid) != extent_attrs.end()) {
    printf("extent_client::getattr found extent id %016llx in cache\n", eid);
    ret = extent_server::getattr(eid, attr);
  }
  else{
    std::string buf;
    printf("extent_client::getattr extent id %016llx not found in cache, fetching from server\n", eid);
    ret = get_and_cache_extent_from_server(eid, buf, attr);
  }
  pthread_mutex_unlock(&extent_mutex);
  pthread_cond_broadcast(extent_cond);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&extent_mutex);
  put_thread_to_wait_if_needed(eid);
  int r;
  ret = extent_server::put(eid, buf, r);
  if (ret == extent_protocol::OK) {
    modified_extents.push_back(eid); // keep track of modified extents
    modified_extent_attributes.push_back(eid); // keep track of modified extent attributes
  }
  pthread_mutex_unlock(&extent_mutex);
  pthread_cond_broadcast(extent_cond);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&extent_mutex);
  put_thread_to_wait_if_needed(eid);
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = extent_server::remove(eid, r);
  if (ret == extent_protocol::OK) {
    removed_extents.push_back(eid); // keep track of removed extents
  }
  pthread_mutex_unlock(&extent_mutex);
  pthread_cond_broadcast(extent_cond);
  return ret;
}


