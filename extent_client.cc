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

  unlock_mutex_and_deque_thread(eid);
  return ret;
}

void extent_client::unlock_mutex_and_deque_thread(extent_protocol::extentid_t &eid)
{
  // remove the current thread from the waiting_threads for this extent
  std::list<pthread_t> &extent_waiting_threads = waiting_threads[eid];
  pthread_t tid = pthread_self();
  extent_waiting_threads.remove(tid);
  pthread_mutex_unlock(&extent_mutex);
  pthread_cond_broadcast(extent_cond);
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
      add_extent_to_modified_queues(eid);

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
  unlock_mutex_and_deque_thread(eid);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&extent_mutex);
  put_thread_to_wait_if_needed(eid);
  extent_protocol::attr a;
  a.size = buf.size();
  a.mtime = time(0);
  a.ctime = time(0);
  a.atime = time(0);
  int r;
  ret = extent_server::put(eid, buf, a, r);
  if (ret == extent_protocol::OK) {
    add_extent_to_modified_queues(eid);
  }
  unlock_mutex_and_deque_thread(eid);
  return ret;
}

void extent_client::add_extent_to_modified_queues(extent_protocol::extentid_t &eid)
{
  // if eid is not already in modified_extents, then add it
  if (std::find(modified_extents.begin(), modified_extents.end(), eid) == modified_extents.end())
  {
    printf("extent_client::add_extent_to_modified_queues adding extent id %016llx to modified extents\n", eid);
    modified_extents.push_back(eid);           // keep track of modified extents

  }
  if (std::find(modified_extent_attributes.begin(), modified_extent_attributes.end(), eid) == modified_extent_attributes.end())
  {
    printf("extent_client::add_extent_to_modified_queues adding extent id %016llx to modified extent attributes\n", eid);
    modified_extent_attributes.push_back(eid); // keep track of modified extent attributes

  }
  // remove eid from removed_extents if it was previously removed
  auto it = std::find(removed_extents.begin(), removed_extents.end(), eid);
  if (it != removed_extents.end())
  {
    printf("extent_client::add_extent_to_modified_queues removing extent id %016llx from removed extents\n", eid);
    removed_extents.erase(it); // remove from removed extents
  }
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
    // remove eid from modified_extents if it was previously modified
    auto it = std::find(modified_extents.begin(), modified_extents.end(), eid);
    if (it != modified_extents.end()) {
      modified_extents.erase(it); // remove from modified extents
    }
    // remove eid from modified_extent_attributes if it was previously modified
    auto it_attr = std::find(modified_extent_attributes.begin(), modified_extent_attributes.end(), eid);
    if (it_attr != modified_extent_attributes.end()) {
      modified_extent_attributes.erase(it_attr); // remove from modified extent attributes
    }
  }
  unlock_mutex_and_deque_thread(eid);
  return ret;
}

void extent_client::flush(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&extent_mutex);
  put_thread_to_wait_if_needed(eid);
    // if eid is in modified_extents, then flush it to the server
  if (std::find(modified_extents.begin(), modified_extents.end(), eid) != modified_extents.end()) {
    printf("extent_client::flush flushing extent id %016llx to server\n", eid);
    std::string buf = extents[eid];
    extent_protocol::attr attr = extent_attrs[eid];
    int r;  
    extent_protocol::status ret = cl->call(extent_protocol::put, eid, buf, attr, r);
    
    if (ret == extent_protocol::OK) {
      printf("extent_client::flush successfully flushed extent id %016llx to server\n", eid);
      modified_extents.remove(eid); // remove from modified extents
      extents.erase(eid); // remove from local cache
      modified_extent_attributes.remove(eid); // remove from modified extent attributes
      extent_attrs.erase(eid); // remove from local cache
    }
  }
  if (std::find(removed_extents.begin(), removed_extents.end(), eid) != removed_extents.end()) {
    printf("extent_client::flush removing extent id %016llx from server\n", eid);
    int r;
    extent_protocol::status ret = cl->call(extent_protocol::remove, eid, r);
    
    if (ret == extent_protocol::OK) {
      printf("extent_client::flush successfully removed extent id %016llx from server\n", eid);
      removed_extents.remove(eid); // remove from removed extents
    }
  }
  unlock_mutex_and_deque_thread(eid);
}

