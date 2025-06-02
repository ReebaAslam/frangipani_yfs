// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, extent_protocol::attr attr, int &r)
{
  printf("Calling put for extent id %016llx\n", id);
  extents[id] = buf;
  extent_attrs[id] = attr;
  r = extent_protocol::OK;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("get for extent id %016llx bool condition %s\n", id, extents.find(id) != extents.end() ? "true" : "false");
  if (extents.find(id) != extents.end() && extent_attrs.find(id) != extent_attrs.end()) {
    buf = extents[id];
    extent_attrs[id].atime = time(0);
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("[extent_server] getattr for extent id %016llx\n", id);

  if (extent_attrs.find(id) != extent_attrs.end()) {
    printf("[extent_server] getattr found extent id %016llx \n", id);
    a = extent_attrs[id];
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}

int extent_server::remove(extent_protocol::extentid_t id, int &r)
{
  r = extent_protocol::NOENT;
  printf("[extent_server]remove for extent id %016llx\n", id);
  if (extents.find(id) != extents.end()) {
    extents.erase(id);
    extent_attrs.erase(id);
    r = extent_protocol::OK;
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}


int extent_server::setattr(extent_protocol::extentid_t id, extent_protocol::attr &a, int &r)
{
  printf("[extent_server] setattr for extent id %016llx\n", id);
  if (extent_attrs.find(id) != extent_attrs.end()) {
    extent_attrs[id] = a;
    r = extent_protocol::OK;
    return extent_protocol::OK;
  }
  return extent_protocol::NOENT;
}
