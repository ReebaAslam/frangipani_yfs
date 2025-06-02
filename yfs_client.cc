// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <set>
#include <random>



yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lock_release_user *lu = new extent_release_user(ec);
  lc = new lock_client_cache(lock_dst, lu);
  std::string dir_content;
  lc->acquire(0x1); // Acquire lock for root directory
  if (ec->get(0x1, dir_content) == extent_protocol::NOENT) 
  {
    printf("[yfs_client] Root directory does not exist, creating it.\n");
    ec->put(0x1, "");
  }
  lc->release(0x1); // Release lock for root directory
}



yfs_client::inum
yfs_client::generate_unique_inum(bool is_file)
{
  static std::random_device rd;
  static std::mt19937_64 gen(rd()); // 64-bit Mersenne Twister RNG
  static std::uniform_int_distribution<uint32_t> dis(1, 0x7FFFFFFF); // 31 bits

  uint32_t rand_id = dis(gen);
  if (is_file) {
    rand_id = 0x80000000 | rand_id; // Set MSB to indicate file
  }
  std::string buf;
  if (ec->get(rand_id, buf) == extent_protocol::OK) {
    return generate_unique_inum(is_file); // Retry if inum already exists
  }
  return rand_id; // Return the unique inum
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lc->acquire(inum);

  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

  release:
  lc->release(inum);

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lc -> acquire(inum);
  int r = OK;

  printf("[yfs_client]getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;
  release:
  lc -> release(inum);
  return r;
}

yfs_client::inum
yfs_client::ilookup(inum di, std::string name)
{
  lc -> acquire(di);
  inum inum = 0;
  std::string buf;

  std::istringstream ist;
  std::string file_name;
  unsigned long long file_inum;

  // Check if di is a directory
  if (!isdir(di)) {
    printf("ilookup: %016llx is not a directory\n", di);
    goto release;
  }
  if (ec->get(di, buf) != extent_protocol::OK) {
    printf("ilookup: get failed for %016llx\n", di);
    goto release;
  }

  ist.str(buf);
  while (ist >> file_inum >> file_name) {
    if (file_name == name) {
      inum = file_inum;
      printf("ilookup: found %s in %016llx\n", name.c_str(), di);
      break;
    }
  }
  release:
  lc -> release(di);
  return inum;
}


yfs_client::status 
yfs_client::createfile(inum parent, const char *name, inum &file_inum){
  return add_entry_to_filesystem(parent, file_inum, name, true);
}

yfs_client::status 
yfs_client::add_entry_to_filesystem(inum parent, inum &entry_inum, const char *name, bool is_file)
{
  lc->acquire(parent); // Lock parent first
  std::string buf;
  int ret = ec->get(parent, buf);
  if (ret != extent_protocol::OK) {
    printf("[yfs_client] Error getting parent %016llx\n", parent);
    lc->release(parent);
    return ret;
  }

  // // Check if file already exists
  // std::istringstream ist(buf);
  // std::string existing_inum, existing_name;
  // while (ist >> existing_inum >> existing_name) {
  //   if (existing_name == name) {
  //     lc->release(parent);
  //     return IOERR; // File exists
  //   }
  // }

  // Safe to proceed, generate new inum
  entry_inum = generate_unique_inum(is_file);
  lc->acquire(entry_inum);

  std::ostringstream ost;
  ost << entry_inum << " " << name << "\n";
  buf += ost.str();

  ret = ec->put(parent, buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }

  // Create file
  ret = ec->put(entry_inum, "");

release:
  lc->release(entry_inum);
  lc->release(parent);
  return ret;
}

yfs_client::status
yfs_client::makedir(inum parent, const char *name, inum &dir_inum)
{
  printf("makedir %016llx %s\n", parent, name);
  return add_entry_to_filesystem(parent, dir_inum, name, false);
}

yfs_client::status
yfs_client::readdir(inum dir, std::vector<dirent> &dir_files)
{
  lc -> acquire(dir);
  std::string buf;
  std::istringstream ist;
  dirent e;
  std::string file_inum;
  std::string file_name;

  int ret = ec->get(dir, buf);

  if (ret != extent_protocol::OK) {
    goto release;
  }

  ist.str(buf);
  while (ist >> file_inum >> file_name) {
    e.inum = n2i(file_inum);
    e.name = file_name;
    dir_files.push_back(e);
  }
  release:
  lc -> release(dir);
  return ret;
}

yfs_client::status
yfs_client::setsize(inum inum, unsigned long long size)
{
  lc -> acquire(inum);
  std::string buf;
  int ret = ec->get(inum, buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }
  if (size > buf.size()) {
    buf.resize(size, '\0');
  }
  else{
    buf.resize(size);
  }
  ret = ec->put(inum, buf);

  release:
  lc -> release(inum);
  return ret;
}


yfs_client::status
yfs_client::write(inum inum, const char *buf, size_t size, off_t off, size_t &bytes_written)
{
  lc -> acquire(inum);
  std::string file_buf;
  int ret = ec->get(inum, file_buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }
  if (off + size > file_buf.size()) {
    file_buf.resize(off + size, '\0');
  }
  memcpy(&file_buf[off], buf, size);
  ret = ec->put(inum, file_buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }
  bytes_written = size;
  release:
  lc -> release(inum);
  return ret;

}

yfs_client::status
yfs_client::read(inum inum, std::string &buf, size_t size, off_t off)
{
  lc ->acquire(inum);
  int ret = ec->get(inum, buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }
  if (off >= buf.size()) {
    buf = "";
    ret = OK;
    goto release;
  }
  if (off + size > buf.size()) {
    size = buf.size() - off;
  }
  buf = buf.substr(off, size);

  release:
  lc -> release(inum);
  return ret;
}


yfs_client::status
yfs_client::unlink(inum parent, const char *name)
{
  lc -> acquire(parent);
  std::string buf;

  std::istringstream ist;
  std::ostringstream ost;
  std::string file_inum;
  std::string file_name;
  std::string inum_to_remove_string;
  inum inum_to_remove;

  bool found = false;

  int ret = ec->get(parent, buf);
  if (ret != extent_protocol::OK) {
    goto release;
  }

  ist.str(buf);
  while (ist >> file_inum >> file_name) {
    if (file_name == name) {
      found = true;
      inum_to_remove_string = file_inum;
      continue; // skip this entry
    }
    ost << file_inum << " " << file_name << "\n";
  }
  inum_to_remove = n2i(inum_to_remove_string);
  lc -> acquire(inum_to_remove);
  if (!found) {
    printf("File not found with name %s\n", name);
    ret = extent_protocol::NOENT; // file not found
  }
  buf = ost.str();
  ret = ec->put(parent, buf);
  if (ret != extent_protocol::OK) {
    printf("Error putting parent %016llx\n", parent);
    goto release;
  }
  ret = ec->remove(inum_to_remove); // remove the file
  if (ret != extent_protocol::OK) {
    printf("Error removing file %016llx\n", n2i(file_inum));
    goto release;
  }

  release:
  lc -> release(inum_to_remove);
  lc -> release(parent);
  return ret;
}
