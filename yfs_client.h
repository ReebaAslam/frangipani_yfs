#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


  class yfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);

  inum generate_unique_inum(bool is_file);
  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  yfs_client::status createfile(inum, const char *, inum &);
  yfs_client::status add_entry_to_filesystem(yfs_client::inum parent, yfs_client::inum &file_inum, const char *name, bool is_file);
  yfs_client::status readdir(inum, std::vector<dirent> &);
  yfs_client::status setsize(inum, unsigned long long size);
  yfs_client::status write(inum, const char *, size_t, off_t, size_t &);
  yfs_client::status read(inum, std::string &, size_t, off_t);
  yfs_client::status makedir(inum, const char *, inum &);
  yfs_client::status unlink(inum, const char *);
};

#endif 
