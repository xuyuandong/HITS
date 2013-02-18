#include "vertical_offline_util.h"

#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>

#include "base/logging.h"
#include "base/flags.h"
#include "base/string_util.h"
#include "base/file_base.h"

DEFINE_string(hdfs_server_path,
              "hdfs://XXXXXXX:9000",
              "hdfs server path");

static const int kGoralShardShiftBits = 58;
static const int kGoralShardPartCount = 1024;
static const char* kHdfsPathPrefix =
    "/production/indexing_pipeline/goral/base_doc/shard_";
static const char* kHdfsSSTablePartPrefix = "mr_doc_joiner-part-";
static const char* kHdfsSSTablePartPostfix = "-01024";

using std::string;

namespace crawl {

string GetDocIDStringFromUrl(const string& url) {
  indexing::DocID doc_id = indexing::ComputeDocIdFromUrl(url);
  return Uint64ToString(doc_id);
}

string GetSSTablePathFromUrl(const string& url, map<string, string>* dir_cache_map) {
  indexing::DocID doc_id = indexing::ComputeDocIdFromUrl(url);

  int shard_id = doc_id >> kGoralShardShiftBits;
  string path = StringPrintf("%s%s%d/",
                      FLAGS_hdfs_server_path.c_str(),
                      kHdfsPathPrefix,
                      shard_id);

  bool need_hdfs_check = true;

  if (dir_cache_map != NULL) {
    map<string, string>::iterator iter = dir_cache_map->find(path);
    if (iter != dir_cache_map->end()) {
      path = iter->second;
      need_hdfs_check = false;
      VLOG(4) << "Find dir by cache OK";
    }
  }
  
  if (need_hdfs_check) {
    if (!file::FileBase::Exists(path)) {
      LOG(ERROR) << "Unable to find sstable path: " << path;
    }

    vector<string> dirs;
    file::Status stat = file::FileBase::GetDirsInDir(path, &dirs);
    if (!stat.ok()) {
      LOG(ERROR) << "Fail to get dirs in dir: " << path
        << " error: " << stat.ToString();
    }
    if (dirs.empty()) {
      LOG(ERROR) << "Fail to get dirs in dir: " << path << "(returns empty)";
    }

    if (dirs.size() > 1) {
      sort(dirs.begin(), dirs.end());
    }
    string lastdir = dirs.back().c_str();

    if (dir_cache_map != NULL)
      dir_cache_map->insert(make_pair(path, lastdir));

    path = lastdir;
  }

  int part_id = doc_id % kGoralShardPartCount;
  StringAppendF(&path,
      "/%s%05d%s",
      kHdfsSSTablePartPrefix,
      part_id,
      kHdfsSSTablePartPostfix);

  return path;
}

string GetDateString(int days, bool norm) {
  int year, mon, day;
  time_t t = time(NULL) - 24*3600*days;
  struct tm* m = localtime(&t);
  year = m->tm_year + 1900;
  mon = m->tm_mon + 1;
  day = m->tm_mday;
  char buf[100];
  if (norm)
    (void) sprintf(buf, "%04d_%02d_%02d", year, mon, day);
  else
    (void) sprintf(buf, "%04d_%d_%d", year, mon, day);
  return string(buf);
}


}  // namespace crawl
