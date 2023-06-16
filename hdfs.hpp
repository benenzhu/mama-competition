#include "hdfs.h"
#include "utils.hpp"
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
using std::ifstream;
using std::string;
using std::vector;
namespace SYS {

inline int command(string s) {
  log("execute " << s);
  return system(s.c_str());
}
} // namespace SYS
  //
  //
struct fileInfo {
  tObjectKind kind;
  string mName;
  fileInfo(tObjectKind k, const string &n) : kind(k), mName(n){};
};

class __hdfs__struct__ {
private:
  hdfsFS fs;

public:
  __hdfs__struct__() { fs = hdfsConnect("hdfs://namenode:9000", 0); }

  inline int download(const string &path, const string &local_path) {
    {
      std::ifstream file(local_path);
      if (file.good()) { // 判断文件是否存在
        log("skip down " << local_path);
        return 0;
      }
    }

    log("download " << path << " to " << local_path);
    return SYS::command("hdfs dfs -fs hdfs://namenode:9000/ -get -f " + path +
                        " " + local_path);
  }

  inline int download2(const string &path, const string &local_path) {

    {
      std::ifstream file(local_path);
      if (file.good()) { // 判断文件是否存在
        log("skip down " << local_path);
        return 0;
      }
    }
    hdfsCopy(fs, path.c_str(), nullptr, local_path.c_str());

    log("download " << path << " to " << local_path);
    return SYS::command("hdfs dfs -fs hdfs://namenode:9000/ -get -f " + path +
                        " " + local_path);
  }

  inline std::vector<fileInfo> listDir(const string &path) {
    int num_files;
    hdfsFileInfo *file_info = hdfsListDirectory(fs, path.c_str(), &num_files);
    if (!file_info) {
      log("err");
    }

    vector<fileInfo> fileVec;
    fileVec.reserve(num_files);
    for (int i = 0; i < num_files; i++) {
      // log("havefile " << file_info[i].mName);
      string name = file_info[i].mName;
      auto a = fileInfo(file_info[i].mKind, name);
      fileVec.emplace_back(a);
    }

    hdfsFreeFileInfo(file_info, num_files);
    return fileVec;
  };

  inline bool exist(const string &path) {
    if (hdfsExists(fs, path.c_str()) == 0) {
      return true;
    }
    return false;
  }

  inline string readFile(const string &filepath) {
    char buffer[1000];
    memset((void *)buffer, 0, sizeof(buffer));

    auto readFile =
        hdfsOpenFile(fs, filepath.c_str(), O_RDONLY, 1000, 1000, 1000);
    if (!readFile) {
      log("Fail");
    }
    auto tSize = hdfsRead(fs, readFile, (void *)buffer, 1000);
    if (tSize < 0) {
      log("Fail");
    }
    hdfsCloseFile(fs, readFile);
    return string(buffer);
  }
};

static __hdfs__struct__ hdfs;

namespace HDFS {} // namespace HDFS
