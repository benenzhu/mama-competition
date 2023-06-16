// TODO: 需要监控的是model.done 不过判题好像不用也。。。。
#include "ModelSliceReader.h"
#include "hdfs.h"
#include "hdfs.hpp"
#include "helloworld.grpc.pb.h"
#include "helloworld.pb.h"
#include "structs.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <fstream>
#include <functional>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/status.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>






using alimama::proto::ModelService;
using alimama::proto::MYRequest;
using alimama::proto::MYResponse;
using alimama::proto::Request;
using alimama::proto::Request2;
using alimama::proto::Response;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using std::string;

#ifdef CHECKSUM
vector<std::atomic<int>> stats(5);
std::atomic<int> every_k;
#endif // CHECKSUM
inline void print() { puts(""); }

template <typename T, typename... Types>
void print(const T &first, const Types &...args) {
  std::cout << first << " ";
  print(args...);
}

typedef unsigned long long ull;
int LoadModel(int model_index = 0, const string &path = "");
enum class CMD : int {
  LoadModel,
  Barrier,
};

int NODE_INDEX = 0; // 当前使用的INDEX
int NODE_NUMBER_ALL = 6;
int SERVER_PORT_START = 50000;

int MODEL_INDEX_NOW = 0;
ThreadPool pool(10);
ThreadPool rpcpool(6);
ThreadPool readpool(2);
string max_path_now = "";
char *model_data[5][240];


std::atomic<int> control_flow;

std::chrono::steady_clock::time_point start;
std::chrono::steady_clock::time_point origin_time;


void get_partition(vector<slice> *sliceVec, char *s, int slice_index);
class GreeterServiceImpl final : public ModelService::Service {
  Status CustomCMD(ServerContext *, const MYRequest *request,
                   MYResponse *response) override {
    CMD cmd = CMD(request->cmd());
    if (cmd == CMD::LoadModel) {
      log("client start load model");
      string path = request->data();
      LoadModel(MODEL_INDEX_NOW + 1, path);

      etcdc.set("/my/" + std::to_string(NODE_INDEX) + "/" +
                    std::to_string(MODEL_INDEX_NOW),
                "1");
      log("client finished load model");

    } else if (cmd == CMD::Barrier) {
      MODEL_INDEX_NOW += 1;
      log("change model to index:" << MODEL_INDEX_NOW);
    }
    response->set_ok(0);
    return Status::OK;
  }
  // TODO: 设置一下 slice属于哪个分片
  Status GetSlice(ServerContext *, const Request2 *request,
                  Response *response) override {
    /* auto reqdata = std::chrono::steady_clock::now(); */

    // 1. 分开构造 request;
    vector<slice> getVec;
    int pos_now = 0;
    int slice_index = request->slice_index();
    for (int j = 0; j < request->slice_request_size(); j++) {
      auto i = request->slice_request(j);
      auto slice_partition = i.slice_partition();
      auto data_start = i.data_start();
      auto data_len = i.data_len();
      getVec.push_back(slice(slice_partition, data_len, data_start, pos_now));
      pos_now += data_len;
    }
    // log("准备完毕");
    char *s = new char[pos_now];
    int now = getVec.size();
    now++;
    get_partition(&getVec, s, slice_index);
    // 3. 返回结果
    //
    response->set_status(0);
    // log("s.size ::" << s.size());
    response->add_slice_data(s, pos_now);

    /* auto reqdataend = std::chrono::steady_clock::now(); */

    delete[] s;

    return Status::OK;
  }

  Status Get(ServerContext *, const Request *request,
             Response *response) override {
    // log("receive request");
    if( __builtin_expect(start == origin_time,0)) start = std::chrono::steady_clock::now();


    auto ret =  std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    if( __builtin_expect(control_flow * 2.5 / ret >= 1, 0)){
      //fail掉
      response->set_status(-1);
      return Status::CANCELLED;
    }
    control_flow++;
    // log("s.size ::" << s.size());
    // TODO 改一下

    MyTimer timer;
    // 1. 分开构造 request;

    vector<vector<slice>> getVec(NODE_NUMBER_ALL);
    int pos_now = 0;
    for (int j = 0; j < request->slice_request_size(); j++) {
      auto i = request->slice_request(j);
      auto slice_partition = i.slice_partition();
      auto data_start = i.data_start();
      auto data_len = i.data_len();
      getVec[slice_partition % NODE_NUMBER_ALL].push_back(
          slice(slice_partition, data_len, data_start, pos_now));
      pos_now += data_len;
    }
    // log("准备完毕");
    char *s = new char[pos_now];

    int model_index_this = MODEL_INDEX_NOW;
    // 2. 发送请求 并等待
    vector<std::future<void>> waitVec;
    for (int i = 0; i < NODE_NUMBER_ALL; i++) {
      /* get_partition(getVec[i], s); */
      int now = getVec[i].size();
      now++;

      waitVec.push_back(
          pool.enqueue(get_partition, &getVec[i], s, model_index_this));
    }

    // log("wait i");
    for (auto &i : waitVec) {
      i.get();
    }
    // log("i passed");

// 3. 数据检验
#ifdef CHECKSUM
    int now = 0;
    // log("s addr:"<< &s);
    for (int j = 0; j < request->slice_request_size(); j++) {
      auto i = request->slice_request(j);
      int slice_partition = i.slice_partition();
      int data_start = i.data_start();
      int data_len = i.data_len();
      int num = *((int *)(s + now));
      if (num != slice_partition * 10000000 + data_start + model_index_this) {
        log("wrong data:"
            << "now:" << num
            << "expect:" << slice_partition * 10000000 + data_start
            << "index:" << slice_partition << "start:" << data_start
            << " model_index_this" << model_index_this);

      } else {
        /* log("right data:" */
        /*     << "now:" << num */
        /*     << "expect:" << slice_partition * 10000 + data_start */
        /*     << "index:" << slice_partition << "start:" << data_start << "
         * model_index_this" << model_index_this); */
      }
      now += data_len;
    }
    stats[model_index_this]++;
    if (every_k++ % 500 == 0) {
      log("0:" << stats[0] << "  1:" << stats[1] << "  2:" << stats[2]);
    }
#endif
    // 3. 返回结果
    //
    if (model_index_this != MODEL_INDEX_NOW) {
      response->set_status(-1);
    } else {
      response->set_status(0);
    }
    // log("s.size ::" << s.size());
    // TODO 改一下
    for (int i = 0; i < pos_now; i += 16) {
      response->add_slice_data(s + i, 16);
    }
    return Status::OK;
  }
};

class GreeterClient {
public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(ModelService::NewStub(channel)) {}

  int CustomCommand(CMD cmd, const string &msg) {
    alimama::proto::MYRequest req;
    alimama::proto::MYResponse reply;
    req.set_cmd((int)cmd);
    req.set_data(msg);
    ClientContext context;
    Status status = stub_->CustomCMD(&context, req, &reply);
    if (status.ok()) {
      return 0;
    } else {
      return -1;
    }
  }
  std::string GetSlice(vector<slice> &sliceV, int SLICE_INDEX) {
    Request2 req;
    req.set_slice_index(SLICE_INDEX);
    req.mutable_slice_request()->Reserve(sliceV.size());
    for (auto &i : sliceV) {
      auto *slice = req.add_slice_request();
      slice->set_slice_partition(i.slice_index);
      slice->set_data_start(i.data_start);
      slice->set_data_len(i.data_len);
    }

    Response reply;
    ClientContext context;
    Status status = stub_->GetSlice(&context, req, &reply);
    if (status.ok()) {
      return reply.slice_data(0);
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string Get(vector<slice> &sliceV) {
    Request req;
    req.mutable_slice_request()->Reserve(sliceV.size());
    for (auto &i : sliceV) {
      auto *slice = req.add_slice_request();
      slice->set_slice_partition(i.slice_index);
      slice->set_data_start(i.data_start);
      slice->set_data_len(i.data_len);
    }

    Response reply;
    ClientContext context;
    Status status = stub_->Get(&context, req, &reply);
    if (status.ok()) {
      return reply.slice_data(0);
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

private:
  std::unique_ptr<ModelService::Stub> stub_;
};

GreeterClient *rpc_client[6];

// TODO: check是否感知 index_this
void get_partition(vector<slice> *sliceVec, char *s, int SLICE_INDEX = 0) {
  // log("开始拷贝 partition");
  // assert(log("zhuzhu"));
  if (sliceVec->empty())
    return;
  // log("s addr:" << (s + 10));
  int rpc_index = (*sliceVec)[0].slice_index % NODE_NUMBER_ALL;
  if (rpc_index == NODE_INDEX) {
    // print(sliceVec->size());
    for (ull j = 0; j < sliceVec->size(); j++) {
      slice i = (*sliceVec)[j];
      memcpy((void *)(s + i.pos_final),
             model_data[SLICE_INDEX][i.slice_index] + i.data_start, i.data_len);
      // log("copy data" << *((int *)(s + i.pos_final)) << "index" <<
      // i.slice_index
      //                << "start" << i.data_start);
    }
  } else {
    string ret_s = rpc_client[rpc_index]->GetSlice(*sliceVec, SLICE_INDEX);
    int now = 0;
    for (auto &i : *sliceVec) {
      memcpy((void *)(s + i.pos_final), ret_s.c_str() + now, i.data_len);
      now += i.data_len;
      // log("copy data" << *((int *)(s + i.pos_final)));
    }
  }
  // log("结束拷贝 partition");
}

string getMaxPath() {
  vector<fileInfo> files = hdfs.listDir("/");
  std::string hdfs_path = "/";

  // getmaxPath
  string target_path = "";

  for (ull i = 0; i < files.size(); i++) {
    auto &file = files[i];
    // log(file.mName);
    string path_name = file.mName;
    if (file.kind == kObjectKindDirectory) {
      string done_path = path_name + "/model.done";
      if (hdfs.exist(done_path)) {
        if (target_path == "" || path_name > target_path) {
          target_path = path_name;
        }
      }
    }
  }

  return target_path;
}

void readFromremote(string line, int model_index, int cnt, int shard_size) {
  auto vec = split(line, ':'); // i :  slice:model_slice.15,size:6400000
  auto A = ModelSliceReader();
  auto name = split(vec[1], ',')[0]; // model_slice.15     --- name
  string remote_path = max_path_now + "/" + name;
  string local_path = split(max_path_now, '/').back() + name;
  log("开始下载 远端:" << remote_path << "本地:" << local_path);
  hdfs.download2(remote_path, local_path);

  log("Load: " << remote_path << "本地:" << local_path);
  A.Load("./" + local_path);
  A.Read(0, shard_size - 1, model_data[model_index][cnt]);

  log("unload model: local_path: " << local_path << ' ');
  A.Unload();
  log("Done");
}

int LoadModel(int model_index, const string &path) {

  /* 1. 赋值 target_path 到目标 path */
  max_path_now = path;
  if (max_path_now == "") {
    if (model_index != 0) {
      log("err, wrong model index" << model_index);
    }
    log("find max pathing...");
    while (max_path_now = getMaxPath(), max_path_now == "")
      ;
    log("find max path: " << max_path_now);
  }
  /* 1. ----------- */

  /* 2. 下载 model 的meta */
  string model_meta_path = max_path_now + "/model.meta";
  string model_meta_local_path =
      split(max_path_now, '/').back() + "_model.meta";
  hdfs.download2(model_meta_path, model_meta_local_path);
  /* 2. ------------ */

  /* 3. 读取 解析 meta 文件*/
  std::ifstream file(model_meta_local_path); // 打开文件
  std::stringstream buffer;                  // 创建缓冲区
  buffer << file.rdbuf();                    // 将文件内容读入缓冲区
  std::string meta = buffer.str(); // 将缓冲区内容转换为string类型
  auto lines = split(meta, '\n');
  int cnt = 0;
  /* 3. --------------- */

  /* 4. 预分配空间 */
  static int shard_size = stoi(split(lines[3], ':')[2]);
  if (model_index == 0) {
    int cnt = 0;
    for (auto &i : lines) {
      if (i[0] == 's')
        cnt++;
    }
    for (int i = NODE_INDEX; i < cnt; i += NODE_NUMBER_ALL) {
      for (int j = 0; j < 2; j++) {
        model_data[j][i] = new char[shard_size + 2];
      }
    }
  }
  /* ------------------ */
  // log("wait i");

  vector<std::future<void>> waitVec;
  auto t = MyTimer();
  /* 5. 读取模型    */
  for (ull j = 0; j < lines.size(); j++) {
    if (lines[j].size() > 10 && lines[j][0] == 's') {
      if (cnt % NODE_NUMBER_ALL == NODE_INDEX) { // 需要读的
        if (model_index >= 2) {                  // 切换时候删除
          model_data[model_index][cnt] = model_data[model_index - 2][cnt];
        }
        log("input to waitVec");
        waitVec.push_back(readpool.enqueue(readFromremote, lines[j],
                                           model_index, cnt, shard_size));
      }
      ++cnt;
    }
  }

  for (auto &i : waitVec) {
    log("done 1");
    i.get();
  }
  log("下载用时used :" << t.get_time()/1000 << "ms");
  /* -------------------- */

  return 0;
}

inline bool have_roll_back() {
  if (hdfs.exist("/rollback.version")) {
    return true;
  }
  return false;
}

inline string get_roll_back_version() {
  return "/" + hdfs.readFile("/rollback.version");
}

void sendCmd(int i, CMD cmd, const string msg) {
  rpc_client[i]->CustomCommand(cmd, msg);
}

void sendMessage(CMD cmd, const string &msg) {

  vector<std::future<void>> waitVec;
  for (int i = 0; i < NODE_NUMBER_ALL; i++) {
    /* get_partition(getVec[i], s); */
    waitVec.push_back(rpcpool.enqueue(sendCmd, i, cmd, msg));
  }
  // log("wait i");
  for (auto &i : waitVec) {
    i.get();
  }
}

void bgThread(int) {
  log("bgthread start");
  while (true) {
    string path = "";
    if (have_roll_back()) {
      path = get_roll_back_version();
      while (path.back() == ' ' || path.back() == '\n')
        path.pop_back();
      // log("rollback path" << path);
    } else {
      path = getMaxPath();
    }

    if (path != max_path_now) {
      log("准备开始版本切换");
      log("start loadModel" << path << " " << max_path_now);
      // 发送rpc开始读取数据
      sendMessage(CMD::LoadModel, path);
      { // 轮训是否读取完成
        int target_index = MODEL_INDEX_NOW;
        for (int i = 0; i < NODE_NUMBER_ALL; i++) {
          string s;
          while (s = etcdc.get("/my/" + std::to_string(i) + "/" +
                               std::to_string(target_index)),
                 s.empty())
            ;
          log("节点" << i << "切换完成");
        }
      }

      // TODO: 这里的开发工作还没写
      //  开启barrier切换版本
      log("Start send Message barrier for version change");
      log("版本切换完成");
      sendMessage(CMD::Barrier, "");
      //
    } else {
#ifdef CHECKSUM
      // log("path:" << path << " " << max_path_now);

#endif // DEBUG
    }
  }
}

void registerServer();
void RunServer() {
  std::string server_address("0.0.0.0:" +
                             std::to_string(SERVER_PORT_START + NODE_INDEX));
  GreeterServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening" << server_address << std::endl;

  if (NODE_INDEX == 0) {
    auto t = std::thread(bgThread, 1);
    t.detach();
    log("bgthread in queue");
  }
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  /* pool.enqueue([] { */
  /*   sleep(1); */
  /*   vector<slice> sliceVec; */
  /*   for (int i = 0; i < 10; i++) { */
  /*     sliceVec.push_back(slice(i, 16, i * 4 * 5, 8)); */
  /*   } */
  /*   string s = rpc_client[0]->Get(sliceVec); */
  /* }); */
  registerServer();
  log("zhuzhu");

  server->Wait();
  log("zhuzhu");
}

void registerServer() {

  const std::string local_ip = getLocalIP();
  const std::string EXTERNAL_ADDRESS =
      local_ip + std::string(":") +
      std::to_string(SERVER_PORT_START + NODE_INDEX);

  std::string key = std::string("/my_server/") + std::to_string(NODE_INDEX);
  std::string value(EXTERNAL_ADDRESS);
  etcdc.set(key, EXTERNAL_ADDRESS);

  log("读取etcd注册信息");
  vector<string> server(6);
  /* 开始注册 server */
  for (int i = 0; i < NODE_NUMBER_ALL; i++) {
    /* if (i != NODE_INDEX) { */
    if (true) {
      log("resister server" << i);
      string s;
      while (s = etcdc.get("/my_server/" + std::to_string(i)), s.empty())
        ;
      rpc_client[i] = new GreeterClient(
          grpc::CreateChannel(s, grpc::InsecureChannelCredentials()));
      server[i] = s;
    } else {
      log("regist self rpc_client");
      rpc_client[i] = new GreeterClient(grpc::CreateChannel(
          EXTERNAL_ADDRESS, grpc::InsecureChannelCredentials()));
    }
  }
  if (NODE_INDEX == 0) {
    log("主节点注册");
    server[0] = EXTERNAL_ADDRESS;
    for (int i = 0; i < NODE_NUMBER_ALL; i++) {
      log("注册" << server[i]);
      etcdc.set(std::string("/services/modelservice/") + server[i], "");
    }
  }
}

int main() {

  NODE_INDEX = stoi(string(std::getenv("NODE_ID")));
  NODE_NUMBER_ALL = stoi(string(std::getenv("NODE_NUM")));
  NODE_INDEX -= 1; // 排序有问题
  log("INDEX" << NODE_INDEX);
  log("PARTITION_NUMBER" << NODE_NUMBER_ALL);

  LoadModel(MODEL_INDEX_NOW, "");

  // TODO: 改了下本地是没有server的，怎么注册一下？
  RunServer();

  log("at here?");
  sleep(100000);
  return 0;
}
