
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#include <sys/types.h>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
using std::string;
#include "log.hpp"
using std::vector;

static inline 
unsigned int hashSample(int a, int b) {
    return a^b;
}

static inline 
unsigned int myhash(int a, int b) {
    return hashSample(a, b);
}



class __ETCD_client{
private:
  etcd::Client c;
public:
  inline void set(const string& key, const string& value){
    log("etcd 设置" << key << " " << value);
    c.set(key, value).get();
    log("etcd 设置" << key << " " << value << "done");
  }
  inline string get(const string& key){
    return c.get(key).get().value().as_string();
  }
  inline __ETCD_client(): c("http://etcd:2379"){ }
};

static __ETCD_client etcdc;


inline std::string getLocalIP() {
  struct ifaddrs *ifAddrStruct = NULL;
  void *tmpAddrPtr = NULL;
  std::string localIP;
  getifaddrs(&ifAddrStruct);
  while (ifAddrStruct != NULL) {
    if (ifAddrStruct->ifa_addr->sa_family == AF_INET) {
      tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      std::string interfaceName(ifAddrStruct->ifa_name);
      if (interfaceName == "en0" || interfaceName == "eth0") {
        return addressBuffer;
      }
    }
    ifAddrStruct = ifAddrStruct->ifa_next;
  }
  return "";
}


class MyTimer {
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point last;

  public:
    MyTimer() {
        reset();
    }
    double  get_time() {
        auto now = std::chrono::steady_clock::now();
        auto ret =  std::chrono::duration<double, std::milli>(now - last).count();
        last = now;
        return ret;
    }
    void reset() {
        start = std::chrono::steady_clock::now();
        last = start;
    }
};


inline vector<string> split(std::string param, char seg = '\n') {
  vector<string> arr;
  std::string temp = "";
  for (auto it = param.begin(); it != param.end(); it++) {
    if (*it == seg) {
      arr.push_back(temp);
      temp = "";
    } else {
      temp += *it;
    }
  }
  arr.push_back(temp);
  return arr;
}

class ThreadPool {
public:
  ThreadPool(size_t num_threads) : stop(false) {
    for (size_t i = 0; i < num_threads; ++i) {
      workers.emplace_back([this] {
        for (;;) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(this->queue_mutex);
            this->condition.wait(
                lock, [this] { return this->stop || !this->tasks.empty(); });
            if (this->stop && this->tasks.empty())
              return;
            task = std::move(this->tasks.front());
            this->tasks.pop();
          }
          task();
        }
      });
    }
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers)
      worker.join();
  }

  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args)

      -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<return_type> res = task->get_future();
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (stop)
        throw std::runtime_error("enqueue on stopped ThreadPool");
      tasks.emplace([task] { (*task)(); });
    }
    condition.notify_one();
    return res;
  }

private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop;
};
