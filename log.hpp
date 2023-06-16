
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>



#include <string>
inline std::string get_time() {
    // 获取当前时间
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    auto now_tm = std::localtime(&now_c);

    // 格式化时间
    std::ostringstream ss;
    ss << std::put_time(now_tm, "%H:%M:%S");

    return ss.str();
}

#define log(msg) std::cout << get_time() <<  ' '<< __FILE__ << ":" << __LINE__ << "->  " << msg << std::endl
