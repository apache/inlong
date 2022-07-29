/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef DATAPROXY_SDK_BASE_LOGGER_H_
#define DATAPROXY_SDK_BASE_LOGGER_H_

#include <stdint.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "sdk_constant.h"

namespace dataproxy_sdk
{
static const uint32_t kMBSize = 1024 * 1024;
const uint32_t kPid           = getpid();

class Logger;

Logger& getLogger();

// only show fileName
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define LOG_LEVEL(level, fmt, ...)                                                                                               \
    {                                                                                                                            \
        if (dataproxy_sdk::getLogger().enableLevel(level))                                                                           \
        {                                                                                                                        \
            dataproxy_sdk::getLogger().write("[pid:%d][%s][%s:%s:%d]" fmt, kPid, dataproxy_sdk::Logger::level2String(level),             \
                                         __FILENAME__, __func__, __LINE__, ##__VA_ARGS__);                                       \
        }                                                                                                                        \
    }

#define LOG_TRACE(fmt, ...) LOG_TDBUSCAPI(dataproxy_sdk::getLogger(), dataproxy_sdk::Logger::kLogTrace, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) LOG_TDBUSCAPI(dataproxy_sdk::getLogger(), dataproxy_sdk::Logger::kLogDebug, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) LOG_TDBUSCAPI(dataproxy_sdk::getLogger(), dataproxy_sdk::Logger::kLogInfo, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) LOG_TDBUSCAPI(dataproxy_sdk::getLogger(), dataproxy_sdk::Logger::kLogWarn, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_TDBUSCAPI(dataproxy_sdk::getLogger(), dataproxy_sdk::Logger::kLogError, fmt, ##__VA_ARGS__)

#define LOG_TDBUSCAPI(logger, level, fmt, ...)                                                                                   \
    {                                                                                                                            \
        if (logger.enableLevel(level))                                                                                           \
        {                                                                                                                        \
            logger.write("[pid:%d][%s][%s:%s:%d]" fmt, kPid, dataproxy_sdk::Logger::level2String(level), __FILENAME__, __func__,     \
                         __LINE__, ##__VA_ARGS__);                                                                               \
        }                                                                                                                        \
    }

class Logger
{
  public:
    enum Level {
        kLogError = 0,
        kLogWarn  = 1,
        kLogInfo  = 2,
        kLogDebug = 3,
        kLogTrace = 4,
    };

  private:
    uint32_t file_max_size_;
    uint32_t file_num_;
    uint8_t level_;
    uint8_t output_type_;  //2->file, 1->console
    bool enable_limit_;
    std::string base_path_;
    std::string log_name_;

    std::string instance_;

  public:
    Logger()
        : file_max_size_(10)
        , file_num_(10)
        , level_(kLogInfo)
        , output_type_(2)
        , enable_limit_(true)
        , base_path_("./logs/")
        , instance_("DataProxySDK")
    {
    }

    ~Logger() {}

    bool init(uint32_t file_max_size,
              uint32_t file_num,
              uint8_t level,
              uint8_t output_type,
              bool enable_limit,
              const std::string& base_path,
              const std::string& logname = constants::kLogName);
    bool write(const char* sFormat, ...) __attribute__((format(printf, 2, 3)));
    inline bool writeStream(const std::string& msg) { return writeCharStream(msg.c_str()); }
    inline bool enableLevel(Level level) { return ((level <= level_) ? true : false); }
    static const char* level2String(Level level)
    {
        static const char* level_names[] = {
            "ERROR", "WARN", "INFO", "DEBUG", "TRACE",
        };
        return level_names[level];
    }

  private:
    void setUp();
    bool writeCharStream(const char* msg);
};

}  // namespace dataproxy_sdk

#endif  // CPAI_BASE_LOGGER_H_