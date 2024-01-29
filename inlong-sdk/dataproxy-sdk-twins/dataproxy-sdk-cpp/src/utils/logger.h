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

#pragma once
#ifndef INLONG_SDK_LOGGER_H
#define INLONG_SDK_LOGGER_H

#include "../config/sdk_conf.h"
#include <iostream>
#include <log4cplus/fileappender.h>
#include <log4cplus/helpers/loglog.h>
#include <log4cplus/initializer.h>
#include <log4cplus/log4cplus.h>
#include <log4cplus/logger.h>
#include <log4cplus/loglevel.h>
#include <sys/stat.h>
using namespace log4cplus::helpers;

namespace inlong {
static const char kDefaultPath[] = "./";
static const char kLogName[] = "/inlong-sdk.log";
static bool CheckPath(const std::string &path) {
  struct stat st_stat = {0};
  int ret = stat(path.c_str(), &st_stat);
  if (ret && errno != ENOENT) {
    std::cout << "Check directory error:" << strerror(errno) << std::endl;
    return false;
  }

  if ((ret && errno == ENOENT) || (!ret && !S_ISDIR(st_stat.st_mode))) {
    if (mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      std::cout << "Crate directory error:" << strerror(errno) << std::endl;
      return false;
    }
  }
  return true;
}

static void initLog4cplus() {
  std::string log_path = SdkConfig::getInstance()->log_path_;
  if (!CheckPath(log_path)) {
    log_path = kDefaultPath;
  }
  std::string log_name = log_path + kLogName;
  log4cplus::SharedAppenderPtr the_append(new log4cplus::RollingFileAppender(
      log_name, SdkConfig::getInstance()->log_size_,
      SdkConfig::getInstance()->log_num_));
  log4cplus::tstring pattern =
      LOG4CPLUS_TEXT("%D{%m-%d %H:%M:%S:%Q} %m [%b:%L]\n");
  the_append->setLayout(
      std::auto_ptr<log4cplus::Layout>(new log4cplus::PatternLayout(pattern)));
  switch (SdkConfig::getInstance()->log_level_) {
  case 0:
    log4cplus::Logger::getRoot().setLogLevel(log4cplus::ERROR_LOG_LEVEL);
    break;
  case 1:
    log4cplus::Logger::getRoot().setLogLevel(log4cplus::WARN_LOG_LEVEL);
    break;
  case 2:
    log4cplus::Logger::getRoot().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    break;
  case 3:
    log4cplus::Logger::getRoot().setLogLevel(log4cplus::DEBUG_LOG_LEVEL);
    break;
  default:
    log4cplus::Logger::getRoot().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    break;
  }

  log4cplus::Logger::getRoot().addAppender(the_append);
}

#define LOG_ERROR(x)                                                           \
  {                                                                            \
    std::stringstream _log_ss;                                                 \
    _log_ss << "ERROR " << x;                                                  \
    LOG4CPLUS_ERROR(log4cplus::Logger::getRoot(), _log_ss.str());              \
  }

#define LOG_WARN(x)                                                            \
  {                                                                            \
    std::stringstream _log_ss;                                                 \
    _log_ss << "WARN " << x;                                                   \
    LOG4CPLUS_WARN(log4cplus::Logger::getRoot(), _log_ss.str());               \
  }

#define LOG_INFO(x)                                                            \
  {                                                                            \
    std::stringstream _log_ss;                                                 \
    _log_ss << "INFO " << x;                                                   \
    LOG4CPLUS_INFO(log4cplus::Logger::getRoot(), _log_ss.str());               \
  }

#define LOG_DEBUG(x)                                                           \
  {                                                                            \
    std::stringstream _log_ss;                                                 \
    _log_ss << "DEBUG " << x;                                                  \
    LOG4CPLUS_DEBUG(log4cplus::Logger::getRoot(), _log_ss.str());              \
  }

} // namespace inlong
#endif // INLONG_SDK_LOGGER_H