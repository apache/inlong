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

#include "logger.h"

#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/layout.h>
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#include <stdarg.h>

#include <string>

#include "singleton.h"

namespace dataproxy_sdk
{
Logger& getLogger() { return Singleton<Logger>::instance(); }

bool Logger::init(uint32_t file_max_size,
                  uint32_t file_num,
                  uint8_t level,
                  uint8_t output_type,
                  bool enable_limit,
                  const std::string& base_path,
                  const std::string& logname)
{
    file_max_size_ = file_max_size;
    file_num_      = file_num;
    level_         = level;
    output_type_   = output_type;
    enable_limit_  = enable_limit;
    base_path_     = base_path;
    log_name_      = logname;
    setUp();
    return true;
}

bool Logger::write(const char* format, ...)
{
    char buf[8192];
    va_list ap;
    va_start(ap, format);
    vsnprintf(buf, sizeof(buf) - 1, format, ap);
    va_end(ap);
    return writeCharStream(buf);
}

bool Logger::writeCharStream(const char* log)
{
    auto logger = log4cplus::Logger::getInstance(instance_);
    logger.forcedLog(log4cplus::TRACE_LOG_LEVEL, log);
    return true;
}

void Logger::setUp()
{
    bool immediate_flush = true;
    std::string pattern = "[%D{%Y-%m-%d %H:%M:%S.%q}]%m%n";
    auto logger_d       = log4cplus::Logger::getInstance(instance_);
    logger_d.removeAllAppenders();
    logger_d.setLogLevel(log4cplus::TRACE_LOG_LEVEL);

    if (output_type_ == 2)
    {  //file
        log4cplus::SharedAppenderPtr fileAppender(new log4cplus::RollingFileAppender(
            base_path_ + log_name_, file_max_size_ * kMBSize, file_num_, immediate_flush, true));
        std::unique_ptr<log4cplus::Layout> layout(new log4cplus::PatternLayout(pattern));
        fileAppender->setLayout(std::move(layout));
        logger_d.addAppender(fileAppender);
    }
    else
    {  //console
        log4cplus::SharedAppenderPtr consoleAppender(new log4cplus::ConsoleAppender());
        consoleAppender->setLayout(std::unique_ptr<log4cplus::Layout>(new log4cplus::SimpleLayout()));
        logger_d.addAppender(consoleAppender);
    }
}

}  // namespace dataproxy_sdk
