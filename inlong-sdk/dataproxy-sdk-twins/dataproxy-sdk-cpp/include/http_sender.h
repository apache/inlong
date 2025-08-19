#pragma once
#include <string>
namespace inlong {
class HttpSender {
public:
    HttpSender();
    ~HttpSender();
    int Send(const std::string& url, const std::string& body, int timeout = 5);
};
} 