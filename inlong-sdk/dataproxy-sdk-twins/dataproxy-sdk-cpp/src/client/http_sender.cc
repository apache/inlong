#include "../../include/http_sender.h"
#include <curl/curl.h>
#include <iostream>
namespace inlong {
HttpSender::HttpSender() {}
HttpSender::~HttpSender() {}
int HttpSender::Send(const std::string& url, const std::string& body, int timeout) {
    CURL* curl = curl_easy_init();
    if (!curl) return -1;
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);
    int ret = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return ret;
}
} 