#include <stdio.h>
#include <string>
#include <set>
#include <thread>
#include <chrono>
#include <iostream>

#include "tubemq/tubemq_client.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/tubemq_message.h"
#include "tubemq/tubemq_return.h"
#include "utils.h"

using namespace std;
using namespace tubemq;

using tubemq::ProducerConfig;
using tubemq::TubeMQProducer;

struct MessageSentCallback {  
  static AtomicLong TOTAL_COUNTER;
  static AtomicLong SENT_SUCC_COUNTER;
  static AtomicLong SENT_FAIL_COUNTER;
  static AtomicLong SENT_EXCEPT_COUNTER;
  
  void operator()(const ErrorCode& error_code) {
    TOTAL_COUNTER.IncrementAndGet();
    if (error_code.Value() == err_code::kErrSuccess) {
      SENT_SUCC_COUNTER.IncrementAndGet();
    } else {
      SENT_FAIL_COUNTER.IncrementAndGet();
    }
	}

  static void ShowSentResult() {
    std::cout << "Finished, total sent: " << TOTAL_COUNTER.Get() << ", sent successfully: " << SENT_SUCC_COUNTER.Get() << std::endl;
  }
};

AtomicLong MessageSentCallback::TOTAL_COUNTER;
AtomicLong MessageSentCallback::SENT_SUCC_COUNTER;
AtomicLong MessageSentCallback::SENT_FAIL_COUNTER;
AtomicLong MessageSentCallback::SENT_EXCEPT_COUNTER;

const uint64_t MSG_COUNT = 100;
bool SYNC_PRODUCTION = false;
const uint32_t MSG_DATA_SIZE = 2048;

int main(int argc, char* argv[]) {
	if (argc < 3) {
		printf("\n must ./comd master_addr topic_name [config_file_path]");
		return -1;
	}

	string master_addr = argv[1];
	string topic_name = argv[2];
	string conf_file = "/tubemq-cpp/conf/client.conf";
	if (argc > 3) {
		conf_file = argv[3];
	}
  uint32_t msg_count = MSG_COUNT;
  if (argc > 4) {
    msg_count = std::atoi(argv[4]);
  }

	TubeMQProducer producer;
	set<string> topic_list;
	ProducerConfig producer_config;
	TubeMQServiceConfig service_config;
	service_config.SetLogPrintLevel(2);
	producer_config.SetRpcReadTimeoutMs(20000);
	
	string err_info;
	bool result;
	result = producer_config.SetMasterAddrInfo(err_info, master_addr);
	if (!result) {
		printf("Set Master AddrInfo failure: %s\n", err_info.c_str());
    return -1;
	}

	result = StartTubeMQService(err_info, service_config);
	if (!result) {
		printf("\n StartTubeMQService failure: %s, please check the log for detailed error code and message.", err_info.c_str());
    return -1;
	}

	result = producer.Start(err_info, producer_config);
	if (!result) {
    printf("Initial producer failure, error is: %s \n", err_info.c_str());
    return -2;
  }

  result = producer.Publish(err_info, {topic_name});
  std::this_thread::sleep_for(std::chrono::seconds(10));

  uint64_t send_count = 0;
  std::string send_data;
  Utils::BuildTestData(send_data, MSG_DATA_SIZE);
  std::string curr_time = std::to_string(Utils::CurrentTimeMillis());
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < msg_count; i++) {
    Message message(topic_name, send_data.c_str(), send_data.size());
    message.PutSystemHeader(std::to_string(send_count), curr_time);
    send_count++;
    if (SYNC_PRODUCTION) {
      bool is_success = producer.SendMessage(err_info, message);
      MessageSentCallback::TOTAL_COUNTER.IncrementAndGet();
      if (is_success) {
        MessageSentCallback::SENT_SUCC_COUNTER.IncrementAndGet();
      } else {
        MessageSentCallback::SENT_FAIL_COUNTER.IncrementAndGet();
      }
    } else {
      producer.SendMessage(message, MessageSentCallback());
    }
  }

  while (MessageSentCallback::TOTAL_COUNTER.Get() < (long)msg_count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto stop = std::chrono::steady_clock::now();
  double duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count() * 0.000001;
  std::cout << "Sent costs " << duration << " seconds!!!" << std::endl;
  
  MessageSentCallback::ShowSentResult();
  
	producer.ShutDown();
	
	result = StopTubeMQService(err_info);
  if (!result) {
    printf("\n *** StopTubeMQService failure, reason is %s ", err_info.c_str());
  }

	printf("Finish test producer!\n");
  return 0;
}