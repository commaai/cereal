#include <cassert>
#include <cstring>
#include <iostream>
#include <chrono>
#include <csignal>

#include "cereal/services.h"
#include "cereal/messaging/impl_msgq.h"

using namespace std::chrono;

static bool service_exists(std::string path){
  return services.count(path) > 0;
}

MSGQContext::MSGQContext() {
}

MSGQContext::~MSGQContext() {
}

void MSGQMessage::init(size_t sz) {
  size = sz;
  data = new char[size];
}

void MSGQMessage::init(char * d, size_t sz) {
  size = sz;
  data = new char[size];
  memcpy(data, d, size);
}

void MSGQMessage::takeOwnership(char * d, size_t sz) {
  size = sz;
  data = d;
}

void MSGQMessage::close() {
  if (size > 0){
    delete[] data;
  }
  size = 0;
}

MSGQMessage::~MSGQMessage() {
  this->close();
}

int MSGQSubSocket::connect(Context *context, std::string endpoint, std::string address, bool conflate, bool check_endpoint){
  assert(context);
  assert(address == "127.0.0.1");

  if (check_endpoint && !service_exists(std::string(endpoint))){
    std::cout << "Warning, " << std::string(endpoint) << " is not in service list." << std::endl;
  }

  q = new msgq_queue_t;
  int r = msgq_new_queue(q, endpoint.c_str(), DEFAULT_SEGMENT_SIZE);
  if (r != 0){
    return r;
  }

  msgq_init_subscriber(q);

  if (conflate){
    q->read_conflate = true;
  }

  timeout = -1;

  return 0;
}

Message *MSGQSubSocket::receive(bool non_blocking) {
  msgq_msg_t msg;
  int rc = msgq_msg_recv(&msg, q);

  if (rc == 0 && !non_blocking) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigprocmask(SIG_BLOCK, &mask, nullptr);

    int64_t timieout_ns = ((timeout != -1) ? timeout : 100) * 1000000;
    auto start = steady_clock::now();

    // Continue receiving messages until timeout or interruption by SIGINT or SIGTERM
    while (rc == 0 && timieout_ns > 0) {
      struct timespec ts {timieout_ns / 1000000000, timieout_ns % 1000000000};
      int ret = sigtimedwait(&mask, nullptr, &ts);
      if (ret == SIGINT || ret == SIGTERM) {
        raise(ret);  // Raise the signal again to ensure it's not missed
        break;       // Exit the loop
      } else if (ret == -1 && errno == EAGAIN && timeout != -1) {
        break;  // Timed out
      }

      rc = msgq_msg_recv(&msg, q);
      timieout_ns -= (timeout == -1 ? 0 : duration_cast<nanoseconds>(steady_clock::now() - start).count());
    }
    sigprocmask(SIG_UNBLOCK, &mask, nullptr);
  }

  if (rc > 0) {
    MSGQMessage *r = new MSGQMessage;
    r->takeOwnership(msg.data, msg.size);
    return r;
  }
  return nullptr;
}

void MSGQSubSocket::setTimeout(int t){
  timeout = t;
}

MSGQSubSocket::~MSGQSubSocket(){
  if (q != NULL){
    msgq_close_queue(q);
    delete q;
  }
}

int MSGQPubSocket::connect(Context *context, std::string endpoint, bool check_endpoint){
  assert(context);

  if (check_endpoint && !service_exists(std::string(endpoint))){
    std::cout << "Warning, " << std::string(endpoint) << " is not in service list." << std::endl;
  }

  q = new msgq_queue_t;
  int r = msgq_new_queue(q, endpoint.c_str(), DEFAULT_SEGMENT_SIZE);
  if (r != 0){
    return r;
  }

  msgq_init_publisher(q);

  return 0;
}

int MSGQPubSocket::sendMessage(Message *message){
  msgq_msg_t msg;
  msg.data = message->getData();
  msg.size = message->getSize();

  return msgq_msg_send(&msg, q);
}

int MSGQPubSocket::send(char *data, size_t size){
  msgq_msg_t msg;
  msg.data = data;
  msg.size = size;

  return msgq_msg_send(&msg, q);
}

bool MSGQPubSocket::all_readers_updated() {
  return msgq_all_readers_updated(q);
}

MSGQPubSocket::~MSGQPubSocket(){
  if (q != NULL){
    msgq_close_queue(q);
    delete q;
  }
}


void MSGQPoller::registerSocket(SubSocket * socket){
  assert(num_polls + 1 < MAX_POLLERS);
  polls[num_polls].q = (msgq_queue_t*)socket->getRawSocket();

  sockets.push_back(socket);
  num_polls++;
}

std::vector<SubSocket*> MSGQPoller::poll(int timeout){
  std::vector<SubSocket*> r;

  msgq_poll(polls, num_polls, timeout);
  for (size_t i = 0; i < num_polls; i++){
    if (polls[i].revents){
      r.push_back(sockets[i]);
    }
  }

  return r;
}
