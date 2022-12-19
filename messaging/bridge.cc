#include <cassert>
#include <csignal>
#include <condition_variable>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <string>

typedef void (*sighandler_t)(int sig);

#include "impl_msgq.h"
#include "impl_zmq.h"
#include "services.h"

std::mutex mutex;
int client_conected = 0;
std::condition_variable cv;
std::atomic<bool> do_exit = false;

static void set_do_exit(int sig) {
  do_exit = true;
}

void sigpipe_handler(int sig) {
  assert(sig == SIGPIPE);
  std::cout << "SIGPIPE received" << std::endl;
}

static std::vector<std::string> get_services(std::string whitelist_str, bool zmq_to_msgq) {
  std::vector<std::string> service_list;
  for (const auto& it : services) {
    std::string name = it.name;
    bool in_whitelist = whitelist_str.find(name) != std::string::npos;
    if (name == "plusFrame" || name == "uiLayoutState" || (zmq_to_msgq && !in_whitelist)) {
      continue;
    }
    service_list.push_back(name);
  }
  return service_list;
}

static std::string recvZMQMessage(void *sock) {
  zmq_msg_t msg;
  zmq_msg_init(&msg);
  std::string ret;
  if (zmq_msg_recv(&msg, sock, 0) > 0) {
    ret.assign((char *)zmq_msg_data(&msg), zmq_msg_size(&msg));
  }
  zmq_msg_close(&msg);
  return ret;
}

void zmq_monitor_thread(void *ctx, const std::vector<std::unique_ptr<PubSocket>> &sockets) {
  std::vector<zmq_pollitem_t> pollitems;
  for (int i = 0; i < sockets.size(); ++i) {
    std::string addr = "inproc://op-bridge-monitor-" + std::to_string(i);
    void *raw_sock = dynamic_cast<ZMQPubSocket*>(sockets[i].get())->getRawSocket();
    int ret = zmq_socket_monitor(raw_sock, addr.c_str(), ZMQ_EVENT_ACCEPTED | ZMQ_EVENT_DISCONNECTED);
    assert(ret == 0);
    void *s = zmq_socket(ctx, ZMQ_PAIR);
    assert(s);
    ret = zmq_connect(s, addr.c_str());
    assert(ret == 0);
    pollitems.emplace_back(zmq_pollitem_t{.socket = s, .events = ZMQ_POLLIN});
  }

  while (!do_exit) {
    int ret = zmq_poll(pollitems.data(), pollitems.size(), 1000);
    if (ret < 0) continue;

    for (auto &p : pollitems) {
      if (p.revents & ZMQ_POLLIN) {
        // First frame in message contains event number and value
        std::string frame = recvZMQMessage(p.socket);
        if (frame.empty()) continue;

        uint16_t evt = *(uint16_t *)(frame.data());

        // Second frame in message contains event address
        frame = recvZMQMessage(p.socket);
        if (frame.empty()) continue;

        std::unique_lock lk(mutex);
        if (evt & ZMQ_EVENT_ACCEPTED) {
          ++client_conected;
        } else if (evt & ZMQ_EVENT_DISCONNECTED) {
          if (--client_conected <= 0) {
            do_exit = true;
            printf("all clients disconnected, closing bridge.\n");
          }
        }
        cv.notify_all();
      }
    }
  }

  for (int i = 0; i < pollitems.size(); ++i) {
    void *raw_sock = dynamic_cast<ZMQPubSocket *>(sockets[i].get())->getRawSocket();
    zmq_socket_monitor(raw_sock, nullptr, 0);
    zmq_close(pollitems[i].socket);
  }

  cv.notify_all();
}

int bridge(const std::string &ip, const std::string &whitelist_str, bool zmq_to_msgq) {
  std::unique_ptr<Poller> poller;
  std::unique_ptr<Context> pub_context;
  std::unique_ptr<Context> sub_context;
  if (zmq_to_msgq) {  // republishes zmq debugging messages as msgq
    poller = std::make_unique<ZMQPoller>();
    pub_context = std::make_unique<MSGQContext>();
    sub_context = std::make_unique<ZMQContext>();
  } else {
    poller = std::make_unique<MSGQPoller>();
    pub_context = std::make_unique<ZMQContext>();
    sub_context = std::make_unique<MSGQContext>();
  }

  const auto &endpoints = get_services(whitelist_str, zmq_to_msgq);
  std::vector<std::unique_ptr<PubSocket>> pub_sockets(endpoints.size());
  std::vector<std::unique_ptr<SubSocket>> sub_sockets(endpoints.size());
  std::map<SubSocket *, PubSocket *> sub2pub;
  for (int i = 0; i < endpoints.size(); ++i) {
    if (zmq_to_msgq) {
      pub_sockets[i] = std::make_unique<MSGQPubSocket>();
      sub_sockets[i] = std::make_unique<ZMQSubSocket>();
    } else {
      pub_sockets[i] = std::make_unique<ZMQPubSocket>();
      sub_sockets[i] = std::make_unique<MSGQSubSocket>();
    }
    pub_sockets[i]->connect(pub_context.get(), endpoints[i]);
    sub2pub[sub_sockets[i].get()] = pub_sockets[i].get();
  }

  std::future<void> monitor_future;
  if (!zmq_to_msgq) {
    printf("waiting for zmq subscribers...\n");
    monitor_future = std::async(std::launch::async, zmq_monitor_thread, pub_context->getRawContext(), std::ref(pub_sockets));
    std::unique_lock lk(mutex);
    cv.wait(lk, [&]() { return client_conected > 0 || do_exit; });
    if (do_exit) return 0;
  }

  for (int i = 0; i < sub_sockets.size(); ++i) {
    sub_sockets[i]->connect(sub_context.get(), endpoints[i], ip, false);
    poller->registerSocket(sub_sockets[i].get());
  }

  printf("start bridge\n");

  while (!do_exit) {
    for (auto sub_sock : poller->poll(100)) {
      Message * msg = sub_sock->receive();
      if (msg == NULL) continue;
      int ret;
      do {
        ret = sub2pub[sub_sock]->sendMessage(msg);
      } while (ret == -1 && errno == EINTR && !do_exit);
      assert(ret >= 0 || do_exit);
      delete msg;

      if (do_exit) break;
    }
  }
  return 0;
}

int main(int argc, char **argv) {
  signal(SIGPIPE, (sighandler_t)sigpipe_handler);
  signal(SIGINT, (sighandler_t)set_do_exit);
  signal(SIGTERM, (sighandler_t)set_do_exit);

  bool zmq_to_msgq = argc > 2;
  std::string ip = zmq_to_msgq ? argv[1] : "127.0.0.1";
  std::string whitelist_str = zmq_to_msgq ? std::string(argv[2]) : "";

  return bridge(ip, whitelist_str, zmq_to_msgq);
}
