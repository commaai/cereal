#include <iostream>
#include <string>
#include <cassert>
#include <csignal>
#include <map>

typedef void (*sighandler_t)(int sig);

#include "services.h"

#include "impl_msgq.h"
#include "impl_zmq.h"

void sigpipe_handler(int sig) {
  assert(sig == SIGPIPE);
  std::cout << "SIGPIPE received" << std::endl;
}

static std::vector<std::string> get_services() {
  std::vector<std::string> name_list;

  for (const auto& it : services) {
    std::string name = it.name;
    if (name == "plusFrame" || name == "uiLayoutState") continue;
    name_list.push_back(name);
  }

  return name_list;
}


int main(void){
  signal(SIGPIPE, (sighandler_t)sigpipe_handler);

  bool unbridge = true;
  auto endpoints = get_services();

  std::map<SubSocket*, PubSocket*> sub2pub;

  Context *zmq_context = new ZMQContext();
  Context *msgq_context = new MSGQContext();
  Poller *poller;
  if (unbridge) {
    poller = new ZMQPoller();
  } else {
    poller = new MSGQPoller();
  }

  for (auto endpoint: endpoints){
    SubSocket * sub_sock;
    PubSocket * pub_sock;
    if (unbridge) {
      sub_sock = new ZMQSubSocket();
      sub_sock->connect(zmq_context, endpoint, "127.0.0.1", false);  // TODO: add argument for IP (of laptop)

      pub_sock = new MSGQPubSocket();
      pub_sock->connect(msgq_context, endpoint);
    } else {
      sub_sock = new MSGQSubSocket();
      sub_sock->connect(msgq_context, endpoint, "127.0.0.1", false);

      pub_sock = new ZMQPubSocket();
      pub_sock->connect(zmq_context, endpoint);
    }
    poller->registerSocket(sub_sock);
    sub2pub[sub_sock] = pub_sock;
  }


  while (true){
    for (auto sub_sock : poller->poll(100)){
      Message * msg = sub_sock->receive();
      if (msg == NULL) continue;
      sub2pub[sub_sock]->sendMessage(msg);
      delete msg;
    }
  }
  return 0;
}
