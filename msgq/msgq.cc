#include <iostream>
#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <string>
#include <limits>
#include <vector>

#include <poll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdio.h>

#include "msgq/msgq.h"

uint64_t msgq_get_uid(void){
  // getentropy doesn't need a file descriptor, so this works even when the process is out of them
  uint64_t uid = 0;
  while (uid == 0) {
    if (getentropy(&uid, sizeof(uid)) != 0) abort();
  }
  return uid;
}

// A thread blocked in msgq_poll waits on a unix datagram socket named after a random uid,
// and adds that uid to the waiters list of each queue it polls. Writers clear the list
// and send an empty datagram to every uid that was in it.
static socklen_t wake_addr(uint64_t uid, sockaddr_un *addr) {
  memset(addr, 0, sizeof(*addr));
  addr->sun_family = AF_UNIX;
#ifdef __APPLE__
  snprintf(addr->sun_path, sizeof(addr->sun_path), "/tmp/msgq_wake_%016llx", (unsigned long long)uid);
  return sizeof(*addr);
#else
  // Abstract namespace: no file to clean up, and the name is released when the socket closes
  int n = snprintf(addr->sun_path + 1, sizeof(addr->sun_path) - 1, "msgq_wake_%016llx", (unsigned long long)uid);
  return offsetof(sockaddr_un, sun_path) + 1 + n;
#endif
}

static int wake_socket() {
  int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (fd >= 0) {
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    fcntl(fd, F_SETFL, O_NONBLOCK);
  }
  return fd;
}

static void wake_reader(int fd, uint64_t uid) {
  sockaddr_un addr;
  socklen_t len = wake_addr(uid, &addr);

  char c = 0;
  if (fd >= 0 && (sendto(fd, &c, sizeof(c), 0, reinterpret_cast<sockaddr*>(&addr), len) >= 0 ||
                  errno == ENOENT || errno == ECONNREFUSED)) {
    return;  // sent, or nobody is listening
  }

  // Unread datagrams count against the sender's buffer, so readers that stopped reading
  // can fill it up. Retry with a fresh socket. If that fails too, the reader's queue
  // is full and a wakeup is already pending.
  int tmp = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (tmp < 0) return;
  sendto(tmp, &c, sizeof(c), MSG_DONTWAIT, reinterpret_cast<sockaddr*>(&addr), len);
  close(tmp);
}

struct WakeSocket {
  uint64_t uid = 0;
  int fd = -1;
  pid_t pid = 0;

  void close_fd(bool unlink_path) {
    if (fd < 0) return;
    close(fd);
    fd = -1;
#ifdef __APPLE__
    if (unlink_path) {
      sockaddr_un addr;
      wake_addr(uid, &addr);
      unlink(addr.sun_path);
    }
#else
    UNUSED(unlink_path);
#endif
  }

  ~WakeSocket() { close_fd(pid == getpid()); }
};

// The calling thread's wake socket, with fd -1 if it couldn't be created
static WakeSocket &thread_wake_socket() {
  thread_local WakeSocket w;
  if (w.fd < 0 || w.pid != getpid()) {
    // New thread, or forked from the thread that made it. Leave the parent's socket alone.
    w.close_fd(false);
    w.pid = getpid();
    w.uid = msgq_get_uid();

    sockaddr_un addr;
    socklen_t len = wake_addr(w.uid, &addr);
    int fd = wake_socket();
    if (fd >= 0 && bind(fd, reinterpret_cast<sockaddr*>(&addr), len) < 0) {
      close(fd);
      fd = -1;
    }
    w.fd = fd;
  }
  return w;
}

// Wake up every thread blocked in a poll on this queue
static void wake_waiters(msgq_queue_t *q, int fd) {
  for (size_t i = 0; i < NUM_WAITERS; i++){
    uint64_t uid = *q->waiters[i];
    if (uid != 0 && (uid = q->waiters[i]->exchange(0)) != 0) {
      wake_reader(fd, uid);
    }
  }
}

// Add a waiter to the queue's list, returning its index or -1 if the list is full
static int add_waiter(msgq_queue_t *q, uint64_t uid) {
  for (int i = 0; i < NUM_WAITERS; i++) {
    uint64_t expected = 0;
    if (q->waiters[i]->compare_exchange_strong(expected, uid)) return i;
  }
  return -1;
}

static void drain_wake_socket(int fd) {
  char buf[64];
  while (recv(fd, buf, sizeof(buf), 0) >= 0) {}
}

int msgq_msg_init_size(msgq_msg_t * msg, size_t size){
  msg->size = size;
  msg->data = new(std::nothrow) char[size];

  return (msg->data == NULL) ? -1 : 0;
}


int msgq_msg_init_data(msgq_msg_t * msg, char * data, size_t size) {
  int r = msgq_msg_init_size(msg, size);

  if (r == 0)
    memcpy(msg->data, data, size);

  return r;
}

int msgq_msg_close(msgq_msg_t * msg){
  if (msg->size > 0)
    delete[] msg->data;

  msg->size = 0;

  return 0;
}

void msgq_reset_reader(msgq_queue_t * q){
  int id = q->reader_id;
  q->read_valids[id]->store(true);
  q->read_pointers[id]->store(*q->write_pointer);
}

void msgq_wait_for_subscriber(msgq_queue_t *q){
  while (*q->num_readers == 0){
    // wait for subscriber
  }
}

int msgq_new_queue(msgq_queue_t * q, const char * path, size_t size){
  assert(size < 0xFFFFFFFF); // Buffer must be smaller than 2^32 bytes

#ifdef __APPLE__
  std::string base_path = "/tmp/msgq_";
#else
  std::string base_path = "/dev/shm/msgq_";
#endif
  const char* prefix = std::getenv("OPENPILOT_PREFIX");
  if (prefix) {
    base_path += std::string(prefix) + "/";
  }
  std::string full_path = base_path + path;

  auto fd = open(full_path.c_str(), O_RDWR | O_CREAT, 0664);
  if (fd < 0) {
    std::cout << "Warning, could not open: " << full_path << std::endl;
    return -1;
  }

  int rc = ftruncate(fd, size + sizeof(msgq_header_t));
  if (rc < 0){
    close(fd);
    return -1;
  }

  int mmap_flags = MAP_SHARED;
#ifdef MAP_POPULATE
  if (std::getenv("MSGQ_PREALLOC")) {
    mmap_flags |= MAP_POPULATE;
  }
#endif

  char * mem = (char*)mmap(NULL, size + sizeof(msgq_header_t), PROT_READ | PROT_WRITE, mmap_flags, fd, 0);
  close(fd);

  if (mem == MAP_FAILED){
    return -1;
  }

  q->mmap_p = mem;

  msgq_header_t *header = (msgq_header_t *)mem;

  // Setup pointers to header segment
  q->num_readers = reinterpret_cast<std::atomic<uint64_t>*>(&header->num_readers);
  q->write_pointer = reinterpret_cast<std::atomic<uint64_t>*>(&header->write_pointer);
  q->write_uid = reinterpret_cast<std::atomic<uint64_t>*>(&header->write_uid);

  for (size_t i = 0; i < NUM_READERS; i++){
    q->read_pointers[i] = reinterpret_cast<std::atomic<uint64_t>*>(&header->read_pointers[i]);
    q->read_valids[i] = reinterpret_cast<std::atomic<uint64_t>*>(&header->read_valids[i]);
    q->read_uids[i] = reinterpret_cast<std::atomic<uint64_t>*>(&header->read_uids[i]);
  }
  for (size_t i = 0; i < NUM_WAITERS; i++){
    q->waiters[i] = reinterpret_cast<std::atomic<uint64_t>*>(&header->waiters[i]);
  }

  q->data = mem + sizeof(msgq_header_t);
  q->size = size;
  q->reader_id = -1;

  q->endpoint = path;
  q->read_conflate = false;

  return 0;
}

void msgq_close_queue(msgq_queue_t *q){
  if (q->mmap_p != NULL){
    munmap(q->mmap_p, q->size + sizeof(msgq_header_t));
  }
  if (q->send_fd >= 0) close(q->send_fd);
  q->send_fd = -1;
}


void msgq_init_publisher(msgq_queue_t * q) {
  //std::cout << "Starting publisher" << std::endl;
  uint64_t uid = msgq_get_uid();

  // Socket for waking readers. One per publisher, so readers that stop reading
  // their wakeups only fill up the buffer of the publishers that woke them.
  if (q->send_fd < 0) q->send_fd = wake_socket();

  *q->write_uid = uid;
  *q->num_readers = 0;

  for (size_t i = 0; i < NUM_READERS; i++){
    *q->read_valids[i] = false;
    *q->read_uids[i] = 0;
  }

  // Wake up readers of the old publisher, so they reconnect
  wake_waiters(q, q->send_fd);

  q->write_uid_local = uid;
}

void msgq_init_subscriber(msgq_queue_t * q) {
  assert(q != NULL);
  assert(q->num_readers != NULL);

  uint64_t uid = msgq_get_uid();

  // Get reader id
  while (true){
    uint64_t cur_num_readers = *q->num_readers;
    uint64_t new_num_readers = cur_num_readers + 1;

    // No more slots available. Reset all subscribers to kick out inactive ones
    if (new_num_readers > NUM_READERS){
      //std::cout << "Warning, evicting all subscribers!" << std::endl;
      *q->num_readers = 0;

      for (size_t i = 0; i < NUM_READERS; i++){
        *q->read_valids[i] = false;
        *q->read_uids[i] = 0;
      }

      // Wake up readers in a poll, so they reconnect
      wake_waiters(q, -1);
      continue;
    }

    // Use atomic compare and swap to handle race condition
    // where two subscribers start at the same time
    if (std::atomic_compare_exchange_strong(q->num_readers,
                                            &cur_num_readers,
                                            new_num_readers)){
      q->reader_id = cur_num_readers;
      q->read_uid_local = uid;

      // We start with read_valid = false,
      // on the first read the read pointer will be synchronized with the write pointer
      *q->read_valids[cur_num_readers] = false;
      *q->read_pointers[cur_num_readers] = 0;
      *q->read_uids[cur_num_readers] = uid;
      break;
    }
  }

  //std::cout << "New subscriber id: " << q->reader_id << " uid: " << q->read_uid_local << " " << q->endpoint << std::endl;
  msgq_reset_reader(q);
}

int msgq_msg_send(msgq_msg_t * msg, msgq_queue_t *q){
  // Die if we are no longer the active publisher
  if (q->write_uid_local != *q->write_uid){
    std::cout << "Killing old publisher: " << q->endpoint << std::endl;
    errno = EADDRINUSE;
    return -1;
  }

  uint64_t total_msg_size = ALIGN(msg->size + sizeof(int64_t));

  // We need to fit at least three messages in the queue,
  // then we can always safely access the last message
  assert(3 * total_msg_size <= q->size);

  uint64_t num_readers = *q->num_readers;

  uint32_t write_cycles, write_pointer;
  UNPACK64(write_cycles, write_pointer, *q->write_pointer);

  char *p = q->data + write_pointer; // add base offset

  // Check remaining space
  // Always leave space for a wraparound tag for the next message, including alignment
  int64_t remaining_space = q->size - write_pointer - total_msg_size - sizeof(int64_t);
  if (remaining_space <= 0){
    // Write -1 size tag indicating wraparound
    *(int64_t*)p = -1;

    // Invalidate all readers that are beyond the write pointer
    // TODO: should we handle the case where a new reader shows up while this is running?
    for (uint64_t i = 0; i < num_readers; i++){
      uint64_t read_pointer = *q->read_pointers[i];
      uint64_t read_cycles = read_pointer >> 32;
      read_pointer &= 0xFFFFFFFF;

      if ((read_pointer > write_pointer) && (read_cycles != write_cycles)) {
        *q->read_valids[i] = false;
      }
    }

    // Update global and local copies of write pointer and write_cycles
    write_pointer = 0;
    write_cycles = write_cycles + 1;
    PACK64(*q->write_pointer, write_cycles, write_pointer);

    // Set actual pointer to the beginning of the data segment
    p = q->data;
  }

  // Invalidate readers that are in the area that will be written
  uint64_t start = write_pointer;
  uint64_t end = ALIGN(start + sizeof(int64_t) + msg->size);

  for (uint64_t i = 0; i < num_readers; i++){
    uint32_t read_cycles, read_pointer;
    UNPACK64(read_cycles, read_pointer, *q->read_pointers[i]);

    if ((read_pointer >= start) && (read_pointer < end) && (read_cycles != write_cycles)) {
      *q->read_valids[i] = false;
    }
  }


  // Write size tag
  std::atomic<int64_t> *size_p = reinterpret_cast<std::atomic<int64_t>*>(p);
  *size_p = msg->size;

  // Copy data
  memcpy(p + sizeof(int64_t), msg->data, msg->size);
  __sync_synchronize();

  // Update write pointer
  uint32_t new_ptr = ALIGN(write_pointer + msg->size + sizeof(int64_t));
  PACK64(*q->write_pointer, write_cycles, new_ptr);

  wake_waiters(q, q->send_fd);

  return msg->size;
}


int msgq_msg_ready(msgq_queue_t * q){
 start:
  int id = q->reader_id;
  assert(id >= 0); // Make sure subscriber is initialized

  // Also reconnect if num_readers was reset after we claimed our slot, since
  // publishers only invalidate slots below num_readers
  if (q->read_uid_local != *q->read_uids[id] || (uint64_t)id >= *q->num_readers){
    //std::cout << q->endpoint << ": Reader was evicted, reconnecting" << std::endl;
    msgq_init_subscriber(q);
    goto start;
  }

  // Check valid
  if (!*q->read_valids[id]){
    msgq_reset_reader(q);
    goto start;
  }

  uint32_t read_cycles, read_pointer;
  UNPACK64(read_cycles, read_pointer, *q->read_pointers[id]);
  UNUSED(read_cycles);

  uint32_t write_cycles, write_pointer;
  UNPACK64(write_cycles, write_pointer, *q->write_pointer);
  UNUSED(write_cycles);

  // Check if new message is available
  return (read_pointer != write_pointer);
}

int msgq_msg_recv(msgq_msg_t * msg, msgq_queue_t * q){
 start:
  int id = q->reader_id;
  assert(id >= 0); // Make sure subscriber is initialized

  // Also reconnect if num_readers was reset after we claimed our slot, since
  // publishers only invalidate slots below num_readers
  if (q->read_uid_local != *q->read_uids[id] || (uint64_t)id >= *q->num_readers){
    //std::cout << q->endpoint << ": Reader was evicted, reconnecting" << std::endl;
    msgq_init_subscriber(q);
    goto start;
  }

  // Check valid
  if (!*q->read_valids[id]){
    msgq_reset_reader(q);
    goto start;
  }

  uint32_t read_cycles, read_pointer;
  UNPACK64(read_cycles, read_pointer, *q->read_pointers[id]);

  uint32_t write_cycles, write_pointer;
  UNPACK64(write_cycles, write_pointer, *q->write_pointer);
  UNUSED(write_cycles);

  char * p = q->data + read_pointer;

  // Check if new message is available
  if (read_pointer == write_pointer) {
    msg->size = 0;
    return 0;
  }

  // Read potential message size
  std::atomic<int64_t> *size_p = reinterpret_cast<std::atomic<int64_t>*>(p);
  std::int64_t size = *size_p;

  // Check if the size that was read is valid
  if (!*q->read_valids[id]){
    msgq_reset_reader(q);
    goto start;
  }

  // If size is -1 the buffer was full, and we need to wrap around
  if (size == -1){
    read_cycles++;
    PACK64(*q->read_pointers[id], read_cycles, 0);
    goto start;
  }

  // crashing is better than passing garbage data to the consumer
  // the size will have weird value if it was overwritten by data accidentally
  assert((uint64_t)size < q->size);
  assert(size > 0);

  uint32_t new_read_pointer = ALIGN(read_pointer + sizeof(std::int64_t) + size);

  // If conflate is true, check if this is the latest message, else start over
  if (q->read_conflate){
    if (new_read_pointer != write_pointer){
      // Update read pointer
      PACK64(*q->read_pointers[id], read_cycles, new_read_pointer);
      goto start;
    }
  }

  // Copy message
  if (msgq_msg_init_size(msg, size) < 0)
    return -1;

  __sync_synchronize();
  memcpy(msg->data, p + sizeof(int64_t), size);
  __sync_synchronize();

  // Update read pointer
  PACK64(*q->read_pointers[id], read_cycles, new_read_pointer);

  // Check if the actual data that was copied is valid
  if (!*q->read_valids[id]){
    msgq_msg_close(msg);
    msgq_reset_reader(q);
    goto start;
  }


  return msg->size;
}



int msgq_poll(msgq_pollitem_t * items, size_t nitems, int timeout){
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
  std::vector<int> slots;

  while (true) {
    int num = 0;
    for (size_t i = 0; i < nitems; i++) {
      items[i].revents = msgq_msg_ready(items[i].q);
      num += items[i].revents;
    }
    if (num > 0) return num;

    int timeout_ms = -1;
    if (timeout >= 0) {
      auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
      timeout_ms = std::max<int64_t>(remaining.count(), 0);
      if (timeout_ms == 0) return 0;
    }

    // Ask for a wakeup before checking again, so a message sent in between isn't missed
    WakeSocket &w = thread_wake_socket();
    slots.resize(nitems);
    bool can_block = (w.fd >= 0);
    for (size_t i = 0; i < nitems; i++) {
      slots[i] = add_waiter(items[i].q, w.uid);
      can_block &= (slots[i] >= 0);
      num += msgq_msg_ready(items[i].q);
    }

    if (num == 0) {
      // Without a wakeup, fall back to checking periodically
      int poll_ms = can_block ? timeout_ms : (timeout_ms < 0 ? 10 : std::min(timeout_ms, 10));
      pollfd pfd = {w.fd, POLLIN, 0};
      poll(&pfd, 1, poll_ms);  // spurious wakeups and EINTR are handled by checking again
      if (pfd.revents & POLLIN) {
        drain_wake_socket(w.fd);
      }
    }

    for (size_t i = 0; i < nitems; i++) {
      // A writer may have already taken us off the list
      uint64_t expected = w.uid;
      if (slots[i] >= 0) items[i].q->waiters[slots[i]]->compare_exchange_strong(expected, 0);
    }
  }
}

bool msgq_all_readers_updated(msgq_queue_t *q) {
  uint64_t num_readers = *q->num_readers;
  for (uint64_t i = 0; i < num_readers; i++) {
    if (*q->read_valids[i] && *q->write_pointer != *q->read_pointers[i]) {
      return false;
    }
  }
  return num_readers > 0;
}
