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
#include <random>
#include <string>
#include <limits>
#include <vector>

#include <poll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdio.h>

#include "msgq/msgq.h"

uint64_t msgq_get_uid(void){
  std::random_device rd("/dev/urandom");
  std::uniform_int_distribution<uint64_t> distribution(1, MSGQ_READER_WAITING - 1);
  return distribution(rd);
}

// Readers blocked in msgq_poll wait on a unix datagram socket named after their uid.
// Writers wake them by sending an empty datagram to it.
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

// Whoever clears a reader's waiting bit has to wake it up, or it sleeps forever.

// Clear the waiting bit in a reader's uid entry, waking the reader if it was set
static void wake_if_waiting(int fd, std::atomic<uint64_t> *read_uid) {
  uint64_t old = read_uid->fetch_and(~MSGQ_READER_WAITING);
  if (old & MSGQ_READER_WAITING) {
    wake_reader(fd, old & ~MSGQ_READER_WAITING);
  }
}

// Replace a reader's uid entry, waking the old reader if it was waiting, so it reconnects
static void replace_reader(int fd, std::atomic<uint64_t> *read_uid, uint64_t new_uid) {
  uint64_t old = read_uid->exchange(new_uid);
  if (old & MSGQ_READER_WAITING) {
    wake_reader(fd, old & ~MSGQ_READER_WAITING);
  }
}

// Kick out all readers
static void evict_readers(msgq_queue_t *q, int fd) {
  *q->num_readers = 0;
  for (size_t i = 0; i < NUM_READERS; i++){
    *q->read_valids[i] = false;
    replace_reader(fd, q->read_uids[i], 0);
  }
}

static void close_wake_socket(msgq_queue_t *q) {
  if (q->wake_fd < 0) return;
  close(q->wake_fd);
  q->wake_fd = -1;
#ifdef __APPLE__
  sockaddr_un addr;
  wake_addr(q->read_uid_local, &addr);
  unlink(addr.sun_path);
#endif
}

static void bind_wake_socket(msgq_queue_t *q) {
  sockaddr_un addr;
  socklen_t len = wake_addr(q->read_uid_local, &addr);

  int fd = wake_socket();
  if (fd < 0) return;
  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), len) < 0) {
    close(fd);
    return;
  }
  q->wake_fd = fd;
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
  close_wake_socket(q);
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
  evict_readers(q, q->send_fd);

  q->write_uid_local = uid;
}

void msgq_init_subscriber(msgq_queue_t * q) {
  assert(q != NULL);
  assert(q->num_readers != NULL);

  close_wake_socket(q);
  uint64_t uid = msgq_get_uid();

  // Get reader id
  while (true){
    uint64_t cur_num_readers = *q->num_readers;
    uint64_t new_num_readers = cur_num_readers + 1;

    // No more slots available. Reset all subscribers to kick out inactive ones
    if (new_num_readers > NUM_READERS){
      //std::cout << "Warning, evicting all subscribers!" << std::endl;
      evict_readers(q, -1);
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
      replace_reader(-1, q->read_uids[cur_num_readers], uid);
      break;
    }
  }

  //std::cout << "New subscriber id: " << q->reader_id << " uid: " << q->read_uid_local << " " << q->endpoint << std::endl;
  msgq_reset_reader(q);
  bind_wake_socket(q);
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

  // Wake up readers blocked in a poll. Reload num_readers so a reader that
  // subscribed and started polling during this send isn't missed.
  num_readers = *q->num_readers;
  for (uint64_t i = 0; i < num_readers; i++){
    wake_if_waiting(q->send_fd, q->read_uids[i]);
  }

  return msg->size;
}


int msgq_msg_ready(msgq_queue_t * q){
 start:
  int id = q->reader_id;
  assert(id >= 0); // Make sure subscriber is initialized

  // Also reconnect if num_readers was reset after we claimed our slot, since
  // publishers only look at slots below num_readers
  if (q->read_uid_local != (*q->read_uids[id] & ~MSGQ_READER_WAITING) || (uint64_t)id >= *q->num_readers){
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
  // publishers only look at slots below num_readers
  if (q->read_uid_local != (*q->read_uids[id] & ~MSGQ_READER_WAITING) || (uint64_t)id >= *q->num_readers){
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
  std::vector<pollfd> fds(nitems);
  std::vector<std::atomic<uint64_t>*> armed(nitems);
  std::vector<uint64_t> uids(nitems);

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

    bool reconnected = false;
    bool can_block = true;
    for (size_t i = 0; i < nitems; i++) {
      msgq_queue_t *q = items[i].q;
      uids[i] = q->read_uid_local;

      // Ask for a wakeup before checking again, so a message sent in between isn't missed.
      // This only succeeds while we still own the slot. If we were evicted, the check reconnects.
      armed[i] = q->read_uids[q->reader_id];
      uint64_t expected = uids[i];
      armed[i]->compare_exchange_strong(expected, uids[i] | MSGQ_READER_WAITING);
      num += msgq_msg_ready(q);

      // The reader may have moved to a new slot that isn't armed yet
      reconnected |= (q->read_uid_local != uids[i]);
      can_block &= (q->wake_fd >= 0);
      fds[i] = {q->wake_fd, POLLIN, 0};
    }

    if (num == 0 && !reconnected) {
      // Without a wake socket, fall back to checking periodically
      int poll_ms = can_block ? timeout_ms : (timeout_ms < 0 ? 10 : std::min(timeout_ms, 10));
      poll(fds.data(), fds.size(), poll_ms);  // spurious wakeups and EINTR are handled by checking again
    }

    for (size_t i = 0; i < nitems; i++) {
      // Only clears our own request; the slot may belong to someone else by now
      uint64_t expected = uids[i] | MSGQ_READER_WAITING;
      armed[i]->compare_exchange_strong(expected, uids[i]);
      if (fds[i].revents & POLLIN) {
        drain_wake_socket(fds[i].fd);
      }
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
