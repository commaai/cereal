#include "msgq/visionipc/visionbuf_backend.h"

#ifdef __linux__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "msgq/visionipc/ion_uapi.h"

namespace {

// keep trying if x gets interrupted by a signal
#define HANDLE_EINTR(x)                                       \
  ({                                                          \
    decltype(x) ret;                                          \
    int try_cnt = 0;                                          \
    do {                                                      \
      ret = (x);                                              \
    } while (ret == -1 && errno == EINTR && try_cnt++ < 100); \
    ret;                                                      \
  })

struct IonFileHandle {
  IonFileHandle() {
    fd = open("/dev/ion", O_RDWR | O_NONBLOCK);
    assert(fd >= 0);
  }
  ~IonFileHandle() {
    close(fd);
  }
  int fd = -1;
};

int ion_fd() {
  static IonFileHandle fh;
  return fh.fd;
}

void allocate(VisionBuf *buf, size_t length) {
  struct ion_allocation_data ion_alloc = {0};
  ion_alloc.len = length + sizeof(uint64_t);
  ion_alloc.align = 4096;
  ion_alloc.heap_id_mask = 1 << ION_IOMMU_HEAP_ID;
  ion_alloc.flags = ION_FLAG_CACHED;

  int err = HANDLE_EINTR(ioctl(ion_fd(), ION_IOC_ALLOC, &ion_alloc));
  assert(err == 0);

  struct ion_fd_data ion_fd_data = {0};
  ion_fd_data.handle = ion_alloc.handle;
  err = HANDLE_EINTR(ioctl(ion_fd(), ION_IOC_SHARE, &ion_fd_data));
  assert(err == 0);

  void *mmap_addr = mmap(NULL, ion_alloc.len,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED, ion_fd_data.fd, 0);
  assert(mmap_addr != MAP_FAILED);

  memset(mmap_addr, 0, ion_alloc.len);

  buf->len = length;
  buf->mmap_len = ion_alloc.len;
  buf->addr = mmap_addr;
  buf->handle = ion_alloc.handle;
  buf->fd = ion_fd_data.fd;
  buf->frame_id = (uint64_t*)((uint8_t*)buf->addr + buf->len);
}

void import(VisionBuf *buf) {
  int err;
  assert(buf->fd >= 0);

  // Get handle
  struct ion_fd_data fd_data = {0};
  fd_data.fd = buf->fd;
  err = HANDLE_EINTR(ioctl(ion_fd(), ION_IOC_IMPORT, &fd_data));
  assert(err == 0);

  buf->handle = fd_data.handle;
  buf->addr = mmap(NULL, buf->mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, buf->fd, 0);
  assert(buf->addr != MAP_FAILED);

  buf->frame_id = (uint64_t*)((uint8_t*)buf->addr + buf->len);
}

int sync(VisionBuf *buf, int dir) {
  struct ion_flush_data flush_data = {0};
  flush_data.handle = buf->handle;
  flush_data.vaddr = buf->addr;
  flush_data.offset = 0;
  flush_data.length = buf->len;

  // ION_IOC_INV_CACHES ~= DMA_FROM_DEVICE
  // ION_IOC_CLEAN_CACHES ~= DMA_TO_DEVICE
  // ION_IOC_CLEAN_INV_CACHES ~= DMA_BIDIRECTIONAL

  struct ion_custom_data custom_data = {0};

   assert(dir == VISIONBUF_SYNC_FROM_DEVICE || dir == VISIONBUF_SYNC_TO_DEVICE);
   custom_data.cmd = (dir == VISIONBUF_SYNC_FROM_DEVICE) ?
     ION_IOC_INV_CACHES : ION_IOC_CLEAN_CACHES;

  custom_data.arg = (unsigned long)&flush_data;
  return HANDLE_EINTR(ioctl(ion_fd(), ION_IOC_CUSTOM, &custom_data));
}

int free(VisionBuf *buf) {
  int err = munmap(buf->addr, buf->mmap_len);
  if (err != 0) return err;

  err = close(buf->fd);
  if (err != 0) return err;

  struct ion_handle_data handle_data = {.handle = buf->handle};
  return HANDLE_EINTR(ioctl(ion_fd(), ION_IOC_FREE, &handle_data));
}

// the producer invalidates the CPU caches when it sends a frame the hardware wrote (VisionIpcServer::send with sync),
// so reads need nothing more. writes are cleaned when they're done
int begin_cpu_access(VisionBuf *buf, bool write) {
  return 0;
}

int end_cpu_access(VisionBuf *buf, bool write) {
  return write ? sync(buf, VISIONBUF_SYNC_TO_DEVICE) : 0;
}

int begin_cpu_read(VisionBuf *buf, size_t start, size_t length) {
  return begin_cpu_access(buf, false);
}

const VisionBufBackend backend = {
  .name = "ion",
  .allocate = allocate,
  .import = import,
  .sync = sync,
  .begin_cpu_access = begin_cpu_access,
  .end_cpu_access = end_cpu_access,
  .begin_cpu_read = begin_cpu_read,
  .free = free,
};

}  // namespace

const VisionBufBackend *visionbuf_ion_backend() { return &backend; }

#else

const VisionBufBackend *visionbuf_ion_backend() { return nullptr; }

#endif
