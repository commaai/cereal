#include "msgq/visionipc/visionbuf_backend.h"

#ifdef __linux__

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>

#include "msgq/visionipc/dma_heap_uapi.h"

namespace {

int dma_ioctl(int fd, unsigned long request, void *data) {
  int ret;
  do {
    ret = ioctl(fd, request, data);
  } while (ret == -1 && (errno == EINTR || errno == EAGAIN));
  return ret;
}

int heap_fd() {
  static int heap = []() {
    int fd = open("/dev/dma_heap/system", O_RDWR | O_CLOEXEC);
    if (fd < 0) {
      fprintf(stderr, "visionipc: can't open /dev/dma_heap/system: %s\n", strerror(errno));
      abort();
    }
    return fd;
  }();
  return heap;
}

int dma_buf_sync(VisionBuf *buf, uint64_t flags) {
  struct dma_buf_sync sync_data = {.flags = flags};
  return dma_ioctl(buf->fd, DMA_BUF_IOCTL_SYNC, &sync_data);
}

// each sync is cache maintenance over the whole buffer, repeated for every device the buffer is attached to (a camera
// frame is attached to the ISP, Venus and the GPU). so only the direction the CPU access needs is synced:
// - a read invalidates at the start, so it sees what the hardware wrote. its end has nothing to write back, so it's skipped
// - a write needs no invalidate at the start (WRITE is DMA_TO_DEVICE, a no-op for the CPU), and is cleaned at the end
int begin_cpu_access(VisionBuf *buf, bool write) {
  return dma_buf_sync(buf, DMA_BUF_SYNC_START | (write ? DMA_BUF_SYNC_WRITE : DMA_BUF_SYNC_READ));
}

int end_cpu_access(VisionBuf *buf, bool write) {
  return write ? dma_buf_sync(buf, DMA_BUF_SYNC_END | DMA_BUF_SYNC_WRITE) : 0;
}

// DMA_BUF_IOCTL_SYNC has no range, so a read of part of a buffer invalidates all of it, once per device. a heap buffer
// is ordinary cacheable memory, and arm64 lets userspace invalidate cache lines to the point of coherency itself (Linux
// sets SCTLR_EL1.UCI). so on arm64, only the lines read are invalidated, once. DC CIVAC also cleans, which is right for
// any line the CPU dirtied (the frame id after len, which the hardware never writes) and does nothing to the clean
// ones: CPU writes to the frame are cleaned by end_cpu_access(true)
int begin_cpu_read(VisionBuf *buf, size_t start, size_t length) {
#ifdef __aarch64__
  uint64_t ctr;
  asm volatile("mrs %0, ctr_el0" : "=r"(ctr));
  const uintptr_t line = 4 << ((ctr >> 16) & 0xf);  // DminLine: log2 of the smallest data cache line in 4-byte words
  const uintptr_t end = (uintptr_t)buf->addr + start + length;
  for (uintptr_t p = ((uintptr_t)buf->addr + start) & ~(line - 1); p < end; p += line) {
    asm volatile("dc civac, %0" : : "r"(p) : "memory");
  }
  asm volatile("dsb sy" : : : "memory");
  return 0;
#else
  return begin_cpu_access(buf, false);
#endif
}

// the handoff between producer and consumers needs nothing, CPU accesses are bracketed with begin/end_cpu_access
int sync(VisionBuf *buf, int dir) {
  return 0;
}

void import(VisionBuf *buf) {
  assert(buf->fd >= 0);
  buf->addr = mmap(NULL, buf->mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, buf->fd, 0);
  assert(buf->addr != MAP_FAILED);
  buf->frame_id = (uint64_t*)((uint8_t*)buf->addr + buf->len);
}

void allocate(VisionBuf *buf, size_t length) {
  struct dma_heap_allocation_data alloc = {};
  alloc.len = length + sizeof(uint64_t);
  alloc.fd_flags = O_RDWR | O_CLOEXEC;

  int err = dma_ioctl(heap_fd(), DMA_HEAP_IOCTL_ALLOC, &alloc);
  assert(err == 0);

  buf->len = length;
  buf->mmap_len = alloc.len;
  buf->fd = alloc.fd;
  import(buf);

  err = begin_cpu_access(buf, true);
  assert(err == 0);
  memset(buf->addr, 0, buf->mmap_len);
  err = end_cpu_access(buf, true);
  assert(err == 0);
}

int free(VisionBuf *buf) {
  int err = munmap(buf->addr, buf->mmap_len);
  if (err != 0) return err;
  return close(buf->fd);
}

const VisionBufBackend backend = {
  .name = "dma_heap",
  .allocate = allocate,
  .import = import,
  .sync = sync,
  .begin_cpu_access = begin_cpu_access,
  .end_cpu_access = end_cpu_access,
  .begin_cpu_read = begin_cpu_read,
  .free = free,
};

}  // namespace

const VisionBufBackend *visionbuf_dma_heap_backend() { return &backend; }

#else

const VisionBufBackend *visionbuf_dma_heap_backend() { return nullptr; }

#endif
