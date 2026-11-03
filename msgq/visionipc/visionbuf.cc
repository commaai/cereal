#include "msgq/visionipc/visionbuf.h"
#include "msgq/visionipc/visionbuf_backend.h"

#include <atomic>
#include <stdio.h>
#include <fcntl.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/utsname.h>

// *** shared memory backend ***

namespace {

std::atomic<int> offset = 0;

void *malloc_with_fd(size_t len, int *fd) {
  char full_path[0x100];

#ifdef __APPLE__
  snprintf(full_path, sizeof(full_path)-1, "/tmp/visionbuf_%d_%d", getpid(), offset++);
#else
  snprintf(full_path, sizeof(full_path)-1, "/dev/shm/msgq_visionbuf_%d_%d", getpid(), offset++);
#endif

  *fd = open(full_path, O_RDWR | O_CREAT, 0664);
  assert(*fd >= 0);

  unlink(full_path);

  int ret = ftruncate(*fd, len);
  assert(ret == 0);
  void *addr = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, *fd, 0);
  assert(addr != MAP_FAILED);

  return addr;
}

void shm_allocate(VisionBuf *buf, size_t length) {
  buf->len = length;
  buf->mmap_len = buf->len + sizeof(uint64_t);
  buf->addr = malloc_with_fd(buf->mmap_len, &buf->fd);
  buf->frame_id = (uint64_t*)((uint8_t*)buf->addr + buf->len);
}

void shm_import(VisionBuf *buf) {
  assert(buf->fd >= 0);
  buf->addr = mmap(NULL, buf->mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, buf->fd, 0);
  assert(buf->addr != MAP_FAILED);
  buf->frame_id = (uint64_t*)((uint8_t*)buf->addr + buf->len);
}

int shm_sync(VisionBuf *buf, int dir) { return 0; }
int shm_cpu_access(VisionBuf *buf, bool write) { return 0; }
int shm_cpu_read(VisionBuf *buf, size_t start, size_t length) { return 0; }

int shm_free(VisionBuf *buf) {
  int err = munmap(buf->addr, buf->mmap_len);
  if (err != 0) return err;
  return close(buf->fd);
}

const VisionBufBackend shm_backend = {
  .name = "shm",
  .allocate = shm_allocate,
  .import = shm_import,
  .sync = shm_sync,
  .begin_cpu_access = shm_cpu_access,
  .end_cpu_access = shm_cpu_access,
  .begin_cpu_read = shm_cpu_read,
  .free = shm_free,
};

}  // namespace

const VisionBufBackend *visionbuf_shm_backend() { return &shm_backend; }

// *** backend selection ***

VisionBufBackendType visionbuf_select_backend(bool agnos, const char *kernel_release) {
  if (!agnos) return VisionBufBackendType::SHM;
  // AGNOS's downstream kernel is 4.9
  return atoi(kernel_release) > 4 ? VisionBufBackendType::DMA_HEAP : VisionBufBackendType::ION;
}

namespace {

const VisionBufBackend &backend() {
  static const VisionBufBackend *b = []() {
    struct utsname u = {};
    const bool agnos = access("/AGNOS", F_OK) == 0;
    const VisionBufBackend *ret = nullptr;
    const char *need = nullptr;
    switch (visionbuf_select_backend(agnos, uname(&u) == 0 ? u.release : "")) {
      case VisionBufBackendType::DMA_HEAP: ret = visionbuf_dma_heap_backend(); need = "DMA heap"; break;
      case VisionBufBackendType::ION: ret = visionbuf_ion_backend(); need = "ION"; break;
      case VisionBufBackendType::SHM: ret = visionbuf_shm_backend(); need = "shared memory"; break;
    }
    if (ret == nullptr) {
      fprintf(stderr, "visionipc: this kernel needs %s buffers, but visionipc was built without them\n", need);
      abort();
    }
    return ret;
  }();
  return *b;
}

}  // namespace

const char *visionbuf_backend_name() { return backend().name; }

// *** VisionBuf ***

void VisionBuf::allocate(size_t length) { backend().allocate(this, length); }
void VisionBuf::import() { backend().import(this); }
int VisionBuf::free() { return backend().free(this); }

int VisionBuf::sync(int dir) {
  assert(dir == VISIONBUF_SYNC_FROM_DEVICE || dir == VISIONBUF_SYNC_TO_DEVICE);
  return backend().sync(this, dir);
}

int VisionBuf::begin_cpu_access(bool write) { return backend().begin_cpu_access(this, write); }
int VisionBuf::end_cpu_access(bool write) { return backend().end_cpu_access(this, write); }
int VisionBuf::begin_cpu_read(size_t start, size_t length) {
  if (start > mmap_len || length > mmap_len - start) return -1;
  return backend().begin_cpu_read(this, start, length);
}

void VisionBuf::init_yuv(size_t init_width, size_t init_height, size_t init_stride, size_t init_uv_offset){
  this->width = init_width;
  this->height = init_height;
  this->stride = init_stride;
  this->uv_offset = init_uv_offset;

  this->y = (uint8_t *)this->addr;
  this->uv = this->y + this->uv_offset;
}

uint64_t VisionBuf::get_frame_id() {
  return *frame_id;
}

void VisionBuf::set_frame_id(uint64_t id) {
  *frame_id = id;
}
