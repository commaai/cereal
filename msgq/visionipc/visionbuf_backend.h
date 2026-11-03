#pragma once

#include "msgq/visionipc/visionbuf.h"

// a VisionBuf allocator. the backend is picked once per process, see visionbuf_backend()
struct VisionBufBackend {
  const char *name;
  void (*allocate)(VisionBuf *buf, size_t length);
  void (*import)(VisionBuf *buf);
  int (*sync)(VisionBuf *buf, int dir);
  int (*begin_cpu_access)(VisionBuf *buf, bool write);
  int (*end_cpu_access)(VisionBuf *buf, bool write);
  int (*begin_cpu_read)(VisionBuf *buf, size_t start, size_t length);
  int (*free)(VisionBuf *buf);
};

// nullptr when the backend isn't built: ION and DMA heaps are only built for Linux
const VisionBufBackend *visionbuf_shm_backend();
const VisionBufBackend *visionbuf_ion_backend();
const VisionBufBackend *visionbuf_dma_heap_backend();

enum class VisionBufBackendType { SHM, ION, DMA_HEAP };

// the backend for a system, from whether it's AGNOS and its kernel's release (uname -r). AGNOS's upstream kernel has DMA
// heaps, which the ISP, Venus and the GPU import buffers from, and its downstream 4.9 kernel has ION. it's decided by the
// running kernel, which is the same for every process from the start of boot, unlike device nodes, which appear as
// their drivers probe. everywhere else, shared memory: a desktop's heap is usually root-only, and nothing imports them
VisionBufBackendType visionbuf_select_backend(bool agnos, const char *kernel_release);
