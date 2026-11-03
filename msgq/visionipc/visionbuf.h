#pragma once

#include "msgq/visionipc/visionipc.h"

#define VISIONBUF_SYNC_FROM_DEVICE 0
#define VISIONBUF_SYNC_TO_DEVICE 1

// Stream ids are opaque to visionipc. Producers/consumers pick an id when
// creating a stream; the sentinel below is reserved to request the list of
// available streams from a server.
typedef uint32_t VisionStreamType;
constexpr VisionStreamType VISION_STREAM_LIST = 0xffffffff;
constexpr size_t VISIONIPC_MAX_STREAMS = 64;

// the buffer backend this process uses: "dma_heap" on the upstream kernel's comma devices, "ion" on the downstream
// kernel's, "shm" everywhere else. it's picked at runtime, and aborts if this build lacks the backend the kernel needs
const char *visionbuf_backend_name();

class VisionBuf {
 public:
  size_t len = 0;
  size_t mmap_len = 0;
  void * addr = nullptr;
  uint64_t *frame_id;
  int fd = 0;

  size_t width = 0;
  size_t height = 0;
  size_t stride = 0;
  size_t uv_offset = 0;

  // YUV
  uint8_t * y = nullptr;
  uint8_t * uv = nullptr;

  // Visionipc
  uint64_t server_id = 0;
  size_t idx = 0;
  VisionStreamType type;

  // ion
  int handle = 0;

  void allocate(size_t len);
  void import();
  void init_yuv(size_t width, size_t height, size_t stride, size_t uv_offset);

  // ION's handoff of a frame: VisionIpcServer::send syncs FROM_DEVICE when the hardware wrote it, VisionIpcClient::recv
  // TO_DEVICE. this is all that ION needs, so CPU reads of an ION frame need nothing more. a no-op on the other
  // backends, it goes away with ION (and with it send's sync argument)
  int sync(int dir);

  // bracket every CPU access to a buffer that hardware also accesses, as DMA-BUF requires. write is true when the CPU
  // only writes it (its old contents aren't read), false when it only reads it. 0 on success
  int begin_cpu_access(bool write);
  int end_cpu_access(bool write);
  // begin_cpu_access(false) for a read of only [start, start + length) of the buffer, which may make it cheaper.
  // the read ends with end_cpu_access(false). 0 on success
  // reads don't sync at the end: there's nothing to write back. DMA-BUF asks for the pair, but the system heap's end
  // only cleans, so the end is skipped for its cost (each sync repeats the whole buffer once per attached device)
  int begin_cpu_read(size_t start, size_t length);
  int free();

  // the frame id lives in the 8 bytes after len, in its own cache line (buffers are page sized). it's only for the
  // process that wrote it (e.g. replay's frame cache), consumers get the frame id from VisionIpcBufExtra
  void set_frame_id(uint64_t id);
  uint64_t get_frame_id();
};
