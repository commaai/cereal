#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <string>

#include "msgq/test_runner.h"
#include "msgq/visionipc/visionbuf.h"
#include "msgq/visionipc/visionbuf_backend.h"

TEST_CASE("VisionBuf allocation and fd import") {
  // Include a frame id beyond a page boundary to exercise the full mapping.
  constexpr size_t length = 4096;
  VisionBuf producer;
  producer.allocate(length);
  REQUIRE(producer.len == length);
  REQUIRE(producer.mmap_len >= length + sizeof(uint64_t));

  REQUIRE(producer.begin_cpu_access(false) == 0);
  REQUIRE(producer.get_frame_id() == 0);
  for (size_t i = 0; i < length; ++i) {
    REQUIRE(static_cast<uint8_t *>(producer.addr)[i] == 0);
  }
  REQUIRE(producer.end_cpu_access(false) == 0);

  REQUIRE(producer.begin_cpu_access(true) == 0);
  memset(producer.addr, 0xa5, length);
  producer.set_frame_id(1234);
  REQUIRE(producer.end_cpu_access(true) == 0);
  REQUIRE(producer.sync(VISIONBUF_SYNC_FROM_DEVICE) == 0);

  VisionBuf consumer;
  consumer.fd = dup(producer.fd);
  REQUIRE(consumer.fd >= 0);
  consumer.len = producer.len;
  consumer.mmap_len = producer.mmap_len;
  consumer.import();
  consumer.init_yuv(64, 32, 64, 2048);
  REQUIRE(consumer.y == consumer.addr);
  REQUIRE(consumer.uv == consumer.y + 2048);
  REQUIRE(consumer.sync(VISIONBUF_SYNC_TO_DEVICE) == 0);

  // The imported fd must keep the allocation alive after the producer exits.
  REQUIRE(producer.free() == 0);
  REQUIRE(consumer.begin_cpu_access(false) == 0);
  REQUIRE(consumer.get_frame_id() == 1234);
  for (size_t i = 0; i < length; ++i) {
    REQUIRE(static_cast<uint8_t *>(consumer.addr)[i] == 0xa5);
  }
  REQUIRE(consumer.end_cpu_access(false) == 0);

  REQUIRE(consumer.begin_cpu_read(100, 200) == 0);
  for (size_t i = 100; i < 300; ++i) {
    REQUIRE(static_cast<uint8_t *>(consumer.addr)[i] == 0xa5);
  }
  REQUIRE(consumer.end_cpu_access(false) == 0);
  REQUIRE(consumer.begin_cpu_read(consumer.mmap_len, 0) == 0);
  REQUIRE(consumer.begin_cpu_read(consumer.mmap_len - 8, 9) != 0);
  REQUIRE(consumer.free() == 0);
}

TEST_CASE("VisionBuf backend selection") {
  // AGNOS's upstream kernel, from the start of boot: there's no fallback if its heap can't be opened
  REQUIRE(visionbuf_select_backend(true, "7.3.1") == VisionBufBackendType::DMA_HEAP);
  // AGNOS's downstream kernel
  REQUIRE(visionbuf_select_backend(true, "4.9.103") == VisionBufBackendType::ION);
  // a desktop isn't a comma device, whatever its kernel
  REQUIRE(visionbuf_select_backend(false, "6.8.0-139-generic") == VisionBufBackendType::SHM);
  REQUIRE(visionbuf_select_backend(false, "4.9.103") == VisionBufBackendType::SHM);

  REQUIRE(visionbuf_shm_backend() != nullptr);
#ifdef __linux__
  // every Linux build has both kernels' backends, so one build runs on either
  REQUIRE(visionbuf_ion_backend() != nullptr);
  REQUIRE(visionbuf_dma_heap_backend() != nullptr);
#endif
  const std::string name = visionbuf_backend_name();
  REQUIRE((name == "shm" || name == "ion" || name == "dma_heap"));
}
