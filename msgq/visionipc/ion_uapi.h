#pragma once

// the downstream (4.9 msm) kernel's legacy ION UAPI that visionbuf_ion.cc uses, from linux/ion.h and linux/msm_ion.h.
// vendored so every build has the ION backend, whatever kernel headers the build host has: the upstream kernel's
// images ship none of these headers. goes away with the downstream kernel

#include <linux/ioctl.h>
#include <stddef.h>

typedef int ion_user_handle_t;

struct ion_allocation_data {
  size_t len;
  size_t align;
  unsigned int heap_id_mask;
  unsigned int flags;
  ion_user_handle_t handle;
};

struct ion_fd_data {
  ion_user_handle_t handle;
  int fd;
};

struct ion_handle_data {
  ion_user_handle_t handle;
};

struct ion_custom_data {
  unsigned int cmd;
  unsigned long arg;
};

#define ION_IOC_MAGIC 'I'
#define ION_IOC_ALLOC _IOWR(ION_IOC_MAGIC, 0, struct ion_allocation_data)
#define ION_IOC_FREE _IOWR(ION_IOC_MAGIC, 1, struct ion_handle_data)
#define ION_IOC_SHARE _IOWR(ION_IOC_MAGIC, 4, struct ion_fd_data)
#define ION_IOC_IMPORT _IOWR(ION_IOC_MAGIC, 5, struct ion_fd_data)
#define ION_IOC_CUSTOM _IOWR(ION_IOC_MAGIC, 6, struct ion_custom_data)

#define ION_FLAG_CACHED 1

// msm_ion.h
#define ION_SYSTEM_HEAP_ID 25
#define ION_IOMMU_HEAP_ID ION_SYSTEM_HEAP_ID

struct ion_flush_data {
  ion_user_handle_t handle;
  int fd;
  void *vaddr;
  unsigned int offset;
  unsigned int length;
};

#define ION_IOC_MSM_MAGIC 'M'
#define ION_IOC_CLEAN_CACHES _IOWR(ION_IOC_MSM_MAGIC, 0, struct ion_flush_data)
#define ION_IOC_INV_CACHES _IOWR(ION_IOC_MSM_MAGIC, 1, struct ion_flush_data)
