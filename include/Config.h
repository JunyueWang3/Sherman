#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig {
public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = 1) : cacheSize(cacheSize) {}
};

class DSMConfig {
public:
  CacheConfig cacheConfig;
  uint32_t machineNR;
  uint64_t dsmSize; // G
  char pm_path[MAX_ARG_SIZE]{"/mnt/pmem0/pmem_sherman"};

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t machineNR = 2, uint64_t dsmSize = 8)
      : cacheConfig(cacheConfig), machineNR(machineNR), dsmSize(dsmSize) {}
};

#endif /* __CONFIG_H__ */
