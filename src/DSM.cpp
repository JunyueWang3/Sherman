
#include "DSM.h"
#include "Common.h"
#include "Directory.h"
#include "GlobalAddress.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"
#include "Rdma.h"
#include "Tree.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <libpmem.h>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;
thread_local LocalAllocator DSM::local_allocator;
thread_local RdmaBuffer DSM::rbuf[define::kMaxCoro];
thread_local uint64_t DSM::thread_tag = 0;

DSM *DSM::getInstance(const DSMConfig &conf) {
  static DSM *dsm = nullptr;
  static WRLock lock;

  lock.wLock();
  if (!dsm) {
    dsm = new DSM(conf);
  } else {
  }
  lock.wUnlock();

  return dsm;
}

DSM::DSM(const DSMConfig &conf)
    : conf(conf), appID(0), cache(conf.cacheConfig) {

  // pmem map
  size_t length;
  int is_pmem = 0;

  // For Device DAX mappings, the len argument must be, regardless of the flags,
  // equal to either 0 or the exact size of the device.
  baseAddr = (uint64_t)pmem_map_file(conf.pm_path, conf.dsmSize * define::GB,
                                     PMEM_FILE_CREATE, 0666, &length, &is_pmem);
  Debug::notifyInfo("src_buffer: %p, pmem_is_pmem: %d, %d,length = %lu\n",
                    baseAddr, pmem_is_pmem((void *)baseAddr, length),
                    pmem_is_pmem((void *)baseAddr, conf.dsmSize), length);
  if (!baseAddr) {
    Debug::notifyInfo("pmem_map_file failed for %s\n", strerror(errno));
    Debug::notifyInfo("Map PM file failed");
  } else {
    Debug::notifyInfo("Mapping PM device success size %lu\n", length);
    // conf.dsmSize = length;
  }

  // baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);
  baseLockAddr = (uint64_t)hugePageAlloc(define::kLockChipMemSize);

  Debug::notifyInfo("shared memory size: %dGB, 0x%lx", conf.dsmSize, baseAddr);
  Debug::notifyInfo("cache size: %dGB", conf.cacheConfig.cacheSize);

  // warmup
  // memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
  for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
       i += 2 * define::MB) {
    *(char *)i = 0;
  }

  // clear up first chunk
  pmem_memset((char *)baseAddr, 0, define::kChunkSize, PMEM_F_MEM_NODRAIN);

  initRDMAConnection();

  Debug::notifyInfo("number of threads on memory node: %d", NR_DIRECTORY);
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirAgent[i] =
        new Directory(dirCon[i], remoteInfo, conf.machineNR, i, myNodeID);
  }

  keeper->barrier("DSM-init");
}

DSM::~DSM() {}

void DSM::registerThread() {

  static bool has_init[MAX_APP_THREAD];

  if (thread_id != -1)
    return;

  thread_id = appID.fetch_add(1);
  thread_tag = thread_id + (((uint64_t)this->getMyNodeID()) << 32) + 1;

  iCon = thCon[thread_id];

  if (!has_init[thread_id]) {
    iCon->message->initRecv();
    iCon->message->initSend();

    has_init[thread_id] = true;
  }

  rdma_buffer = (char *)cache.data + thread_id * 12 * define::MB;

  for (int i = 0; i < define::kMaxCoro; ++i) {
    rbuf[i].set_buffer(rdma_buffer + i * define::kPerCoroRdmaBuf);
  }
}

void DSM::initRDMAConnection() {

  Debug::notifyInfo("number of servers (colocated MN/CN): %d", conf.machineNR);

  remoteInfo = new RemoteConnection[conf.machineNR];

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    thCon[i] =
        new ThreadConnection(i, (void *)cache.data, cache.size * define::GB,
                             conf.machineNR, remoteInfo);
  }

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirCon[i] = new DirectoryConnection(
        i, (void *)baseAddr, (void *)baseLockAddr, conf.dsmSize * define::GB,
        conf.machineNR, remoteInfo);
  }

  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.machineNR);

  myNodeID = keeper->getMyNodeID();
}

void DSM::read(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
               CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_sync(char *buffer, GlobalAddress gaddr, size_t size,
                    CoroContext *ctx) {
  read(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroContext *ctx) {
  write(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_atomic(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaWriteAtomic(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, signal);
  } else {
    rdmaWriteAtomic(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_atomic_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroContext *ctx) {
  write_atomic(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}


void DSM::fill_keys_dest(RdmaOpRegion &ror, GlobalAddress gaddr, bool is_chip) {
  ror.lkey = iCon->cacheLKey;
  if (is_chip) {
    ror.dest = remoteInfo[gaddr.nodeID].lockBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].lockRKey[0];
  } else {
    ror.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
  }
}

void DSM::fill_rdma_op_context(RdmaOpContext &roc, GlobalAddress gaddr) {
  roc.lkey = iCon->cacheLKey;

  roc.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
  roc.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
}

void DSM::write_multi(RdmaOpContext *rs, int k, bool signal, CoroContext *ctx) {
  int node_id = -1;
  for(int i = 0;i<k;i++){
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_rdma_op_context(rs[i], gaddr);
  }
  if (ctx == nullptr) {
    rdmaWriteMulti(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteMulti(iCon->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_multi_sync(RdmaOpContext *rs, int k, CoroContext *ctx) {
  write_multi(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

// 仅用于分裂的写回sibling和原节点的非leafmeta部分
void DSM::prepare_and_write_multi_sync(GlobalAddress sibling_gaddr,
                                       char *sibling_buffer,
                                       GlobalAddress original_gaddr,
                                       char *original_buffer,
                                       CoroContext *ctx) {
  RdmaOpContext rocs[3];                                      
  rocs[0] = {
      .source = (uint64_t)sibling_buffer,
      .dest = remoteInfo[sibling_gaddr.nodeID].dsmBase + sibling_gaddr.offset,
      .size = 8,
      .lkey = iCon->cacheLKey,
      .remoteRKey = remoteInfo[sibling_gaddr.nodeID].dsmRKey[0],
  };

  // 写原叶子节点leafheader
  rocs[1]  = {
      .source = (uint64_t)original_buffer,
      .dest = remoteInfo[original_gaddr.nodeID].dsmBase + original_gaddr.offset,
      .size = sizeof(LeafHeader),
      .lkey = iCon->cacheLKey,
      .remoteRKey = remoteInfo[original_gaddr.nodeID].dsmRKey[0],
  };

  // 写原叶子节点leafmeta后的内容
  rocs[2] = {
      .source = (uint64_t)(original_buffer + (uint64_t)(STRUCT_OFFSET(LeafPage, shadowPtr))),
      .dest = remoteInfo[original_gaddr.nodeID].dsmBase + original_gaddr.offset + (uint64_t)(STRUCT_OFFSET(LeafPage, shadowPtr)),
      .size = sizeof(LeafPage) - sizeof(LeafHeader) - sizeof(LeafMeta),
      .lkey = iCon->cacheLKey,
      .remoteRKey = remoteInfo[original_gaddr.nodeID].dsmRKey[0],
  };

  write_multi_sync(rocs, 3);
}

void DSM::write_batch(RdmaOpRegion *rs, int k, bool signal, CoroContext *ctx) {

  int node_id = -1;
  for (int i = 0; i < k; ++i) {

    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
  write_batch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_faa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                    uint64_t add_val, bool signal, CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;

    fill_keys_dest(faa_ror, gaddr, faa_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, signal);
  } else {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_faa_sync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                         uint64_t add_val, CoroContext *ctx) {
  write_faa(write_ror, faa_ror, add_val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_cas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                    uint64_t equal, uint64_t val, bool signal,
                    CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val,
                 signal);
  } else {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_cas_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, CoroContext *ctx) {
  write_cas(write_ror, cas_ror, equal, val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_read(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                   uint64_t equal, uint64_t val, bool signal,
                   CoroContext *ctx) {

  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    fill_keys_dest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, signal);
  } else {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, true,
                ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_read_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                        uint64_t equal, uint64_t val, CoroContext *ctx) {
  cas_read(cas_ror, read_ror, equal, val, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSM::cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx) {
  cas(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

uint64_t swap_endian(uint64_t value) {
  return ((value & 0xFF00000000000000ull) >> 56) |
         ((value & 0x00FF000000000000ull) >> 40) |
         ((value & 0x0000FF0000000000ull) >> 24) |
         ((value & 0x000000FF00000000ull) >> 8) |
         ((value & 0x00000000FF000000ull) << 8) |
         ((value & 0x0000000000FF0000ull) << 24) |
         ((value & 0x000000000000FF00ull) << 40) |
         ((value & 0x00000000000000FFull) << 56);
}

void DSM::faa(GlobalAddress gaddr, uint64_t add, uint64_t *rdma_buffer,
              bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaFetchAndAdd(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                    remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, 1,
                    iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0]);
  } else {
    // todo
    assert(false);
  }
}

uint64_t DSM::faa_sync(GlobalAddress gaddr, uint64_t add, uint64_t *rdma_buffer,
                       CoroContext *ctx) {
  faa(gaddr, add, rdma_buffer, true, ctx);

  uint64_t old_value = 0;
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
    if (wc.opcode == IBV_WC_FETCH_ADD) {
      // 获取远程地址的旧值
      old_value = swap_endian(*rdma_buffer); // 获取旧值（大小端转换）
      std::cout << "FAA operation completed. Old value: " << old_value
                << std::endl;
    } else {
      std::cerr << "Unexpected completion opcode." << std::endl;
    }
  }

  return old_value;
}

void DSM::faa_read(RdmaOpContext &faa_roc, RdmaOpContext &read_roc,
                   uint64_t add, bool signal, CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = faa_roc.dest;
    node_id = gaddr.nodeID;
    fill_rdma_op_context(faa_roc, gaddr);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_roc.dest;
    fill_rdma_op_context(read_roc, gaddr);
  }
  if (ctx == nullptr) {
    rdmaFaaRead(iCon->data[0][node_id], faa_roc, read_roc, 1, signal);
    // rdmaFetchAndAdd(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
    //                 remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, 1,
    //                 iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0]);
  } else {
    // todo
    rdmaFaaRead(iCon->data[0][node_id], faa_roc, read_roc, 1, signal,
                ctx->coro_id);
    assert(false);
  }
}

uint64_t DSM::faa_read_sync(RdmaOpContext &faa_roc, RdmaOpContext &read_roc,
                            uint64_t add, CoroContext *ctx) {
  faa_read(faa_roc, read_roc, add, true, ctx);

  uint64_t old_value = 0;
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
    if (wc.opcode == IBV_WC_RDMA_READ) {
      // 获取远程地址的旧值
      old_value = swap_endian(faa_roc.source); // 获取旧值（大小端转换）
      std::cout << "FAA Read operation completed. Old value: " << old_value
                << std::endl;
    } else {
      std::cerr << "Unexpected completion opcode." << std::endl;
    }
  }

  return old_value;
}

// 只用于插入叶子节点的第一次rtt，对leaf meta做faa并读取leaf header
uint64_t DSM::prepare_and_faa_read_sync(GlobalAddress faa_gaddr, uint64_t add,
                                        uint64_t *faa_buffer,
                                        GlobalAddress read_gaddr,
                                        char *read_buffer, CoroContext *ctx) {
  RdmaOpContext faa_roc = {
      .source = (uint64_t)faa_buffer,
      .dest = remoteInfo[faa_gaddr.nodeID].dsmBase + faa_gaddr.offset,
      .size = 8,
      .lkey = iCon->cacheLKey,
      .remoteRKey = remoteInfo[faa_gaddr.nodeID].dsmRKey[0],
  };

  RdmaOpContext read_roc = {
      .source = (uint64_t)read_buffer,
      .dest = remoteInfo[read_gaddr.nodeID].dsmBase + read_gaddr.offset,
      .size = sizeof(LeafHeader),
      .lkey = iCon->cacheLKey,
      .remoteRKey = remoteInfo[read_gaddr.nodeID].dsmRKey[0],
  };

  return faa_read_sync(faa_roc, read_roc, add);
}

// void DSM::cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                    uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//   rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID],
//   (uint64_t)rdma_buffer,
//                          remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                          equal, val, iCon->cacheLKey,
//                          remoteInfo[gaddr.nodeID].dsmRKey[0], mask,
//                          signal);
// }

// bool DSM::cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t
// val,
//                         uint64_t *rdma_buffer, uint64_t mask) {
//   cas_mask(gaddr, equal, val, rdma_buffer, mask);
//   ibv_wc wc;
//   pollWithCQ(iCon->cq, 1, &wc);

//   return (equal & mask) == (*rdma_buffer & mask);
// }

// void DSM::faa_boundary(GlobalAddress gaddr, uint64_t add_val,
//                        uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                        CoroContext *ctx) {
//   if (ctx == nullptr) {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].dsmBase +
//                             gaddr.offset, add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].dsmRKey[0], mask,
//                             signal);
//   } else {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].dsmBase +
//                             gaddr.offset, add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].dsmRKey[0], mask,
//                             true, ctx->coro_id);
//     (*ctx->yield)(*ctx->master);
//   }
// }

// void DSM::faa_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                             uint64_t *rdma_buffer, uint64_t mask,
//                             CoroContext *ctx) {
//   faa_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//   if (ctx == nullptr) {
//     ibv_wc wc;
//     pollWithCQ(iCon->cq, 1, &wc);
//   }
// }

void DSM::read_dm(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
                  CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_dm_sync(char *buffer, GlobalAddress gaddr, size_t size,
                       CoroContext *ctx) {
  read_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_dm(const char *buffer, GlobalAddress gaddr, size_t size,
                   bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1,
              signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_dm_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                        CoroContext *ctx) {
  write_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_dm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], true,
                       ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_dm_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, CoroContext *ctx) {
  cas_dm(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

// void DSM::cas_dm_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                       uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//   rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                          remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                          equal, val, iCon->cacheLKey,
//                          remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
// }

// bool DSM::cas_dm_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                            uint64_t *rdma_buffer, uint64_t mask) {
//   cas_dm_mask(gaddr, equal, val, rdma_buffer, mask);
//   ibv_wc wc;
//   pollWithCQ(iCon->cq, 1, &wc);

//   return (equal & mask) == (*rdma_buffer & mask);
// }

// void DSM::faa_dm_boundary(GlobalAddress gaddr, uint64_t add_val,
//                           uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                           CoroContext *ctx) {
//   if (ctx == nullptr) {

//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].lockRKey[0], mask,
//                             signal);
//   } else {
//     rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID],
//     (uint64_t)rdma_buffer,
//                             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                             add_val, iCon->cacheLKey,
//                             remoteInfo[gaddr.nodeID].lockRKey[0], mask, true,
//                             ctx->coro_id);
//     (*ctx->yield)(*ctx->master);
//   }
// }

// void DSM::faa_dm_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                                uint64_t *rdma_buffer, uint64_t mask,
//                                CoroContext *ctx) {
//   faa_dm_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//   if (ctx == nullptr) {
//     ibv_wc wc;
//     pollWithCQ(iCon->cq, 1, &wc);
//   }
// }

uint64_t DSM::poll_rdma_cq(int count) {
  ibv_wc wc;
  pollWithCQ(iCon->cq, count, &wc);

  return wc.wr_id;
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id) {
  ibv_wc wc;
  int res = pollOnce(iCon->cq, 1, &wc);

  wr_id = wc.wr_id;

  return res == 1;
}