#include "DSM.h"
#include "Tree.h"

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];

extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

int main() {

  DSMConfig config;
  config.machineNR = 1;
  DSM *dsm = DSM::getInstance(config);

  
  dsm->registerThread();

  auto tree = new Tree(dsm);

  Value v;

  if (dsm->getMyNodeID() != 0) {
    while (true)
      ;
  }

  uint64_t key_nums = 10240000;

  timespec s, e;

  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);

  for (uint64_t i = 1; i <= key_nums; ++i) {
    tree->insert(i, i * 2);
  }

  // for (uint64_t i = 1; i <= key_nums; ++i) {
  //   auto res = tree->search(i, v);
  //   if (v != i * 2) {
  //     assert(res && v == i * 2);
  //     std::cout << "search result:  k = " << i << ", v = " << res << " v: " << v
  //               << std::endl;
  //   }
  // }

  clock_gettime(CLOCK_REALTIME, &e);
  int microseconds =
      (e.tv_sec - s.tv_sec) * 1000000 + (double)(e.tv_nsec - s.tv_nsec) / 1000;

  uint64_t all = 0;
  uint64_t hit = 0;
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    all += (cache_hit[i][0] + cache_miss[i][0]);
    hit += cache_hit[i][0];
  }

  if (++count % 3 == 0 && dsm->getMyNodeID() == 0) {
    cal_latency();
  }

  double per_node_tp = key_nums * 1.0 / microseconds;

  printf("%d, time %d us , throughput %.4f Mops\n", dsm->getMyNodeID(), microseconds, per_node_tp);
  // for (uint64_t i = 1; i < 128; ++i) {
  //   auto res = tree->search(i, v);
  //   std::cout << "search result:  " << res << " v: " << v << std::endl;
  //   assert(res && v == i * 2);
  // }

  // for (uint64_t i = 10240 - 1; i >= 1; --i) {
  //   tree->insert(i, i * 3);
  // }

  // for (uint64_t i = 1; i < 10240; ++i) {
  //   auto res = tree->search(i, v);
  //   assert(res && v == i * 3);
  //   std::cout << "search result:  " << res << " v: " << v << std::endl;
  // }

  // for (uint64_t i = 1; i < 10240; ++i) {
  //   tree->del(i);
  // }

  // for (uint64_t i = 1; i < 10240; ++i) {
  //   auto res = tree->search(i, v);
  //   std::cout << "search result:  " << res << std::endl;
  // }

  // for (uint64_t i = 10240 - 1; i >= 1; --i) {
  //   tree->insert(i, i * 3);
  // }

  // for (uint64_t i = 1; i < 10240; ++i) {
  //   auto res = tree->search(i, v);
  //   assert(res && v == i * 3);
  //   std::cout << "search result:  " << res << " v: " << v << std::endl;
  // }

  printf("Hello\n");

  while (true)
    ;
}