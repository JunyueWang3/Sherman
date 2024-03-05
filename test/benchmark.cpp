#include "Common.h"
#include "Timer.h"
#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <cstdlib>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

//////////////////// workload parameters /////////////////////

// #define USE_CORO
const int kCoroCnt = 3;

int kReadRatio;
int kThreadCount;
int kNodeCount;
int kOperType = 0;

uint64_t kKeySpace = 125000000;
uint64_t operNum = 10000000;
uint64_t operPerTh = 0;
double kWarmRatio = 0.8;
double zipfan = 0;
uint64_t scan_size = 100;

//////////////////// workload parameters /////////////////////

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];

extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern uint64_t time_per_th[MAX_APP_THREAD];

uint64_t latency_th_all[LATENCY_WINDOWS];

Tree *tree;
DSM *dsm;

inline Key to_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSM *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSM *dsm, int id) {
  return new RequsetGenBench(coro_id, dsm, id);
}

Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic<int64_t> finish_oper_cnt{0};

std::atomic_bool ready{false};
void thread_run(int id) {

  bindCore(id);

  dsm->registerThread();

  uint64_t all_thread = kThreadCount * dsm->getClusterSize();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;

  printf("I am thread %ld on compute nodes\n", my_id);

  if (id == 0) {
    bench_timer.begin();
    tree->clear_rtt_time();
  }

  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < end_warm_key; ++i) {
    if (i % all_thread == my_id) {
      tree->insert(to_key(i), i * 2);
    }
  }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", dsm->getMyNodeID());
    dsm->barrier("warm_finish");

    uint64_t ns = bench_timer.end();
    printf("warmup time %ldus\n", ns / 1000);

    tree->print_rtt_time();
    tree->clear_rtt_time();

    tree->index_cache_statistics();
    tree->clear_statistics();

    ready = true;

    warmup_cnt.store(0);
  }

  while (warmup_cnt.load() != 0)
    ;

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, kCoroCnt);
#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);

  Timer total_timer;
  Timer timer;
  while (true) {

    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);

    Value v;
    Value *buffer = new Value[110];
    timer.begin();
    total_timer.begin();

    switch (kOperType) {
    case 0:
      // put and get
      if (rand_r(&seed) % 100 < kReadRatio) { // GET
        tree->search(key, v);
      } else {
        v = 12;
        tree->insert(key, v);
      }
      break;
    case 1:
      // delete
      tree->del(key);
      break;
    case 2:
      // scan
      tree->range_query_from(key, scan_size, buffer);
      break;
    default:
      break;
    }

    auto us_10 = timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;

    if (++tp[id][0] == operPerTh) {
      time_per_th[id] = total_timer.end() / 1000;
      finish_oper_cnt.fetch_add(1);
      break;
    }
  }
#endif
}

void parse_args(int argc, char *argv[]) {
  if (argc != 6) {
    printf("Usage: ./benchmark kNodeCount kReadRatio kThreadCount kOperType "
           "zipfan\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kReadRatio = atoi(argv[2]);
  kThreadCount = atoi(argv[3]);
  kOperType = atoi(argv[4]);
  zipfan = atoi(argv[5]) / 100.0;

  printf("kNodeCount %d, kReadRatio %d, kThreadCount %d\n, kOperType "
         "%d zipfan %f",
         kNodeCount, kReadRatio, kThreadCount, kOperType, zipfan);
}

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

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  config.machineNR = kNodeCount;
  dsm = DSM::getInstance(config);

  dsm->registerThread();
  tree = new Tree(dsm);

  operPerTh = operNum / kThreadCount;

  if (dsm->getMyNodeID() == 0) {
    for (uint64_t i = 1; i < 102400; ++i) {
      tree->insert(to_key(i), i * 2);
    }
  }

  dsm->barrier("benchmark");
  dsm->resetThread();

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;

  timespec s, e;
  uint64_t pre_tp = 0;

  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  tree->clear_rtt_time();
  while (finish_oper_cnt.load() != kThreadCount)
    ;
  
  tree->print_rtt_time();
  clock_gettime(CLOCK_REALTIME, &e);
  int microseconds =
      (e.tv_sec - s.tv_sec) * 1000000 + (double)(e.tv_nsec - s.tv_nsec) / 1000;

  printf("oper finished, cost %d us\n",microseconds);
  uint64_t all = 0;
  uint64_t hit = 0;
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    all += (cache_hit[i][0] + cache_miss[i][0]);
    hit += cache_hit[i][0];
  }

  if (dsm->getMyNodeID() == 0) {
    cal_latency();
  }

  double per_node_tp = operNum * 1.0 / microseconds;
  uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));

  printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

  if (dsm->getMyNodeID() == 0) {
    printf("cluster throughput %.3f\n", cluster_tp / 1000.0);
    printf("cache hit rate: %lf\n", hit * 1.0 / all);
  }

  while (true)
    ;
  /*
   clock_gettime(CLOCK_REALTIME, &s);

   while (true) {
     tree->clear_rtt_time();
     sleep(2);
     tree->print_rtt_time();
     clock_gettime(CLOCK_REALTIME, &e);
     int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                        (double)(e.tv_nsec - s.tv_nsec) / 1000;

     uint64_t all_tp = 0;
     for (int i = 0; i < kThreadCount; ++i) {
       all_tp += tp[i][0];
     }
     uint64_t cap = all_tp - pre_tp;
     pre_tp = all_tp;

     uint64_t all = 0;
     uint64_t hit = 0;
     for (int i = 0; i < MAX_APP_THREAD; ++i) {
       all += (cache_hit[i][0] + cache_miss[i][0]);
       hit += cache_hit[i][0];
     }

     clock_gettime(CLOCK_REALTIME, &s);

     if (++count % 3 == 0 && dsm->getMyNodeID() == 0) {
       cal_latency();
     }

     double per_node_tp = cap * 1.0 / microseconds;
     uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));

     printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

     if (dsm->getMyNodeID() == 0) {
       printf("cluster throughput %.3f\n", cluster_tp / 1000.0);
       printf("cache hit rate: %lf\n", hit * 1.0 / all);
     }
   }
   */
  return 0;
}