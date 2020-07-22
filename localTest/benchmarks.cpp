#include "local.h"

#include <chrono>
#include <future>

using namespace std;
using namespace std::chrono;

// The maximum number of elements that can be handled by one GPU at once
// Our devices have 4GiB of memory and need two copies of the data so 2GB is
// max size
#define NMAX_PER_DEV (256*1024*1024)

// Number of devices on the system
#define NDEV 2

class timer {
  public:
    void start() {
      startTime = high_resolution_clock::now();
    }

    void stop() {
      totalTime += (int64_t)duration_cast<microseconds>(high_resolution_clock::now() - startTime).count();
    }

    int64_t report(void) {
      return totalTime;
    }

  private:
    time_point<high_resolution_clock> startTime;

    // Total time in microseconds
    int64_t totalTime = 0;
};

bool singleSort(uint32_t *data, size_t len)
{
  timer tTotal = timer();
  tTotal.start();
  bool ret = providedGpu(data, len);
  tTotal.stop();

  printf("Single Device Local Sort Statistics:\n");
  printf("\tData Size:  %lfMiB\n", (double)(len * sizeof(unsigned int)) / (1024*1024));
  printf("\tTotal Time: %jdms\n", tTotal.report());
  return ret;
}

#define DISTRIB_STEP_WIDTH 8
#define DISTRIB_NBUCKET (1 << DISTRIB_STEP_WIDTH)
#define DISTRIB_NSTEP (32 / DISTRIB_STEP_WIDTH)
bool distribSort(uint32_t *data, size_t len)
{
  // temporary place for shuffle outputs
  uint32_t *tmp = new uint32_t[len];

  // Boundary of each radix bucket in each partition
  uint32_t *p1_bkt_bounds = new uint32_t[DISTRIB_NBUCKET];
  uint32_t *p2_bkt_bounds = new uint32_t[DISTRIB_NBUCKET];

  size_t p1len = len / 2;
  size_t p2len = (len / 2) + (len % 2);
  
  timer tTotal = timer();
  timer tWorker = timer();
  timer tShuffle = timer();

  // We zero copy between steps by switching which array we're using as 'data' and which is for shuffling.
  uint32_t *cur = data;
  uint32_t *next = tmp;

  tTotal.start();
  for(int i = 0; i < DISTRIB_NSTEP; i++) {
    uint32_t *p1 = &cur[0];
    uint32_t *p2 = &cur[p1len];

    tWorker.start();
    // Single threaded sort
    // if(!gpuPartial(p1, p1_bkt_bounds, p1len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH)) {
    //   return false;
    // }
    // if(!gpuPartial(p2, p2_bkt_bounds, p2len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH)) {
    //   return false;
    // }

    // Parallel. Set to 2 because there are 2 GPUs on our standard test system
    auto p1Fut = std::async(std::launch::async, gpuPartial,
        p1, p1_bkt_bounds, p1len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH);

    auto p2Fut = std::async(std::launch::async, gpuPartial,
        p2, p2_bkt_bounds, p2len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH);

    bool p1Err = p1Fut.get();
    bool p2Err = p2Fut.get();
    if(!p1Err || !p2Err) {
      return false;
    }
    tWorker.stop();

    //shuffle
    tShuffle.start();
    size_t next_slot = 0;
    for(int bkt = 0; bkt < DISTRIB_NBUCKET; bkt++) {
      size_t p1_bkt_len, p2_bkt_len;
      if(bkt == DISTRIB_NBUCKET - 1) {
        p1_bkt_len = p1len - p1_bkt_bounds[bkt];
        p2_bkt_len = p2len - p2_bkt_bounds[bkt];
      } else {
        p1_bkt_len = p1_bkt_bounds[bkt+1] - p1_bkt_bounds[bkt];
        p2_bkt_len = p2_bkt_bounds[bkt+1] - p2_bkt_bounds[bkt];
      }

      memcpy(&next[next_slot], &p1[p1_bkt_bounds[bkt]], p1_bkt_len*sizeof(uint32_t));
      next_slot += p1_bkt_len;

      memcpy(&next[next_slot], &p2[p2_bkt_bounds[bkt]], p2_bkt_len*sizeof(uint32_t));
      next_slot += p2_bkt_len;
    }
    tShuffle.stop();

    //swap inputs for zcopy rounds
    uint32_t *tmp = next;
    next = cur;
    cur = tmp;
  }
  tTotal.stop();

  int64_t totalMS = tTotal.report();
  int64_t workerMS = tWorker.report();
  int64_t shuffleMS = tShuffle.report();

  printf("Statistics:\n");
  printf("\tData Size: %lfMiB\n", (double)(len * sizeof(unsigned int)) / (1024*1024));
  printf("\tBits per step: %d (%d steps)\n", DISTRIB_STEP_WIDTH, DISTRIB_NSTEP);
  printf("\tTotal Time: %jdms (%lfms per step)\n", totalMS, (double)totalMS / DISTRIB_NSTEP);
  printf("\tWorker Time: %jdms (%lfms per step)\n", workerMS, (double)workerMS / DISTRIB_NSTEP);
  printf("\tShuffle Time: %jdms (%lfms per step)\n", shuffleMS, (double)shuffleMS / DISTRIB_NSTEP);
	// cout << "Time taken by function: "
	// 		 << tTotal.report() << " microseconds" << endl;

  return true;
}

#include <limits.h>
// Time how long it takes to generate n ints
bool benchGenerate(size_t n) {
  timer tGen = timer();
  unsigned int *test = generateInput(n);
  tGen.stop();
	cout << "Time taken to generate " << n << " ints: "
			 << tGen.report() << " microseconds" << endl;

  unsigned int max = 0;
  unsigned int min = UINT_MAX;
  for(size_t i = 0; i < n; i++) {
    if(test[i] < min) {
      min = test[i];
    }
    if(test[i] > max) {
      max = test[i];
    }
  }
  cout << "Min: " << min << "\n";
  cout << "Max: " << max << "\n";

  freeInput(test); 
  return true;
}

bool runBenches(void)
{
  bool success;
  uint64_t maxElem = NMAX_PER_DEV*NDEV;
  unsigned int* orig = generateInput(maxElem);
  unsigned int* test = new unsigned int[maxElem];
  
  printf("Running distributed test:\n");
  memcpy(test, orig, maxElem*4);
  success = distribSort(test, maxElem);
  if(!success) {
    return false;
  }

  printf("Running Local Single-Device test:\n");
  memcpy(test, orig, NMAX_PER_DEV*sizeof(unsigned int));
  success = singleSort(test, NMAX_PER_DEV);
  if(!success) {
    return false;
  }

  delete[] test;
  freeInput(orig);
  return true;
}
