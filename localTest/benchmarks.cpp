#include "local.h"

#include <chrono>
#include <future>

using namespace std;
using namespace std::chrono;

class timer {
  public:
    timer(void) {
      startTime = high_resolution_clock::now();
    }

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
  
  // auto totalStart = high_resolution_clock::now();
  timer tTotal = timer();

  // We zero copy between steps by switching which array we're using as 'data' and which is for shuffling.
  uint32_t *cur = data;
  uint32_t *next = tmp;
  for(int i = 0; i < DISTRIB_NSTEP; i++) {
    uint32_t *p1 = &cur[0];
    uint32_t *p2 = &cur[p1len];

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

    //shuffle
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

    //swap inputs for zcopy rounds
    uint32_t *tmp = next;
    next = cur;
    cur = tmp;
  }

  // auto totalEnd = high_resolution_clock::now();
  //
	// auto duration = duration_cast<microseconds>(end - start);

  tTotal.stop();
	cout << "Time taken by function: "
			 << tTotal.report() << " microseconds" << endl;

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

bool runBenches(int nelem)
{
  bool success;
  unsigned int *test = generateInput(nelem);
  
  success = distribSort(test, nelem);
  if(!success) {
    return false;
  }

  freeInput(test);
  return true;
}
