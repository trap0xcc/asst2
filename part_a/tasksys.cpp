#include "tasksys.h"
#include <chrono>
// #include <print>
#include <ratio>
#include <thread>

using namespace std::chrono_literals;

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  std::vector<std::thread> threads;
  threads.reserve(num_total_tasks);
  for (int i = 0; i < num_total_tasks; i++) {
    threads.push_back(
        std::thread(&IRunnable::runTask, runnable, i, num_total_tasks));
  }

  for (auto &t : threads)
    t.join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  thread_pool.reserve(num_threads);
  for (int i = 0; i < num_threads; i++)
    thread_pool.push_back(
        std::thread(&TaskSystemParallelThreadPoolSpinning::worker, this, i));
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  stop.store(true);
  for (auto &t : thread_pool)
    t.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // std::println(".run called: {}", num_total_tasks);
  mutex.lock();
  for (int i = 0; i < num_total_tasks; i++) {
    work_queue.push(task{runnable, i, num_total_tasks});
  }
  mutex.unlock();

  // NOTE: one limitaiton of this style of implementing sync run is that
  // multiple threads calling run on the same instance will block each other
  // until the shared work_queue has been completed.
  // NOTE: also checking work_queue.empty does not ensure that all work has
  // completed, just that it has been assigned to a worker thread.
  bool done = false;
  for (;;) {
    mutex.lock();
    done = work_queue.empty();
    // std::println("main: empty: {}", done);
    // std::println("main: count: {}", work_queue.size());
    mutex.unlock();
    if (done) {
      // std::println("main: done");
      return;
    }
    std::this_thread::sleep_for(1ms);
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads) {
  thread_pool.reserve(num_threads);
  for (int i = 0; i < num_threads; i++)
    thread_pool.push_back(
        std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this, i));
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  stop();
  for (auto &t : thread_pool)
    t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // std::println("ADDING WORK {}", num_total_tasks);
  mutex.lock();
  for (int i = 0; i < num_total_tasks; i++) {
    work_queue.push(task{runnable, i, num_total_tasks});
  }
  mutex.unlock();
  cond.notify_all();

  // NOTE: one limitaiton of this style of implementing sync run is that
  // multiple threads calling run on the same instance will block each other
  // until the shared work_queue has been completed.
  // NOTE: also checking work_queue.empty does not ensure that all work has
  // completed, just that it has been assigned to a worker thread.
  bool done = false;
  // int count = 0;
  for (;;) {
    // std::println("main: waiting on lock for done check");
    mutex.lock();
    // std::println("main: lock acquired");
    done = work_queue.empty();
    // count = work_queue.size();
    mutex.unlock();
    // std::println("main: lock released");
    if (done) {
      // std::println("main: done");
      return;
    }
    // std::println("main: !done, {}", count);
    std::this_thread::sleep_for(1ms);
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}
