#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <cstdio>
#include <mutex>
// #include <print>
#include <queue>
#include <thread>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

class task {
public:
  IRunnable *runnable;
  int task_id;
  int num_total_tasks;

  void run() { runnable->runTask(task_id, num_total_tasks); };
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */

class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
  std::vector<std::thread> thread_pool = {};
  std::mutex mutex = {};
  std::queue<task> work_queue = {};

public:
  std::atomic<bool> stop = {};

  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
  void worker(int id) {
    // std::println("worker {}: started", id);
    for (;;) {
      // std::println("worker {}: stop check", id);
      if (stop.load()) {
        // std::println("worker {}: stopping", id);
        return;
      }

      // std::println("worker {}: waiting on lock", id);
      mutex.lock();
      // std::println("worker {}: lock acquired", id);
      if (work_queue.empty()) {
        // std::println("worker {}: empty continuing", id);
        mutex.unlock();
        continue;
      }

      // std::println("worker {}: running task", id);
      auto t = work_queue.front();
      work_queue.pop();
      mutex.unlock();

      t.run();
    }
  };
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
  std::vector<std::thread> thread_pool = {};
  std::mutex mutex = {};
  std::condition_variable_any cond = {};
  std::queue<task> work_queue = {};
  bool _stop = {};

public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

  void stop() {
    // std::println("stopping");
    mutex.lock();
    // std::println("stopping lock acquired");
    _stop = true;
    mutex.unlock();
    // std::println("stopping unlock");
    cond.notify_all();
    // std::println("stopping notified all");
  };
  void worker(int id) {
    // std::println("worker {}: started", id);
    for (;;) {
      // std::println("worker {}: waiting on lock", id);
      mutex.lock();
      // std::println("worker {}: lock acquired", id);

      cond.wait(mutex, [this, id] {
        // std::println("worker {}: predicate: {} {}", id, !work_queue.empty(),
        // _stop);
        return !work_queue.empty() || _stop;
      });
      // std::println("worker {}: wait done", id);

      if (_stop) {
        // std::println("STOPPING WITH QUEUE SIZE OF: {}", work_queue.size());
        mutex.unlock();
        return;
      }

      if (work_queue.empty()) {
        // std::println("worker {}: no work to do", id);
        mutex.unlock();
        continue;
      }
      // std::println("worker {}: doing work", id);

      auto t = work_queue.front();
      work_queue.pop();
      mutex.unlock();

      t.run();
    }
  };
};

#endif
