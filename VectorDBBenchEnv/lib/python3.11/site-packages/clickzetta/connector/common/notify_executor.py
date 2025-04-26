import logging
import queue
import threading
import time
from concurrent.futures import Future
from queue import PriorityQueue
from threading import Condition, Lock
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)


class DelayedTask:
    """Task with delayed execution time"""

    def __init__(self, run_time_ns: int, task: Callable[[], Any], future: Optional[Future] = None):
        self.run_time_ns = run_time_ns
        self.task = task
        self.future = future or Future()
        self.notify = False

    def __lt__(self, other):
        return self.run_time_ns < other.run_time_ns


class NotifyScheduledExecutor:
    """Executor service that supports scheduled tasks with notification"""

    def __init__(self, name: str, core_pool_size: int = 4):
        self.name = name
        self.core_pool_size = core_pool_size
        self.task_queue: PriorityQueue = PriorityQueue()
        self.lock = Lock()
        self.condition = Condition(self.lock)
        self.exception = None
        self.is_stopped = False
        self.workers: list[threading.Thread] = []
        self._init_workers()

    def _init_workers(self):
        """Initialize worker threads"""
        for i in range(self.core_pool_size):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"{self.name}-notify-worker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)

    def _worker_loop(self):
        """Main worker loop for processing tasks"""
        while not self.is_stopped:
            try:
                # Get next task
                try:
                    task = self.task_queue.get(timeout=0.5)
                except queue.Empty:
                    if self.is_stopped:
                        return
                    continue

                # Check if task should run now
                delay = task.run_time_ns - time.time_ns()
                if delay > 0 and not task.notify:
                    # Put back and wait if not ready
                    self.task_queue.put(task)
                    with self.condition:
                        self.condition.wait(delay / 1e9)
                    continue

                # Execute task
                try:
                    result = task.task()
                    if task.future:
                        task.future.set_result(result)
                except Exception as e:
                    if task.future:
                        task.future.set_exception(e)
                    self.exception = e
                finally:
                    self.task_queue.task_done()
                    with self.condition:
                        self.condition.notify_all()

            except Exception as e:
                log.error(f"Worker error: {e}")
                self.exception = e
                return

    def add_task(self, task: Callable[[], Any], delay_ms: int) -> Future:
        """Schedule a task with delay"""
        if self.is_stopped:
            raise RuntimeError(f"NotifyScheduledExecutor {self.name} is stopped")
        if self.exception:
            raise self.exception

        run_time = time.time_ns() + (delay_ms * 1_000_000)
        delayed_task = DelayedTask(run_time, task)
        self.task_queue.put(delayed_task)

        with self.condition:
            self.condition.notify_all()

        return delayed_task.future

    def notify_all_scheduled_tasks(self):
        """Notify all tasks to run immediately"""
        # Mark all tasks for immediate execution
        with self.task_queue.mutex:
            for task in self.task_queue.queue:
                task.notify = True

        # Wake up workers
        with self.condition:
            self.condition.notify_all()

    def close(self, wait_time_ms: float = 1000):
        """Shutdown the executor"""
        self.is_stopped = True
        log.info(f"Shutting down {self.name} executor...")
        # Try to complete remaining tasks
        self.notify_all_scheduled_tasks()

        # Wait for workers to finish
        end_time = time.time() + (wait_time_ms / 1000)
        for worker in self.workers:
            timeout = max(0.0, end_time - time.time())
            try:
                worker.join(timeout=timeout)
            except Exception:
                pass

        # Force shutdown if needed
        still_running = [w for w in self.workers if w.is_alive()]
        if still_running:
            log.warning(f"{len(still_running)} workers did not shutdown gracefully")

        if self.exception:
            raise self.exception
