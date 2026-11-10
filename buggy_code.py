import threading, time, random
from collections import deque

class SharedQueue:
    def __init__(self):
        self._queue = deque()
        self._cv = threading.Condition()
        self._is_producing = True

    def produce(self, item):
        with self._cv:
            self._queue.append(item)
            self._cv.notify()

    def consume(self):
        with self._cv:
            while not self._queue:
                if not self._is_producing:
                    print(f"Потік {threading.current_thread().name}: Виробник завершив, черга порожня. Завершую.")
                    return None
                self._cv.wait()

            if not self._queue:
                return None

            item = self._queue.popleft()
            print(f"Потік {threading.current_thread().name}: Обробив {item}")
            return item

    def stop_producing(self):
        with self._cv:
            self._is_producing = False
            self._cv.notify_all()

def producer_task(q, count):
    for i in range(count):
        q.produce(f"Дані-{i}")
        time.sleep(random.uniform(0.1, 0.3))
    q.stop_producing()

def consumer_task(q):
    while True:
        item = q.consume()
        if item is None:
            break
        time.sleep(random.uniform(0.1, 0.5))

shared_data_queue = SharedQueue()

producer_thread = threading.Thread(target=producer_task, args=(shared_data_queue, 10))
consumers = [threading.Thread(target=consumer_task, args=(shared_data_queue,)) for _ in range(3)]

producer_thread.start()
[t.start() for t in consumers]

producer_thread.join()
[t.join() for t in consumers]

print("Усі потоки завершили роботу.")
