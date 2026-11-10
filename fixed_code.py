import threading
import time
import random
from collections import deque

class SharedQueueFixed:
    def __init__(self, max_size=10):
        self._queue = deque()
        self._lock = threading.Lock()
        
        # Дві умовні змінні на одному локу для роздільного контролю
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        
        self._active_producers = 0
        self._max_size = max_size

    def register_producer(self):
        """Реєстрація нового виробника для коректного завершення роботи."""
        with self._lock:
            self._active_producers += 1

    def produce(self, item):
        with self._not_full:
            # Чекаємо, поки в черзі не з'явиться вільне місце (вирішення проблеми №1)
            while len(self._queue) >= self._max_size:
                self._not_full.wait()
                
            self._queue.append(item)
            self._not_empty.notify() # Сповіщаємо одного споживача

    def consume(self):
        with self._not_empty:
            while not self._queue:
                if self._active_producers <= 0:
                    return None
                self._not_empty.wait()

            item = self._queue.popleft()
            self._not_full.notify() # Сповіщаємо виробника, що з'явилося місце
            return item

    def stop_producing(self):
        with self._lock:
            self._active_producers -= 1
            # Зупиняємо споживачів ЛИШЕ якщо всі виробники завершили роботу (вирішення проблеми №2)
            if self._active_producers <= 0:
                self._not_empty.notify_all()

# --- Приклад використання ---
if __name__ == "__main__":
    q = SharedQueueFixed(max_size=5)
    
    def producer_task(q, count):
        q.register_producer()
        for i in range(count):
            q.produce(f"Дані-{i}")
            time.sleep(random.uniform(0.01, 0.1))
        q.stop_producing()

    def consumer_task(q):
        while True:
            item = q.consume()
            if item is None:
                break
            print(f"[{threading.current_thread().name}] Обробив: {item}")
            time.sleep(random.uniform(0.05, 0.15))

    producers = [threading.Thread(target=producer_task, args=(q, 10), name=f"Prod-{i}") for i in range(2)]
    consumers = [threading.Thread(target=consumer_task, args=(q,), name=f"Cons-{i}") for i in range(3)]

    for p in producers: p.start()
    for c in consumers: c.start()
    for p in producers: p.join()
    for c in consumers: c.join()
    
    print("Усі потоки завершили роботу коректно.")