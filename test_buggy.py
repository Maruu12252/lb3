import threading
import time
from buggy_code import SharedQueue

def test_unbounded_queue_memory_leak():
    """Перевірка помилки №1: черга дозволяє безконтрольне додавання елементів."""
    q = SharedQueue()
    # Виробник миттєво додає 10 000 елементів
    for i in range(10000):
        q.produce(i)
    # Розмір черги не обмежений, що може призвести до MemoryError
    assert len(q._queue) == 10000 

def test_multiple_producers_data_loss():
    """Перевірка помилки №2: споживач зупиняється передчасно, гублячи дані іншого виробника."""
    q = SharedQueue()
    consumed = []

    def fast_producer():
        q.produce("Data-Fast")
        q.stop_producing()  # Миттєво зупиняє споживачів

    def slow_producer():
        time.sleep(0.1) # Імітація довгої роботи
        q.produce("Data-Slow") # Ці дані будуть втрачені

    def consumer_task():
        while True:
            item = q.consume()
            if item is None:
                break
            consumed.append(item)

    c = threading.Thread(target=consumer_task)
    p1 = threading.Thread(target=fast_producer)
    p2 = threading.Thread(target=slow_producer)

    c.start(); p1.start(); p2.start()
    p1.join(); p2.join(); c.join()

    # Дані від повільного виробника зависли у черзі
    assert len(q._queue) == 1
    # Споживач їх не отримав
    assert "Data-Slow" not in consumed
    assert "Data-Fast" in consumed