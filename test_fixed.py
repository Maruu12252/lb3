import threading
import time
from fixed_code import SharedQueueFixed

def test_bounded_queue_blocks_producer():
    """Тест підтверджує, що виробник блокується, коли черга переповнена."""
    q = SharedQueueFixed(max_size=3)
    
    # Заповнюємо чергу до ліміту
    for i in range(3):
        q.produce(i)

    # Функція намагається додати 4-й елемент
    def over_produce():
        q.produce(99)

    t = threading.Thread(target=over_produce)
    t.start()
    t.join(timeout=0.2)
    
    # Потік повинен бути живим, оскільки він заблокований (чекає місце)
    assert t.is_alive() == True 

    # Споживаємо один елемент, щоб звільнити місце
    q.consume()
    t.join(timeout=0.2)
    
    # Тепер потік завершився, елемент успішно додано
    assert t.is_alive() == False
    assert len(q._queue) == 3

def test_multiple_producers_no_data_loss():
    """Тест підтверджує, що при кількох виробниках дані обробляються повністю."""
    q = SharedQueueFixed(max_size=10)
    consumed = []

    q.register_producer()
    q.register_producer()

    def fast_producer():
        q.produce("Data-Fast")
        q.stop_producing()

    def slow_producer():
        time.sleep(0.1)
        q.produce("Data-Slow")
        q.stop_producing()

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

    # Всі дані успішно дійшли до споживача
    assert len(q._queue) == 0
    assert "Data-Fast" in consumed
    assert "Data-Slow" in consumed