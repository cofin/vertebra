
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from vertebra._vertebra import AsyncRing, AsyncMpmcRing

PAYLOAD = b"x" * 64
SLOTS = 1024
SLOT_SIZE = 256

def bench_spsc_mutex(total_msgs, num_threads):
    print(f"--- SPSC + Mutex ({num_threads} threads) ---")
    ring = AsyncRing(SLOT_SIZE, SLOTS)
    lock = threading.Lock()
    msgs_per_thread = total_msgs // num_threads
    
    # Consumer
    stop_event = threading.Event()
    def consumer():
        count = 0
        while not stop_event.is_set() or count < total_msgs:
            while True:
                data = ring.try_recv()
                if data is None:
                    break
                count += 1
    
    cons_thread = threading.Thread(target=consumer)
    cons_thread.start()
    
    start = time.perf_counter()
    
    def producer():
        for _ in range(msgs_per_thread):
            sent = False
            while not sent:
                with lock:
                    sent = ring.try_send(PAYLOAD, 0)
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(executor.map(lambda _: producer(), range(num_threads)))
        
    end = time.perf_counter()
    stop_event.set()
    cons_thread.join()
    
    duration = end - start
    rate = total_msgs / duration
    print(f"Time: {duration:.4f}s, Rate: {rate:,.0f} msg/s")
    return rate

def bench_mpmc(total_msgs, num_threads):
    print(f"--- MPMC Lock-Free ({num_threads} threads) ---")
    ring = AsyncMpmcRing(SLOT_SIZE, SLOTS)
    msgs_per_thread = total_msgs // num_threads
    
    # Consumer
    stop_event = threading.Event()
    def consumer():
        count = 0
        while not stop_event.is_set() or count < total_msgs:
            while True:
                data = ring.try_recv()
                if data is None:
                    break
                count += 1
    
    cons_thread = threading.Thread(target=consumer)
    cons_thread.start()
    
    start = time.perf_counter()
    
    def producer():
        for _ in range(msgs_per_thread):
            while not ring.try_send(PAYLOAD, 0):
                pass
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(executor.map(lambda _: producer(), range(num_threads)))
        
    end = time.perf_counter()
    stop_event.set()
    cons_thread.join()
    
    duration = end - start
    rate = total_msgs / duration
    print(f"Time: {duration:.4f}s, Rate: {rate:,.0f} msg/s")
    return rate

if __name__ == "__main__":
    TOTAL = 10_000
    THREADS = 4
    
    rate_spsc = bench_spsc_mutex(TOTAL, THREADS)
    rate_mpmc = bench_mpmc(TOTAL, THREADS)
    
    diff = (rate_mpmc - rate_spsc) / rate_spsc * 100
    print(f"\nImprovement: {diff:+.2f}%")
