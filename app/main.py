from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from app.counter_service import UserCounterService
from app.database_config import DATABASE_URL


def run_test(service, method_name, method_func):
    print(f"\n{'=' * 50}")
    print(f"Testing: {method_name}")
    print(f"{'=' * 50}")

    service.setup()
    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda i: method_func(i), range(10))

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    final_counter = service.get_counter()

    print(f"\n--- Results ---")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Expected counter: 100000")
    print(f"Actual counter: {final_counter}")
    print(f"Lost updates: {100000 - final_counter}")
    print(f"Accuracy: {(final_counter / 100000 * 100):.2f}%")


if __name__ == "__main__":
    service = UserCounterService(DATABASE_URL)

    run_test(service, "LOST UPDATE", service.increment_lost_update)
    run_test(service, "SERIALIZABLE", service.increment_serializable)
    run_test(service, "IN-PLACE UPDATE", service.increment_in_place)
    run_test(service, "ROW-LEVEL LOCKING", service.increment_row_locking)
    run_test(service, "OPTIMISTIC CONCURRENCY", service.increment_optimistic)
