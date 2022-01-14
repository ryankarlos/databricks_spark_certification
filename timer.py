from dataclasses import dataclass
import time


@dataclass
class Timer:
    def start(self):
        self._start_time = time.perf_counter()

    def stop(self):
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        print(f"Elapsed time: {elapsed_time} seconds")
        return elapsed_time

    def __enter__(self):
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info):
        """Stop the context manager timer"""
        self.stop()
