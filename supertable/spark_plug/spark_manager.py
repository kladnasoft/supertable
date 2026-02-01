import io
import sys
import threading
import time
from pyspark.sql import SparkSession
from contextlib import redirect_stdout


class SparkClusterManager:
    def __init__(self):
        self.spark = None
        self._lock = threading.Lock()

    def start_cluster_generator(self, retries=3):
        """Yields progress messages during Spark initialization with retries."""
        with self._lock:
            if self.spark:
                yield "Spark Session already active."
                return

            for attempt in range(retries):
                try:
                    yield f"Initializing Spark session (Attempt {attempt + 1}/{retries})..."

                    self.spark = SparkSession.builder \
                        .appName("SupertableSparkNotebook") \
                        .master("spark://localhost:7077") \
                        .config("spark.driver.host", "127.0.0.1") \
                        .config("spark.driver.bindAddress", "127.0.0.1") \
                        .config("spark.executor.memory", "512m") \
                        .config("spark.cores.max", "1") \
                        .getOrCreate()

                    yield f"Connected! Spark version: {self.spark.version}"
                    yield "Spark Session Ready"
                    return
                except Exception as e:
                    yield f"Attempt {attempt + 1} failed: {str(e)}"
                    if attempt < retries - 1:
                        time.sleep(2)
                    else:
                        yield "All connection attempts failed."

    def execute_pyspark(self, code_str):
        if not self.spark:
            return {"status": "error", "message": "Spark session not started"}

        output = io.StringIO()
        # Ensure 'spark' is available in the local scope of the executed code
        exec_globals = {"spark": self.spark, "__builtins__": __builtins__}

        try:
            with redirect_stdout(output):
                exec(code_str, exec_globals)
            return {"status": "success", "output": output.getvalue()}
        except Exception as e:
            import traceback
            return {"status": "error", "message": traceback.format_exc()}

    def stop_cluster(self):
        with self._lock:
            if self.spark:
                self.spark.stop()
                self.spark = None