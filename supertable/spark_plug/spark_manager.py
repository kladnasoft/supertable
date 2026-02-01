import io
from pyspark.sql import SparkSession
from contextlib import redirect_stdout


class SparkClusterManager:
    def __init__(self):
        self.spark = None

    def start_cluster_generator(self):
        """Yields progress messages during Spark initialization."""
        try:
            yield "Initializing Spark Session..."
            # Connect to the Master running in Docker
            self.spark = SparkSession.builder \
                .appName("SupertableSparkNotebook") \
                .master("spark://localhost:7077") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .getOrCreate()

            yield "Connected to Master at spark://localhost:7077"
            yield "Spark Session Ready"
        except Exception as e:
            yield f"Connection Failed: {str(e)}"

    def execute_pyspark(self, code_str):
        if not self.spark:
            return {"status": "error", "message": "Spark session not started"}

        output = io.StringIO()
        exec_globals = {"spark": self.spark}

        try:
            with redirect_stdout(output):
                exec(code_str, exec_globals)
            return {"status": "success", "output": output.getvalue()}
        except Exception as e:
            import traceback
            return {"status": "error", "message": traceback.format_exc()}