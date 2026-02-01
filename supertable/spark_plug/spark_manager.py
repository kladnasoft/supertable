import io
import sys
from pyspark.sql import SparkSession
from contextlib import redirect_stdout


class SparkClusterManager:
    def __init__(self):
        self.spark = None

    def start_cluster(self):
        try:
            # Connect to the Master running in Docker via the forwarded port
            self.spark = SparkSession.builder \
                .appName("SupertableSparkNotebook") \
                .master("spark://localhost:7077") \
                .config("spark.driver.host", "localhost") \
                .getOrCreate()
            return "Spark Session Connected to Docker Cluster"
        except Exception as e:
            return f"Failed to connect: {str(e)}"

    def execute_pyspark(self, code_str):
        if not self.spark:
            return {"status": "error", "message": "Spark session not started"}

        output = io.StringIO()
        # Provide the 'spark' object to the executed code
        exec_globals = {"spark": self.spark}

        try:
            with redirect_stdout(output):
                exec(code_str, exec_globals)
            return {"status": "success", "output": output.getvalue()}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def stop_cluster(self):
        if self.spark:
            self.spark.stop()