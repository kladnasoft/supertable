import io
from pyspark.sql import SparkSession
from contextlib import redirect_stdout


class SparkClusterManager:
    def __init__(self):
        self.spark = None

    def start_cluster(self):
        try:
            # Persistent session connected to your Docker cluster
            self.spark = SparkSession.builder \
                .appName("SupertableSparkNotebook") \
                .master("spark://localhost:7077") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .getOrCreate()
            return "Spark Session Ready"
        except Exception as e:
            return f"Failed to connect: {str(e)}"

    def execute_pyspark(self, code_str):
        if not self.spark:
            return {"status": "error", "message": "Spark session not started"}

        output = io.StringIO()
        # Passing the session into the exec context allows multi-line code to use 'spark'
        exec_globals = {"spark": self.spark}

        try:
            with redirect_stdout(output):
                # Using exec for dynamic execution of multiple lines
                exec(code_str, exec_globals)
            return {"status": "success", "output": output.getvalue()}
        except Exception as e:
            import traceback
            return {"status": "error", "message": traceback.format_exc()}