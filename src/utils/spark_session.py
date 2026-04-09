import os
import shutil
import sys
from pyspark.sql import SparkSession


def _validate_java_runtime() -> None:
    java_home = os.environ.get("JAVA_HOME")
    java_on_path = shutil.which("java")
    java_from_home = None

    if java_home:
        java_exe = "java.exe" if os.name == "nt" else "java"
        java_from_home = os.path.join(java_home, "bin", java_exe)

    java_ready = bool(java_on_path) or (java_from_home and os.path.exists(java_from_home))
    if not java_ready:
        raise RuntimeError(
            "Java runtime not found. Install JDK 17+ and set JAVA_HOME "
            "(example on Windows PowerShell: "
            "$env:JAVA_HOME='C:\\Program Files\\Eclipse Adoptium\\jdk-17.x.x'; "
            "$env:Path += ';'+$env:JAVA_HOME+'\\bin')."
        )


def get_spark():
    _validate_java_runtime()
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("TradeIntel")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )
    return spark