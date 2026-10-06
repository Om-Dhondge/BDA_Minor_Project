import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Coordinate confirmed working in Task 1 Step 3. Update here if it changes.
# Resolves from the spark-packages repo, not Maven Central.
GRAPHFRAMES_PACKAGE = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"

DEFAULT_SCRATCH = Path(os.environ.get("SPARK_SCRATCH", r"C:\spark-tmp"))

# Hadoop's Shell class shells out to winutils.exe for local-filesystem chmod,
# which SparkContext.addFile triggers for every jar pulled in by
# spark.jars.packages. Without it the JVM dies before any work starts.
DEFAULT_HADOOP_HOME = Path(r"C:\hadoop")


def _java_home_from_registry():
    """Read the user-scope JAVA_HOME straight out of the registry."""
    if sys.platform != "win32":
        return None
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            return winreg.QueryValueEx(key, "JAVA_HOME")[0]
    except OSError:
        return None


def _java_home_from_disk():
    """Locate a Temurin JDK 17 install without consulting the environment."""
    root = Path(r"C:\Program Files\Eclipse Adoptium")
    if not root.exists():
        return None
    matches = sorted(root.glob("jdk-17*"), reverse=True)
    return matches[0] if matches else None


def _ensure_java_home():
    """Recover JAVA_HOME when the shell predates the JDK install.

    Setting JAVA_HOME at user scope only reaches processes started afterwards.
    A terminal - or the editor that spawned it - opened before the JDK was
    installed keeps its original environment, and PySpark then dies with
    `JAVA_GATEWAY_EXITED: Java gateway process exited before sending its port
    number`, whose text never mentions the real cause. Fall back to the
    registry value, then to the install location on disk.
    """
    if os.environ.get("JAVA_HOME"):
        return

    for candidate in (_java_home_from_registry(), _java_home_from_disk()):
        if candidate and (Path(candidate) / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return


def _ensure_hadoop_home():
    """Point HADOOP_HOME at the winutils install, and put its bin on PATH.

    The JVM loads hadoop.dll from PATH, not from HADOOP_HOME. A shell that
    inherits HADOOP_HOME from user scope still lacks its bin on PATH, and
    GraphFrames' Connected Components then dies with UnsatisfiedLinkError on
    NativeIO$Windows.access0. So PATH is fixed up even when HADOOP_HOME is set.
    """
    if not os.environ.get("HADOOP_HOME"):
        if not (DEFAULT_HADOOP_HOME / "bin" / "winutils.exe").exists():
            return
        os.environ["HADOOP_HOME"] = str(DEFAULT_HADOOP_HOME)
    hadoop_bin = str(Path(os.environ["HADOOP_HOME"]) / "bin")
    path = os.environ.get("PATH", "")
    if hadoop_bin.lower() not in path.lower().split(os.pathsep):
        os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{path}"


def _ensure_python_workers():
    """Pin executor workers to the interpreter running this process.

    Left unset, Spark launches workers as bare `python3`, which on this machine
    is the Windows Store stub - no pyspark, so every worker dies on import and
    the JVM reports only `Python worker exited unexpectedly (crashed)`.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_session(app_name, scratch_root=None, driver_memory="8g"):
    """Create a local-mode SparkSession with GraphFrames and checkpointing ready.

    Scratch directories default to C: because D: has insufficient free space
    for shuffle spill.
    """
    _ensure_java_home()
    _ensure_hadoop_home()
    _ensure_python_workers()

    scratch = Path(scratch_root) if scratch_root else DEFAULT_SCRATCH
    local_dir = scratch / "local"
    checkpoint_dir = scratch / "checkpoints"
    local_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # spark.driver.memory must reach the launcher before the JVM starts; in
    # local mode a builder config alone is applied too late to matter.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {driver_memory} pyspark-shell"
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", GRAPHFRAMES_PACKAGE)
        .config("spark.local.dir", str(local_dir))
        .config("spark.driver.memory", driver_memory)
        # Loopback, not the hostname: the hostname resolves to the Wi-Fi
        # address, and a network switch mid-run leaves the driver unable to
        # reach its own block manager (TaskResultLost).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(str(checkpoint_dir))
    spark.sparkContext.setLogLevel("WARN")
    return spark
