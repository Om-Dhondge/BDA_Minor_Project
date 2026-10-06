import os

from lib.session import _ensure_hadoop_home


def test_preset_hadoop_home_still_puts_its_bin_on_path(tmp_path, monkeypatch):
    """hadoop.dll loads from PATH, not from HADOOP_HOME.

    Once HADOOP_HOME is set at user scope, every new shell inherits it, but
    not %HADOOP_HOME%\\bin on PATH. Without it the JVM cannot load hadoop.dll,
    and GraphFrames' Connected Components dies with UnsatisfiedLinkError on
    NativeIO$Windows.access0.
    """
    hadoop_bin = tmp_path / "bin"
    hadoop_bin.mkdir()
    (hadoop_bin / "winutils.exe").touch()
    monkeypatch.setenv("HADOOP_HOME", str(tmp_path))
    monkeypatch.setenv("PATH", r"C:\Windows\System32")

    _ensure_hadoop_home()

    assert str(hadoop_bin) in os.environ["PATH"].split(os.pathsep)


def test_session_binds_to_loopback(spark):
    """Local mode must not depend on the machine's network address.

    Spark otherwise advertises the Wi-Fi address it found at startup. When the
    laptop switched networks mid-run, the driver could no longer reach its own
    block manager and Connected Components died with TaskResultLost.
    """
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"
