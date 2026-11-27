import pytest
import quasardb


def test_perf_disable(native_insecure_cluster):
    native_insecure_cluster.perf().disable()


def test_perf_enable(native_insecure_cluster):
    native_insecure_cluster.perf().enable()


def test_perf_enable_secure_conn(native_secure_cluster):
    native_secure_cluster.perf().enable()


def test_perf_get__when_empty(native_insecure_cluster):
    profiles = native_insecure_cluster.perf().get()
    assert len(profiles) == 0


def test_perf_get_after_blob_put(native_insecure_cluster, entry_name, random_string):
    native_insecure_cluster.perf().enable()
    native_insecure_cluster.blob(entry_name).put(random_string)
    native_insecure_cluster.blob(entry_name).get()

    profiles = native_insecure_cluster.perf().get()
    assert len(profiles) == 2
    assert profiles[0][0] == "blob.put"
    assert profiles[1][0] == "common.get"


def test_perf_clear_after_blob_put(native_insecure_cluster, entry_name, random_string):
    native_insecure_cluster.perf().enable()
    native_insecure_cluster.blob(entry_name).put(random_string)

    profiles = native_insecure_cluster.perf().get()
    assert len(profiles) != 0

    native_insecure_cluster.perf().clear()
    profiles = native_insecure_cluster.perf().get()
    assert len(profiles) == 0


def test_perf_flamechart(native_insecure_cluster, entry_name, random_string):
    native_insecure_cluster.perf().enable()
    native_insecure_cluster.blob(entry_name).put(random_string)

    x = native_insecure_cluster.perf().get(flame=True)
    assert len(x) > 0

    x = native_insecure_cluster.perf().get(flame=True)
    assert len(x) == 0


def test_perf_flamechart_to_file(native_insecure_cluster, entry_name, random_string):
    native_insecure_cluster.perf().enable()
    native_insecure_cluster.blob(entry_name).put(random_string)

    filename = "test_perf.out"
    xs = native_insecure_cluster.perf().get(flame=True, outfile=filename)

    x = "\n".join(xs)
    x = x + "\n"

    with open(filename, "r") as fp:
        assert x == fp.read()
