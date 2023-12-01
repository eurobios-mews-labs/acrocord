import os
import time

import pandas as pd

from acrocord.utils import cache


def test_name():
    assert cache.name("a", "b", "c", 1) == "a_b_c_1"


def test_set_cache_folder_exists():
    cache.set_cache_folder()
    assert os.path.exists(cache.cache_folder)


def test_size_cache():
    cache.clean_cache()
    assert cache.folder_size_gio() == 0
    cache.set_cache_folder()
    cache.write(pd.DataFrame(index=range(1000), columns=range(1000)), "test")
    assert cache.folder_size_gio() > 0
    cache.clean_cache()
    assert cache.folder_size_gio() == 0


def test_write(
        get_connection,
        get_connection_cache,
        get_example_dataframe_building
):
    get_connection.create_schema("main")
    get_connection.write_table(get_example_dataframe_building, "test")
    get_connection_cache.create_schema("main")
    get_connection_cache.write_table(get_example_dataframe_building, "test")
    get_connection.read_table("main.test")
    get_connection_cache.read_table("main.test")

    start = time.time()
    df1 = get_connection.read_table("main.test")
    end = time.time()
    t1 = end - start

    start = time.time()
    df2 = get_connection_cache.read_table("main.test")
    end = time.time()

    t2 = end - start
    assert t2 < t1
    print(t1, t2)
    assert df1.equals(df2)
