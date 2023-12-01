# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings

import pandas as pd


def test_dataframe_equals_reading_writing(get_connection,
                                          get_example_data_frame):

    get_connection.write_table(get_example_data_frame, 'test.test_data')
    dataframe_read = get_connection.read_table('test.test_data')
    get_connection.drop_table('test.test_data')
    assert get_example_data_frame.equals(
        dataframe_read), "dataframe and dataframe_read are the same"


def test_warning_types(get_example_data_frame):
    from acrocord.utils.types import warning_type
    dataframe = get_example_data_frame.astype(int)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        warning_type(dataframe, 3)
        assert len(w) == dataframe.shape[1]


def test_warning_types_64(get_example_data_frame):
    from acrocord.utils.types import warning_type
    dataframe = get_example_data_frame.astype("int64")
    warning_type(dataframe, 3)


def test_get_metadata(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, 'test.dataframe')
    df_meta = get_connection.get_metadata('test.dataframe')
    error_msg = "The number of line in meta data does correspond to the number of columns "
    number_columns = len(get_connection.get_columns('test.dataframe'))
    get_connection.drop_table('test.dataframe')
    assert len(df_meta) == number_columns, error_msg


def test_foreign_keys(get_connection, get_example_data_frame,
                      get_example_data_frame_other):
    df1 = get_example_data_frame
    df2 = get_example_data_frame_other
    get_connection.write_table(df1, "test.main", primary_key='a')
    get_connection.write_table(df2, "test.other",
                               foreign_keys={'a': "test.main"})


def test_get_shape(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main", primary_key='a')
    s = get_connection.get_shape("test.main")
    assert (s == get_example_data_frame.shape)


def test_query_where(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main",
                               primary_key='a')
    df = get_connection.read_table("test.main", where="a in (155)")
    assert len(df) == 1


def test_query_limit(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main",
                               primary_key='a')
    df = get_connection.read_table("test.main", limit=2)
    assert len(df) == 2


def test_create_insert(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main")
    get_connection.create_insert(get_example_data_frame, "test.main")
    df = get_connection.read_table("test.main")
    assert len(df) == len(get_example_data_frame) * 2


def test_create_insert_create(get_connection, get_example_data_frame):
    get_connection.drop_table("test.main")
    get_connection.create_insert(get_example_data_frame, "test.main")
    df = get_connection.read_table("test.main")
    assert len(df) == len(get_example_data_frame)


def test_create_drop_create_column(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main")
    get_connection.drop_column("test.main", "b")
    df = get_connection.read_table("test.main")
    assert df.shape[1] == get_example_data_frame.shape[1] - 1

    get_connection.add_columns(
        "test.main",
        get_example_data_frame[["a", "b"]],
        index="a"
    )


def test_copy(get_connection, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.main")
    get_connection.copy("test.main", "test.main_copy")
    df1 = get_connection.read_table("test.main")
    df2 = get_connection.read_table("test.main_copy")
    assert df1.equals(df2)


def test_get_table_names(get_connection, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.main")
    assert len(get_connection.get_table_names("test")) > 0


def test_get_view_names(get_connection, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.main")
    get_connection.execute("create view public.test as select * from test.main")
    assert len(get_connection.get_view_names("public")) > 0


def test_create_schema(get_connection, get_example_data_frame):
    get_connection.create_schema("test_ci")
    get_connection.write_table(get_example_data_frame, "test_ci.test_schema")
    assert "test_ci" in get_connection.get_schema_names()


# def test_geopandas(get_connection, get_example_data_frame):
#     import geopandas
#     connection = get_connection
#     dataframe = get_example_data_frame
#     gdf = geopandas.GeoDataFrame(
#         dataframe,
#         geometry=geopandas.points_from_xy(dataframe.lon, dataframe.lat))
#     connection.write_table(gdf, "test.test_lat_lon")


def test_list_table(get_connection):
    list_table = get_connection.list_table("public")
    print(list_table)


def test_update(get_connection, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.other")
    get_connection.update("test.other", where="a=155",
                          value="150", column="a")
    df_read = get_connection.read_table("test.other")
    assert 150 in df_read["a"].values


def test_get_dtypes(get_connection, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.other")
    res = get_connection.get_dtypes("test.other")
    assert isinstance(res, pd.DataFrame)
    assert len(res) == len(get_connection.get_columns("test.other"))


def test_drop(get_connection, get_example_data_frame, get_example_data_frame_other):
    get_connection.write_table(get_example_data_frame_other, "test.other")
    get_connection.write_table(get_example_data_frame, "test.main")
    get_connection.drop("test.main", "table", "a")
    assert "a" not in get_connection.get_columns("test.main")
    get_connection.write_table(get_example_data_frame_other, "test.main")


def test_execute(get_connection, get_example_data_frame):
    get_connection.write_table(get_example_data_frame, "test.main",
                               primary_key='a')
    get_connection.execute("select * from test.main")


def test_names_empty_list(get_connection):
    get_connection.execute("DROP SCHEMA if exists test_names")
    get_connection.create_schema("test_names")
    assert len(get_connection.get_table_names("test_names")) == 0
    get_connection.execute("DROP SCHEMA test_names")
