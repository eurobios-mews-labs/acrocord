# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
import pytest
from sqlalchemy.exc import ProgrammingError

from acrocord.utils import auxiliaries


def test_execute_sql_cmd_raise_error(get_connection):
    sql_cmd_with_error = "select *"
    with pytest.raises(ProgrammingError) as e_info:
        auxiliaries.execute_sql_cmd(get_connection, sql_cmd_with_error)


def test_drop_table():
    assert auxiliaries.drop_table("test") == "DROP TABLE IF " \
                                             "EXISTS test CASCADE;"


def test_count():
    assert auxiliaries.count("test") == "SELECT COUNT(*) FROM test;"


def test_list_table():
    assert auxiliaries.list_table(
        "test") == f"SELECT * FROM information_schema.tables " \
                   f"WHERE table_schema = 'test'"


def test_data_types():
    a, b, c, d = "int32", "int4", "datetime64[s]", "timestamp"
    assert auxiliaries.db_data_type(a) == b
    assert auxiliaries.db_data_type(b, invert=True) == a
    assert auxiliaries.db_data_type(c) == d
    assert auxiliaries.db_data_type(d, invert=True) == c
