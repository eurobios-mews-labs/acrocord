# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import pandas as pd
import pytest

from acrocord import ConnectDatabase


@pytest.fixture(autouse=True)
def get_connection(postgresql):
    db = ConnectDatabase()
    connection = db.connect(verbose=3,
                            connection={
                                "dbname": postgresql.info.dbname,
                                "password": " ",
                                "user": postgresql.info.user,
                                "port": postgresql.info.port,
                                "host": postgresql.info.host
                            })
    connection.create_schema("test")
    return connection


@pytest.fixture(scope="module")
def get_example_data_frame():
    dataframe = pd.DataFrame({'a': [155, 20, 3],
                              'b': [11, 299, 45],
                              'c': [73, 3, 39],
                              'd': [783, 488, 739],
                              "lat": [45, 45, 45],
                              "lon": [0, 1, 2]
                              })
    return dataframe


@pytest.fixture(scope="module")
def get_example_data_frame_other():
    dataframe = pd.DataFrame({'a': [155, 20, 155],
                              'e': ["a", "b", "b"]
                              })
    return dataframe


@pytest.fixture(scope="module")
def db_connector(get_connection):
    return get_connection


@pytest.fixture(scope="module")
def get_example_dataframe_building():
    df = pd.DataFrame(data={'building_id': [11, 20, 14, 34, 61],
                            'architect': ["Durand", "Blanc", "Blanc",
                                          "Dubois", "Martin"],
                            'height': [14.4, 24.4, 35.3, 12.3, 14.4],
                            'construction_date': ['10/03/1957',
                                                  '30/11/2087',
                                                  '01/02/2070',
                                                  '04/01/1989',
                                                  '28/10/2003']
                            })
    return df
