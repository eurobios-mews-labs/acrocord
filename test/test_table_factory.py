# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import os
from typing import Dict, Tuple

import pandas as pd
import pytest

from acrocord.factory.table import TableFactory
from acrocord.utils.types import EligibleDataType


class BuildingTableFactory(TableFactory):
    # name of the table
    @classmethod
    def table_name(cls) -> str:
        return 'buildings'

    # name of the schema where to store the table
    @classmethod
    def schema_name(cls) -> str:
        return 'test'

    # column format statement and documentation.
    @classmethod
    def data_definition(cls) -> Dict[str, Tuple[EligibleDataType, str]]:
        return {
            'building_id': (EligibleDataType.INTEGER, 'Identification number'),
            'architect': (EligibleDataType.STRING, 'Architect name'),
            'height': ("float16", ''),
            'construction_date': (
                EligibleDataType.DATE_TIME,
                'Construction Date of the building'),
            # 'is_constructed': (
            #     EligibleDataType.BOOLEAN, 'Does the building already construct') -> test
        }

    # this is the primary key of your table it must exist in
    # BuildingTableFactory._doc_cols
    # and as it is a row identifier, each row of your
    # table must have a unique non-null value
    @classmethod
    def id_key(cls) -> str:
        return 'building_id'

    # This is the main table where you get data from other sources
    # (possibly another TableFactory).
    # The only restriction is to
    def _create_table(self) -> None:
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
        df['construction_date'] = pd.to_datetime(df.construction_date,
                                                 format='%d/%m/%Y')
        from datetime import datetime
        df['is_constructed'] = df.construction_date < datetime.now()

        self._set_table(df)  # Required line to store the result

    @classmethod
    def get_db_connection(cls):
        return cls.connection


@pytest.fixture
def get_table_factory(get_connection) -> BuildingTableFactory:
    BuildingTableFactory.connection = get_connection
    table_factory = BuildingTableFactory(verbose=True)
    return table_factory


def test_write(get_table_factory: BuildingTableFactory, get_connection) -> None:

    get_table_factory.write_table()
    get_table_factory.get_table()
    assert "buildings" in get_connection.get_table_names("test")


def test_read(get_table_factory: BuildingTableFactory) -> None:
    get_table_factory.write_table()
    get_table_factory.read_table()


def test_implement_foreign_keys(
        get_table_factory: BuildingTableFactory) -> None:
    get_table_factory.add_foreign_keys()


def test_get_table_name(get_table_factory: BuildingTableFactory) -> None:
    assert "test.buildings" == get_table_factory.get_full_name()


def test_write_to_excel(get_table_factory: BuildingTableFactory) -> None:
    import tempfile
    get_table_factory.write_to_excel()
    assert "buildings.xlsx" in os.listdir(tempfile.gettempdir())


def test_columns(get_table_factory: BuildingTableFactory) -> None:
    print(get_table_factory.columns)
    for c in get_table_factory.columns:
        assert c in ['building_id', 'architect', 'height',
                     'construction_date',
                     'is_constructed'], f"column {c} is not in columns"
    assert isinstance(get_table_factory.columns,
                      list), "columns attribute is not a list"


def test_get_instance(get_table_factory):
    inst = get_table_factory.get_instance()
    assert inst._table is None

    inst.get_table()


def test_check_table_doc(get_table_factory):
    get_table_factory.check_table_doc(get_table_factory.get_table(),
                                      get_table_factory.data_definition())

# Test to add
# TODO:  test if deployed and _table is freed
# TODO:  test if second _get_data is faster
