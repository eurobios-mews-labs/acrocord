# Copyright 2023 Eurobios
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#
from typing import Type, Iterable

import pandas as pd

from acrocord import ConnectDatabase
from acrocord.utils.types import EligibleDataType


def not_nullable(data: pd.DataFrame, columns: Iterable[str]):
    """ Function that verify if a column does not contains null values """
    for column in columns:
        assert not data[column].isnull().values.any(), f"Column" \
            f" {column} " \
            f"contains null values"


def data_type(data: pd.DataFrame, columns: Iterable[str],
              dtype: Type):
    """ Function that verify if the type of column is correct """
    for column in columns:
        assert data[column].dtype == dtype, \
            f"Column {column} is {data[column].dtype}"


def eligible_data_type(data: pd.DataFrame):
    result = data.dtypes
    for column, type_ in result.items():
        msg = f'The type of the column {column} is {type_} ' \
              f'which is not an eligible type.'
        assert type_ in EligibleDataType.get_list(), msg


def quantile(data: pd.DataFrame, columns: Iterable[str], q, threshold):
    """ Function that verifies the quantile according to a threshold  """
    for column in columns:
        assert data[column].quantile(q) < threshold


def unique(data: pd.DataFrame, columns: Iterable[str]):
    """ Function that checks if the values of a column is unique """
    for column in columns:
        assert data[
            column].nunique() == data.__len__(), f"Column {column} is not unique"


def nb_unique_index(data: pd.DataFrame, columns: Iterable[str], minimum,
                    maximum):
    """ Function that checks if the number of unique objects is included in a given interval """

    for column in columns:
        msg = f"Column {column} est compris dans l'intervalle "
        assert (data[column].nunique() < maximum) and (
            data[column].nunique() >= minimum), msg


class DataConstraints:

    def __init__(self, data: pd.DataFrame = None,
                 connection: ConnectDatabase = None):
        self.data = data.copy()
        self.constraints = {}

    def add_constraint(self, constraint: dict):
        self.constraints = {**self.constraints, **constraint}

    def test(self):
        for name in self.constraints.keys():
            function, args = self.constraints[name]
            args["data"] = self.data
            function(**args)
