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
"""
This module aims  to specify data types that are consistent with
the usage of :object:`pypgsql`
"""
import warnings
from typing import List

import numpy as np
import pandas as pd


class EligibleDataType:

    # nullable integers
    INTEGER_64 = pd.Int64Dtype()  # [-9_223_372_036_854_775_808   to      9_223_372_036_854_775_807]
    INTEGER_32 = pd.Int32Dtype()  # [-2_147_483_648               to      2_147_483_647]
    INTEGER_16 = pd.Int16Dtype()  # [-32_768                      to      32_767]
    INTEGER_8 = pd.Int8Dtype()  # [-128                         to      127]
    INTEGER = INTEGER_64

    # float
    FLOAT_16 = np.float16
    FLOAT_32 = np.float32
    FLOAT_64 = np.float64
    FLOAT = FLOAT_64

    # nullable boolean
    BOOLEAN = pd.BooleanDtype()

    STRING = pd.StringDtype()

    # time
    DATE_TIME = np.dtype('datetime64[ns]')

    @classmethod
    def get_list(cls) -> List:
        """

        Returns
        -------
            list of eligible data types
        """
        return [
            cls.BOOLEAN,
            cls.INTEGER,
            cls.INTEGER_8,
            cls.INTEGER_16,
            cls.INTEGER_32,
            cls.INTEGER_64,
            cls.FLOAT_16,
            cls.FLOAT_32,
            cls.FLOAT_64,
            cls.FLOAT,
            cls.BOOLEAN,
            cls.STRING,
            cls.DATE_TIME
        ]


def warning_type(dataframe: pd.DataFrame, verbose: int):
    for column, column_type in dataframe.dtypes.items():
        if verbose == 2:
            if str(column_type) not in EligibleDataType.get_list():
                return warnings.warn(
                    "The types of this dataframe are not eligible types")
        if verbose > 2:
            if str(column_type) not in EligibleDataType.get_list():
                msg = f"The type of the column {column} " \
                      f"is {column_type} which is not an eligible type."
                warnings.warn(msg)
