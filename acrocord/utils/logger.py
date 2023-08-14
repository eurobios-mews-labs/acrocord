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

from datetime import datetime
from typing import Union
from typing import Optional
from acrocord.misc.execution import execution_time
from acrocord.misc.execution import print_
from acrocord.databasepg import ConnectDatabase

import warnings
import pandas as pd

warnings.filterwarnings('ignore')

__DEFAULT_COL_NAME__ = ['date', 'value', 'message', 'other_info']
__DEFAULT_COL_TYPE__ = ['datetime', 'str', 'str', 'str']


class Logger:

    def __init__(self, connection: ConnectDatabase, table_name: str, schema: str = 'public', columns=None,
                 column_types=None):
        """
        Initialize class logger to write log messages
        Create a new table to write logs if it doesn't already exist

        Parameters
        ----------
        connection: ConnectDatabase's instance
            The connection instance, allowing to connect to a specific database
        table_name: str
            The table name in the database
        schema: str
            The schema name in the database, default is 'public'
        columns: Iterable of str
            columns names
        column_types: Iterable if str
            The columns types
        """

        if column_types is None:
            column_types = __DEFAULT_COL_TYPE__
        if columns is None:
            columns = __DEFAULT_COL_NAME__

        self.db_connection = connection
        self.table_name = f"{schema}.{table_name}"
        print_(f"{self.table_name = }", color='OKBLUE')

        if table_name in self.db_connection.get_table_names(schema):
            print_(f"\nTable {table_name} ALREADY EXIST, do not create a new one", color='OKBLUE')
            columns = self.db_connection.get_columns(self.table_name)
            column_types = self.db_connection.get_dtypes(self.table_name)
        else:
            print_(f"CREATE new table with {table_name = }", color='OKGREEN')
            self.db_connection.create_table(name=self.table_name, columns=columns, dtypes=column_types)

        self.columns = columns
        self.column_types = column_types

    @execution_time
    def write_log(self, data: Union[pd.Series, pd.DataFrame, dict]) -> Optional[ValueError]:
        """
        Main function to write log messages

        Parameters
        ----------
        data: pd.Series or pd.Dataframe or dictionary
            The log to be written to the table, specifically:
                - pd.Series: all values are written into 'value' column
                - pd.DataFrame: only data belonging to predefined columns are written
                - dict: only one log (one value for each key) is written
        Returns
        -------
            None
        """

        if isinstance(data, pd.Series):
            return self._write_log(self._series2df(data))
        elif isinstance(data, dict):
            return self._write_log(self._dict2df(data))
        elif isinstance(data, pd.DataFrame):
            return self._write_log(data.copy())
        else:
            print_(f"\nError of data type = {type(data)}", color='WARNING')
            raise ValueError("Input data should be type of [pd.Series, pd.DataFrame, or dict]")

    @staticmethod
    def _series2df(data: pd.Series) -> pd.DataFrame:
        """
        Convert pd.Series into pd.Dataframe

        Parameters
        ----------
        data: pd.Series
            The value to be converted

        Returns
        -------
            pd.DataFrame
        """
        return data.to_frame(name='value')

    @staticmethod
    def _dict2df(data: dict) -> pd.DataFrame:
        """
        Convert a dictionary into pd.Dataframe

        Parameters
        ----------
        data: dict
            The data to be converted, only one value for each key

        Returns
        -------
            pd.DataFrame
        """
        return pd.DataFrame(data, index=[0])

    def _write_log(self, log_message: pd.DataFrame) -> None:
        """

        Parameters
        ----------
        log_message: pd.DataFrame
            The log message to be written to the predefined table

        Returns
        -------
            None
        """
        input_cols = list(log_message.columns)
        cols_write = list(set(self.columns) & set(input_cols))
        cols_null = list(set(self.columns) - set(cols_write))
        print_(f"\n{cols_write = }\n{cols_null = }", color='BOLD')

        df_write = pd.DataFrame()
        df_write[cols_write] = log_message[cols_write]
        df_write[cols_null] = ''

        if 'date' in cols_write:
            df_write['date'] = pd.to_datetime(df_write['date']).strftime("%Y-%m-%d %H:%M:%S")
        else:
            df_write['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df_write.reset_index(drop=True, inplace=True)
        self.db_connection.insert(data=df_write, table_name=self.table_name)

    def print_info(self):
        print_(f"\ntable_name: {self.table_name}"
               f"\ncolumns: {self.columns}"
               f"\ncolumns types: \n{self.column_types}", color='Magenta')
