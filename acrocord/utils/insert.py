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
import io
from typing import Iterator, Optional

import pandas as pd

from acrocord.misc import execution


@execution.execution_time
def insert(connection: "ConnectDatabase", data: pd.DataFrame, table_name: str,
           chunksize: int = 1000):
    storage = io.StringIO()
    data.to_csv(storage, sep='\t', header=False,
                index=False, encoding="utf8", chunksize=chunksize)
    storage.seek(0)
    sii = StringIteratorIO(storage)
    connection = connection.engine.connect()
    columns = tuple(f'{c}' for c in data.columns)

    with connection.connection.cursor() as cursor:
        cursor.execute(f'SET search_path TO {table_name.split(".")[0]}')
        cursor.copy_from(sii,
                         table_name.split(".")[1], null="",
                         columns=columns, sep="\t")
        cursor.connection.commit()
        cursor.close()


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter_: Iterator[str]):
        self._iter = iter_
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n_line: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n_line]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                line_str = self._read1()
                if not line_str:
                    break
                line.append(line_str)
        else:
            while n > 0:
                line_str = self._read1(n)
                if not line_str:
                    break
                n -= len(line_str)
                line.append(line_str)
        return ''.join(line)
