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
# limitations under the License.
import os
import os.path

import pandas as pd

home_directory = os.path.expanduser('~')

cache_folder = f'{home_directory}/.acrocord/cache'
cache_volume_gio = 0.5


def name(table_name: str, columns: iter = (), where: str = '',
         limit: int = 0) -> str:
    return f"{table_name}_{columns}_{where}_{limit}".replace(
        "(", "_").replace(")", "_")


def folder_size_gio():
    return sum(
        os.path.getsize(f"{cache_folder}/{f}")
        for f in os.listdir(cache_folder)) / 1e9


def set_cache_folder():
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)


def write(data: pd.DataFrame, table_name, **kwargs):
    if folder_size_gio() < cache_volume_gio:
        data.to_pickle(f"{cache_folder}/{name(table_name, **kwargs)}")


def read(table_name: str, **kwarg):
    name_ = name(table_name, **kwarg)
    if name_ in os.listdir(cache_folder):
        data = pd.read_pickle(f'{cache_folder}/{name_}')
        return data
    else:
        return pd.DataFrame()


def clean_cache():
    if os.path.exists(cache_folder):
        for f in os.listdir(cache_folder):
            os.remove(f"{cache_folder}/{f}")
