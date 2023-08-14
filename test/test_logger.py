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

from acrocord.utils.logger import Logger


@pytest.fixture
def get_logger(get_connection) -> Logger:
    logger = Logger(connection=get_connection, table_name='log_table', schema='test')
    assert 'test.log_table' == logger.table_name
    return logger


def test_write_log_dataframe(get_logger: Logger, get_example_log_dataframe) -> None:
    get_logger.write_log(get_example_log_dataframe)


def test_write_log_series(get_logger: Logger, get_example_log_series) -> None:
    get_logger.write_log(get_example_log_series)


def test_write_log_dict(get_logger: Logger, get_example_log_dict) -> None:
    get_logger.write_log(get_example_log_dict)


def test_write_log_other_type(get_logger: Logger, get_example_log_other_type) -> None:
    with pytest.raises(ValueError) as exc_info:
        get_logger.write_log(get_example_log_other_type)

        assert str(exc_info.value) == "Input data should be type of [pd.Series, pd.DataFrame, or dict]"
