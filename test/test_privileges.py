# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
from acrocord.utils import privileges


def test_init_privileges(get_connection):
    privileges.init_table(get_connection)
    assert "privileges" in get_connection.get_table_names("public")


def test_update_privileges(get_connection):
    get_connection.drop_table("public.privileges")
    privileges.update(get_connection, "test")
