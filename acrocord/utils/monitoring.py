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
def print_date(connection, schema: str, table: str):
    from acrocord.misc import execution
    with execution.silence_stdout():
        data = connection.read_table("public.informations_table").query(
            f"nom=='{table}'").query(f"schema=='{schema}'")
    if len(data) > 0:
        try:
            msg = "[info] creation date :"
            msg += "-" * (40 - len(msg)) + "  "
            msg += data["date_creation"].iloc[0][:-10]
            execution.print_(msg, "Grey")
            msg = "[info] author name :"
            msg += "-" * (40 - len(msg)) + "  "
            msg += data["auteur_creation"].iloc[0]
            execution.print_(msg, "Grey")
        except TypeError as error:
            execution.print_(error, color="Red")
