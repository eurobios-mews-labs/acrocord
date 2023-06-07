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
import pandas as pd
import numpy as np


def init_table(db: "ConnectDatabse") -> None:
    users = db.read_table("pg_catalog.pg_user")
    schemas = db.get_schema_names()
    privileges = pd.DataFrame(index=users.usename, columns=schemas)

    # set default values
    privileges["public"] = "read"
    for user in privileges.index:
        if user in privileges.columns:
            privileges.loc[user, user] = "write"
    db.write_table(privileges.reset_index(), "public.privileges")


def update(db: "ConnectDatabse", schema: str) -> None:

    from acrocord.misc import execution
    schema_save = "public"
    if schema == schema_save:
        return None
    if "privileges" not in db.get_table_names(schema_save):
        init_table(db)
    with execution.silence_stdout():
        privileges = db.read_table("public.privileges")

    if schema not in privileges.columns:
        privileges[schema] = np.nan
        with execution.silence_stdout():
            db.write_table(privileges, "public.privileges")

    privileges = privileges.set_index("usename")
    for user in privileges.index:
        read = privileges.loc[user, schema] == "read"
        write = privileges.loc[user, schema] == "write"

        if read or write:
            grant_query = f'GRANT USAGE ON SCHEMA {schema} TO '
            grant_query_all = f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO '

            db.engine.execute(grant_query + user)
            db.engine.execute(grant_query_all + user)
