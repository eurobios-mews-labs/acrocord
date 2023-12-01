
<img src="./.static/acrocord_logo.png" width="350"/>

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)
[![pytest](https://github.com/eurobios-scb/acrocord/actions/workflows/pytest.yml/badge.svg?event=push)](https://docs.pytest.org)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/eurobios-mews-labs/acrocord/graphs/commit-activity)
[![PyPI version](https://badge.fury.io/py/acrocord.svg)](https://badge.fury.io/py/acrocord)

A python API for managing postgresql database
## Install and setup
Install third party packages:
```shell
sudo apt install python3-dev libpq-dev unixodbc-dev
```

To install the package

```shell
python3 -m pip install acrocord
```

*use python in the proper environment e.g. in conda powershell*


#### Setup database configuration and connection

**SSL connection:** if SSL connection is required, put the certificates given by administrator in `/home/$USER/.postgresql` create the folder if it does not already exist


Default connection configuration can be saved in `connections.cfg` in the folder `/home/$USER/.postgresql/` for linux 
user or typically `C:\Users\$USER\.postgresql` for windows user.

Example of `connections.cfg`:

```ini
[connection-name]
user=USERNAME
dbname=DATABASENAME
port=PORT 
host=HOST
ssh=False
password=PASSWORD
```

> [!TIP]
> - the `host` field does not recognize ssh alias, use ip address
> - the `port` field is typically 5432 or 5433
> - the name of the database is `dbname`

Then in python the connection can directly be instantiate using the keyword `connection-name`

```python
from acrocord import ConnectDatabase

db = ConnectDatabase()
db.connect(connection="connection-name")
```
Alternatively, you can use the following syntax

```python
from acrocord import ConnectDatabase

db = ConnectDatabase()
connection = dict(
    user="USERNAME",
    print_sql_cmd=True,
    dbname="DATABASENAME",
    port="PORT",
    host="HOST",
    ssh=False
)
db.connect(print_sql_cmd=True, connection=connection)
```

#### Simple usage

```python
import pandas as pd
# create schema (i.e. an independent database: requires privileges)
# write table in schema
# read table as pandas dataframe
db.create_schema("SCHEMA")
db.write_table(pd.DataFrame(1, index=[1, 2, 3], columns=[1, 2, 3]), "SCHEMA.NAME")
db.read_table("SCHEMA.NAME")
```

> [!CAUTION]
> * If the password is trivial (for local connection), add password field to the dictionary `connection`
> * Password field can be added in `connections.cfg` file
> * If no password is provided python will open an log in window
> * No password is needed with ssl connection

#### Other topics

* **[Deploy database](doc/database_deployment.md)**: install postresgql server and create database on premise 
* **[Manage spatial data using postgis](doc/postgis.md)**: how to install postgis and manipulate spatial data

#### Author
- Eurobios Mews labs

<img src="./.static/logo_escb.png" width="100"/>
