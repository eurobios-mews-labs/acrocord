# Spatial data in postgresql and pypgsql

> PostGIS is a spatial database extender for PostgreSQL object-relational database.
> It adds support for geographic objects allowing location queries to be run in SQL.

### Requirements 
 **On the server hosting the database**
Install postgis


```shell
sudo apt update
sudo apt install postgis postgresql-X-postgis-X
```
for instance `postgresql-12-postgis-3` for ubuntu 18

**On the python environment** install the plugin to connect and geopandas

```shell
pip install geoalchemy2
pip install geopandas
```

### Activate postgis as an extension

Enable postgis on database, using psql. 

```shell
psql DATABASE_NAME
```

```sql
CREATE EXTENSION postgis;
```


### Usage in python

`pypgsql` works with `GeoPandas` to write spatial dataframe. The API is identical either a GeoPandas DataFrame 
or a classic DataFrame is provided. The user has only to create Geopandas' DataFrame in the following way

```python
import pandas as pd
import geopandas
dataframe = pd.DataFrame({'a': [155, 20, 3],
                          'b': [11, 299, 45],
                          'c': [73, 4, 39],
                          'd': [783, 488, 739],
                          "lat": [45, 45, 45],
                          "lon": [0, 1, 2]
                          })
gdf = geopandas.GeoDataFrame(
    dataframe, geometry=geopandas.points_from_xy(dataframe.lon, dataframe.lat))
db.write_table(gdf, "SCHEMA.NAME")
```

**Note :** The writing of geopandas dataframe is note optimised the same way as pandas dataframe. 
For the same amount of data geopandas dataframe is excepted to take more time to be inserted in the database
