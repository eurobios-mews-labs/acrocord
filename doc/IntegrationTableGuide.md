### How to integrate a new table in the format of TableFactory: A Simple Guide

### TableFactory: a short introduction

1. TableFactory is an 'abstract' class for table creation and interaction with the database using inheritance.
   - The word 'abstract' signifies that we CANNOT create an instance from this class DIRECTLY.
   - It is just a STRUCTURE to follow when we design a Table.
2. Instead, we NEED to CREATE ANOTHER CLASS that inherits the class 'TableFactory', meaning: 
   - Allows us to define a class that inherits all the methods and properties from 'TableFactory'.
   - Precise functions inside that depend on the target's user.
3. You MUST SPECIFY the 'abstract' functions in the inherited class:
   - Because these functions have defined with '@abstractmethod' in the class TableFactory.
   - They are demonstrated in the 'Example' section.
4. Setting Table DEPENDENCIES if necessary
### Example
1. Create a CLASS BuildingTableFactory that inherits from the class TableFactory, e.g.

```python
from acrocord.utils.types import EligibleDataType

INTEGER = EligibleDataType.INTEGER  # similaire pour STRING, FLOAT, DATE_TIME, and BOOLEAN


# Build a real class which inherits the structure of TableFactory
class BuildingTableFactory(TableFactory):
```
2. SPECIFY these following functions:
   - table_name: name of the table you will create
   - schema_name: name of the schema you will store the created table
   - data_definition: format of each column in your table
   - _create_table: set the table
   - id_key: define the primary key of the table

```python
# name of the table
@classmethod
def table_name(cls) -> str:
    return 'buildings'

# schema name to store the table, e.g. 'test' database
@classmethod
def schema_name(cls) -> str:
    return 'test'

# data definition for columns in the table
@classmethod
def data_definition(cls) -> Dict[str, Tuple[ExtensionDtype, str]]:
    return {
        'building_id': (INTEGER, 'Identification number'),
        'architect': (STRING, 'Architect name'),
        'height': (FLOAT, 'Height of the building (m)'),
        'construction_date': (DATE_TIME, 'Construction Date of the building'),
        'is_constructed': (BOOLEAN, 'Does the building already construct')
     }

# create the table
def _create_table(self) -> None:
    df = pd.DataFrame(data={'building_id': [11, 20, 14, 34, 61],
                            'architect': ["Durand", "Blanc", "Blanc", "Dubois", "Martin"],
                            'height': [14.4, 24.4, 35.3, 12.3, 14.4],
                            'construction_date': ['10/03/1957', '30/11/2087', '01/02/2070', '04/01/1989',
                                                    '28/10/2003']
                                   })
    df['construction_date'] = pd.to_datetime(df.construction_date, format='%d/%m/%Y')
    from datetime import datetime
    df['is_constructed'] = df.construction_date < datetime.now()
    
    self._set_table(df)  # Required line to store the result

# set primary key of the table
def id_key(cls) -> str:
    return 'building_id'
```
3. Setting table dependencies
   - get_foreign_keys: create relation between the created table with other existing tables in the database, e.g.
```python
# Table 'BuildingTableFactory' connects with table 'ArchitectTableFactory' by 'architect' column
# (equivalent to 'last_name' column in 'ArchitectTableFactory')
@classmethod
def get_foreign_keys(cls):
    return {'architect': (ArchitectTableFactory, "last_name")}
```
### A short DEMO
Suppose that you have been specified all abstract classes and set table dependencies.
Now you want to see how it look, let get started:
```python
# object 'buildings' instantiated from the class BuildingFactory
buildings: BuildingTableFactory = BuildingTableFactory()
# create the table and save to the database
buildings.write_table()
# read the table in the saved database
buildings.read_table()
# add foreign keys
buildings.add_foreign_keys()
# write the table into Excel file
# need to specify the 'path' to save the file
# if not, it will be stored in a temporary directory folder
buildings.write_to_excel() # 'path' is not specified in this command, it will saved to ./tmp
```
