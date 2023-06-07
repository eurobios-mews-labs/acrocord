# Deploy database on premise
## Create your own database locally
### 1. Install Postgresql
> How to install postgresql on your workstation and connect to admin role `postgres`
#### On Unix platform
In the shell type `sudo apt-get install postgresql` for installation. Then change the current user to postgres' user with
`
sudo -su postgres
` 
and launch psql, this step stands as a connection to the base postgres with user postgres.

```shell script
psql
``` 
When the connection is made, you can create database with
```sql
create database mydb
```
#### On Win platform
Download installer from
https://www.enterprisedb.com/postgresql-tutorial-resources-training?cid=437 (official release)
and follow instructions.

Launch newly created shell application named "psql shell", and pass all default options for connection. 
The password is the same as the password asked during installation.

```psql
Server [localhost]:
Database [postgres]:
Port [5432]:
Username [postgres]:
```
When the connection is made, you can create database with


```sql
create database mydb;
```

### 2. Create User Role
User `postgres` is an admin role that should not be used for writing and querying data.
With the psql shell and under role `postrgres`, an admin user can be created with appropriate password.
Then you can grant all privileges to `myuser` for the data server.

```sql
CREATE USER myuser with password '*enyUyCp6!a$Z6C@rXk7hq%mrqCQty378mQJ6@#Gozn7Z^$';
ALTER USER myuser WITH SUPERUSER;
```

### 3. Test connection
Using dbeaver, create a new connection with the following specifications

1. Host : localhost
2. Port : 5432
3. Database : mydb
4. User : myuser
5. Password : '*enyUyCp6!a$Z6C@rXk7hq%mrqCQty378mQJ6@#Gozn7Z^$'

### 4. Export and restore a database

To export a backup:

```shell script
pg_dump -C -U myuser -F p -f /output/mydb.sql mydb
``` 
To restore from a backup :

```shell script
psql -U myuser mydb < /backups/mydb.sql
```
