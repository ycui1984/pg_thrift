# pg\_thrift

Thrift support for PostgreSQL.

## Step1. Install PostgreSQL
```
brew install postgresql
```

## Step2. Start PostgreSQL server
```
brew info postgres
postgres -D /usr/local/var/postgres
```

## Step3. Connect with the default DB
```
psql postgres
```

## Step4. Build plugin
```
make && make install
```

## Step5. Confirm plugin has been installed
```
postgres=# select * from pg_available_extensions where name = 'pg_thrift';
```

## Step6. Load pg_thrift extension
```
postgres=# create extension pg_thrift;
```

## Step7. Confirm plugin has been loaded
```
postgres=# \dx
```
test
