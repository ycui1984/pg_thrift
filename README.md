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

## Step5. Run tests
```
make install && make installcheck
```

## Step6. Confirm plugin has been installed
```
postgres=# select * from pg_available_extensions where name = 'pg_thrift';
```

## Step7. Load pg_thrift extension
```
postgres=# create extension pg_thrift;
```

## Step8. Confirm plugin has been loaded
```
postgres=# \dx
```

## Usage Example (Creating Index Based on Thrift Bytes):

```
create extension pg_thrift;

create table thrift_index (x bytea);
-- put random data in the following schema
-- struct {
--  1: string   
--}
insert into thrift_index
select E'\\x0b00010000000c' || convert_to(substring(md5('' || random() || random()), 0, 14), 'utf-8') || E'\\x00' from generate_series(1,10000);

create table thrift_no_index (x bytea);
insert into thrift_no_index
select E'\\x0b00010000000c' || convert_to(substring(md5('' || random() || random()), 0, 14), 'utf-8') || E'\\x00' from generate_series(1,10000);

create index thrift_string_idx on thrift_index using btree(thrift_binary_get_string(x, 1));

select thrift_binary_get_string(x, 1) from thrift_index order by thrift_binary_get_string(x, 1) limit 10;

explain select thrift_binary_get_string(x, 1) from thrift_index order by thrift_binary_get_string(x, 1) limit 10;
---------------------------------------------------------------------------------------------------
-- Limit  (cost=0.29..0.88 rows=10 width=22)
--   ->  Index Scan using thrift_string_idx on thrift_index  (cost=0.29..595.28 rows=10000 width=22)
--(2 rows)

explain select thrift_binary_get_string(x, 1) from thrift_no_index order by thrift_binary_get_string(x, 1) limit 10;
----------------------------------------------------------------------------------
-- Limit  (cost=405.10..405.12 rows=10 width=22)
--   ->  Sort  (cost=405.10..430.10 rows=10000 width=22)
--         Sort Key: (thrift_binary_get_string(x, 1))
--         ->  Seq Scan on thrift_no_index  (cost=0.00..189.00 rows=10000 width=22)
--(4 rows)
```
