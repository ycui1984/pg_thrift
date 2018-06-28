[![Build Status](https://travis-ci.org/charles-cui/pg_thrift.svg?branch=master)](https://travis-ci.org/charles-cui/pg_thrift)

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


## API
## Thrift Binary Protocol API:
```
thrift_binary_get_bool          //get bool from struct bytea
thrift_binary_get_byte          //get byte from struct bytea
thrift_binary_get_double        //get double from struct bytea
thrift_binary_get_int16         //get int16 from struct bytea
thrift_binary_get_int32         //get int32 from struct bytea
thrift_binary_get_int64         //get int64 from struct bytea
thrift_binary_get_string        //get string from struct bytea
thrift_binary_get_struct_bytea  //get struct bytea from struct bytea
thrift_binary_get_list_bytea    //get array of bytea from struct bytea
thrift_binary_get_set_bytea     //get array of bytea from struct bytea
thrift_binary_get_map_bytea     //get array of bytea from struct bytea

parse_thrift_binary_boolean     //get bool from bytea
parse_thrift_binary_string      //get string from bytea
parse_thrift_binary_bytes       //get bytes from bytea
parse_thrift_binary_int16       //get int16 from bytea
parse_thrift_binary_int32       //get int32 from bytea
parse_thrift_binary_int64       //get int64 from bytea
parse_thrift_binary_double      //get double from bytea
parse_thrift_binary_list_bytea  //get array of bytea from bytea
parse_thrift_binary_map_bytea   //get array of bytea from bytea
```
## Creating Index Based on Thrift Bytes:

```
create extension pg_thrift;

create table thrift_index (x bytea);
-- store random data in the schema
-- struct { 1: string }
insert into thrift_index
select E'\\x0b00010000000c' || convert_to(substring(md5('' || random() || random()), 0, 14), 'utf-8') || E'\\x00' from generate_series(1,10000);

create table thrift_no_index (x bytea);
insert into thrift_no_index
select E'\\x0b00010000000c' || convert_to(substring(md5('' || random() || random()), 0, 14), 'utf-8') || E'\\x00' from generate_series(1,10000);

create index thrift_string_idx on thrift_index using btree(thrift_binary_get_string(x, 1));

select thrift_binary_get_string(x, 1) from thrift_index order by thrift_binary_get_string(x, 1) limit 10;

explain select thrift_binary_get_string(x, 1) from thrift_index order by thrift_binary_get_string(x, 1) limit 10;
---------------------------------------------------------------------------------------------------
Limit  (cost=0.29..0.88 rows=10 width=22)
   ->  Index Scan using thrift_string_idx on thrift_index  (cost=0.29..595.28 rows=10000 width=22)
(2 rows)

explain select thrift_binary_get_string(x, 1) from thrift_no_index order by thrift_binary_get_string(x, 1) limit 10;
----------------------------------------------------------------------------------
Limit  (cost=405.10..405.12 rows=10 width=22)
  ->  Sort  (cost=405.10..430.10 rows=10000 width=22)
        Sort Key: (thrift_binary_get_string(x, 1))
        ->  Seq Scan on thrift_no_index  (cost=0.00..189.00 rows=10000 width=22)
(4 rows)
```
