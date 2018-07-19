[![Build Status](https://travis-ci.org/charles-cui/pg_thrift.svg?branch=master)](https://travis-ci.org/charles-cui/pg_thrift)

## Thrift support for PostgreSQL (pg\_thrift)
## Motivation
One of advantages of document-oriented databases like MongoDB or Couchbase over RDBMSs is an ability to change the data scheme easily, fast and often. The traditional approach in RDBMS world involves doing an expensive ALTER TABLE operation, slow upgrade of an existing data, and stuff like this. This approach is often slow and inconvenient for application developers.

To solve this issue PostgreSQL provides JSON and JSONB datatypes. Unfortunately JSONB has a disadvantage of storing all documents keys, which is a lot of redundant data.

One possibility to to reduce JSONB redundancy is to use zson extension. It compresses JSONB documents using shared dictionary of common strings that appear in all or most documents. This approach has its limitations though. Particularly, since data schema evolves, the dictionary has to be updated from time to time. Also zson can affect the build-in mechanism of PostgreSQL of compressing data using PGLZ algorithm since this mechanism uses some heuristics to recognize data that compresses well. Thus sometimes zson can reduce the overall performance.

There is another extension(pg_protobuf). Basically it provides Protobuf support for PostgreSQL. It seems to solve all the issues described above and doesn't have any disadvantages of zson extension.

The idea of this project is to create a similar extension that would provide Thrift support. Some users may prefer Thrift to Protobuf or just use it by historical reasons. This project is a bit more complicated than pg_protobuf since unlike Protobuf Thrift support various encoding protocols. This project implements two encoding
protocols (binary and compact) based on the document here (https://erikvanoosten.github.io/thrift-missing-specification/#_integer_encoding)

## Install
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
thrift_binary_get_bool          /* get bool from struct bytea */
thrift_binary_get_byte          /* get byte from struct bytea */
thrift_binary_get_double        /* get double from struct bytea */
thrift_binary_get_int16         /* get int16 from struct bytea */
thrift_binary_get_int32         /* get int32 from struct bytea */
thrift_binary_get_int64         /* get int64 from struct bytea */
thrift_binary_get_string        /* get string from struct bytea */
thrift_binary_get_struct_bytea  /* get struct bytea from struct bytea */
thrift_binary_get_list_bytea    /* get array of bytea from struct bytea */
thrift_binary_get_set_bytea     /* get array of bytea from struct bytea */
thrift_binary_get_map_bytea     /* get array of bytea from struct bytea */

parse_thrift_binary_boolean     /* get bool from bytea */
parse_thrift_binary_string      /* get string from bytea */
parse_thrift_binary_bytes       /* get bytes from bytea */
parse_thrift_binary_int16       /* get int16 from bytea */
parse_thrift_binary_int32       /* get int32 from bytea */
parse_thrift_binary_int64       /* get int64 from bytea */
parse_thrift_binary_double      /* get double from bytea */
parse_thrift_binary_list_bytea  /* get array of bytea from bytea */
parse_thrift_binary_map_bytea   /* get array of bytea from bytea */
```

## Thrift Compact Protocol API:
```
thrift_compact_get_bool         /* get bool from struct bytea */
thrift_compact_get_byte         /* get byte from struct bytea */
thrift_compact_get_double       /* get double from struct bytea */
thrift_compact_get_int16        /* get int16 from struct bytea */
thrift_compact_get_int32        /* get int32 from struct bytea */
thrift_compact_get_int64        /* get int64 from struct bytea */
thrift_compact_get_string       /* get string from struct bytea */
thrift_compact_get_struct_bytea /* get struct bytea from struct bytea */
thrift_compact_get_list_bytea   /* get array of bytea from struct bytea */
thrift_compact_get_set_bytea    /* get array of bytea from struct bytea */
thrift_compact_get_map_bytea    /* get array of bytea from struct bytea */

parse_thrift_compact_boolean    /* get bool from bytea */
parse_thrift_compact_string     /* get string from bytea */
parse_thrift_compact_bytes      /* get bytes from bytea */
parse_thrift_compact_int16      /* get int16 from bytea */
parse_thrift_compact_int32      /* get int32 from bytea */
parse_thrift_compact_int64      /* get int64 from bytea */
parse_thrift_compact_double     /* get double from bytea */
parse_thrift_compact_list_bytea /* get array of bytea from bytea */
parse_thrift_compact_map_bytea  /* get array of bytea from bytea */
```

## Thrift Binary Type
To ease the use of thrift type, custom data types are created.
User provide json format as input, thrift bytes are stored. The custom type
supports binary protocol now, but should be easy to extend to compact protocol.
```
thrift_binary_in                /* json to thrift binary bytes */
thrift_binary_out               /* thrift binary to json bytes */
```


## API Use Case1. Parse field (using compact protocol):
```
--struct(id=[1, 2, 3, 4, 5])
SELECT parse_thrift_compact_int32(UNNEST(thrift_compact_get_set_bytea(E'\\x1a58020406080a00' :: bytea, 1)));
  parse_thrift_compact_int32
 ----------------------------
                           1
                           2
                           3
                           4
                           5
 (5 rows)


-- struct(id=123, phones=["123456", "abcdef"])  //item1
-- struct(id=456, phones=["123456", "abcdef"])  //item2
-- struct(id=123, items=[item1, item2])
SELECT parse_thrift_compact_string(UNNEST(thrift_compact_get_list_bytea(UNNEST(thrift_compact_get_list_bytea(E'\\x15f601192c15f601192b0c3132333435360c61626364656600159007192b0c3132333435360c6162636465660000', 2)), 2)));
  parse_thrift_compact_string
 -----------------------------
  123456
  abcdef
  123456
  abcdef
 (4 rows)
```

## API Use Case2. Creating Index Based on Thrift Bytes (using binary protocol):
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

## API Use Case3. Using custom type:
```
postgres=# create table thrift_example(x thrift_binary);
CREATE TABLE
postgres=# insert into thrift_example values('{"type" : "int16", "value" : 60}');
INSERT 0 1
postgres=# select * from example;
              x              
-----------------------------
 {"type":"int16","value":60}
(1 row)
```
