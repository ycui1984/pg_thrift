-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_thrift" to load this file. \quit

CREATE FUNCTION thrift_binary_get_bool(bytea, int)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_byte(bytea, int)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_double(bytea, int)
    RETURNS double precision
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_int16(bytea, int)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_int32(bytea, int)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_int64(bytea, int)
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_string(bytea, int)
    RETURNS text
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_struct_bytea(bytea, int)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_list_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_set_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_binary_get_map_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_boolean(bytea)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_string(bytea)
    RETURNS text
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_bytes(bytea)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_int16(bytea)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_int32(bytea)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_int64(bytea)
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_double(bytea)
    RETURNS double precision
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_list_bytea(bytea)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_binary_map_bytea(bytea)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_bool(bytea, int)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_byte(bytea, int)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_string(bytea, int)
    RETURNS text
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_int16(bytea, int)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_int32(bytea, int)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_int64(bytea, int)
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_double(bytea, int)
    RETURNS double precision
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_list_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_struct_bytea(bytea, int)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_set_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION thrift_compact_get_map_bytea(bytea, int)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


CREATE FUNCTION parse_thrift_compact_string(bytea)
    RETURNS text
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_bytes(bytea)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_int16(bytea)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_int32(bytea)
    RETURNS int
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_int64(bytea)
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_double(bytea)
    RETURNS double precision
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_list_bytea(bytea)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION parse_thrift_compact_map_bytea(bytea)
    RETURNS bytea[]
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE thrift_binary;

CREATE FUNCTION thrift_binary_in(cstring)
    RETURNS thrift_binary
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION thrift_binary_out(thrift_binary)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE thrift_binary (
    INPUT = thrift_binary_in,
    OUTPUT = thrift_binary_out,
    LIKE = bytea
);
