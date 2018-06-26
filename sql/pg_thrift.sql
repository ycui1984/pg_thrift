CREATE EXTENSION pg_thrift;

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_binary_get_int32(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 1);

SELECT thrift_binary_get_bool(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 1);

SELECT thrift_binary_get_byte(E'\\x0300010000000ce4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- hello world
SELECT thrift_binary_get_string(E'\\x0b00010000000ce4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- 1234567890.1234567890
SELECT thrift_binary_get_double(E'\\x04000141d26580b487e6b700' :: bytea, 1);

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_binary_get_list_bytea(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 2);

SELECT parse_thrift_binary_string(UNNEST(thrift_binary_get_list_bytea(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 2)));

-- struct (id = true)
SELECT thrift_compact_get_bool(E'\\x1100' :: bytea, 1);

-- struct (id = false)
SELECT thrift_compact_get_bool(E'\\x1200' :: bytea, 1);

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_compact_get_int16(E'\\x14f6011928063132333435360661626364656600' :: bytea, 1);

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_compact_get_int32(E'\\x15f6011928063132333435360661626364656600' :: bytea, 1);

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_compact_get_int64(E'\\x16f6011928063132333435360661626364656600' :: bytea, 1);

-- 你好世界
SELECT thrift_compact_get_byte(E'\\x1318e4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- 你好世界
SELECT thrift_compact_get_string(E'\\x1818e4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- 1234567890.1234567890
SELECT thrift_compact_get_double(E'\\x1741d26580b487e6b700' :: bytea, 1);

-- [1, 2, 3, 4, 5]
SELECT parse_thrift_compact_int16(UNNEST(thrift_compact_get_list_bytea(E'\\x1956020406080a00' :: bytea, 1)));

-- [1, 2, 3, 4, 5]
SELECT parse_thrift_compact_int32(UNNEST(thrift_compact_get_set_bytea(E'\\x1a58020406080a00' :: bytea, 1)));

DROP EXTENSION pg_thrift;
