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

-- {'a' : 2}
SELECT parse_thrift_compact_string((thrift_compact_get_map_bytea(E'\\x1b02b602610400' :: bytea, 1))[1]);

-- {'a' : 2}
SELECT parse_thrift_compact_int16((thrift_compact_get_map_bytea(E'\\x1b02b602610400' :: bytea, 1))[2]);

-- struct(id=123, phones=["123456", "abcdef"])
SELECT parse_thrift_compact_string(UNNEST(thrift_compact_get_list_bytea(E'\\x15f601192b0c3132333435360c61626364656600', 2)));

-- struct(id=123, phones=["123456", "abcdef"])
-- struct(id=456, phones=["123456", "abcdef"])
-- struct(id=123, items=[item1, item2])
SELECT parse_thrift_compact_string(UNNEST(thrift_compact_get_list_bytea(UNNEST(thrift_compact_get_list_bytea(E'\\x15f601192c15f601192b0c3132333435360c61626364656600159007192b0c3132333435360c6162636465660000', 2)), 2)));

-- struct1 (id = 123, phones=["123456", "abcdef"])
-- struct2 (id = 123, phones=["123456", "abcdef"])
-- struct (id = 123, phones=[struct1, struct2])
SELECT parse_thrift_binary_string(UNNEST(thrift_binary_get_list_bytea(UNNEST(thrift_binary_get_list_bytea(E'\\x0800010000007b0f00020c000000020800010000007b0f00020b000000020000000631323334353600000006616263646566000800010000007b0f00020b0000000200000006313233343536000000066162636465660000' :: bytea, 2)), 2)));

SELECT thrift_binary_in('{"type" : "bool", "value" : 1}');

SELECT thrift_binary_in('{"type" : "int16", "value" : 60}');

SELECT thrift_binary_in('{"type" : "int32", "value" : 123}');

SELECT thrift_binary_in('{"type" : "int64", "value" : 123456789}');

SELECT thrift_binary_in('{"type":"double", "value" :123456.789}');

SELECT thrift_binary_in('{"type": "string", "value" : "你好世界"}');

DROP EXTENSION pg_thrift;
