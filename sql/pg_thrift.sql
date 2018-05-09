CREATE EXTENSION pg_thrift;

-- struct (id = 123, phones=["123456", "abcdef"])
SELECT thrift_binary_get_int32(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 1);

SELECT thrift_binary_get_bool(E'\\x0800010000007b0f00020b00000002000000063132333435360000000661626364656600' :: bytea, 1);

SELECT thrift_binary_get_byte(E'\\x0300010000000ce4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- hello world
SELECT thrift_binary_get_string(E'\\x0b00010000000ce4bda0e5a5bde4b896e7958c00' :: bytea, 1);

-- 1234567890.1234567890
SELECT thrift_binary_get_double(E'\\x04000141d26580b487e6b700' :: bytea, 1);
