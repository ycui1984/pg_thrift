#include <postgres.h>
#include <port.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(thrift_decode);
Datum thrift_decode(PG_FUNCTION_ARGS) {
  char* result_buff = "hello world";
  PG_RETURN_CSTRING((Datum)result_buff);
}
