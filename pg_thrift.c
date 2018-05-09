#include <postgres.h>
#include <port.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(thrift_binary_get_bool);
PG_FUNCTION_INFO_V1(thrift_binary_get_byte);
PG_FUNCTION_INFO_V1(thrift_binary_get_double);
PG_FUNCTION_INFO_V1(thrift_binary_get_int16);
PG_FUNCTION_INFO_V1(thrift_binary_get_int32);
PG_FUNCTION_INFO_V1(thrift_binary_get_int64);
PG_FUNCTION_INFO_V1(thrift_binary_get_string);
static const char* invalid_thrift = "Invalid thrift struct";

Datum thrift_binary_decode(uint8* data, Size size, int32 field_id, int32 type_id);
Datum parse_field(uint8* start, uint8* end, int32 type_id);
uint8* skip_field(uint8* start, uint8* end);
bool is_big_endian();

bool is_big_endian() {
  uint32 i = 1;
  char *c = (char*)&i;
  return !(*c);
}

void swap_bytes(char* bytes, int len) {
  for (int i = 0; i < len/2; i++) {
    char tmp = bytes[i];
    bytes[i] = bytes[len - 1 - i];
    bytes[len - 1 - i] = tmp;
  }
}

Datum parse_field(uint8* start, uint8* end, int32 type_id) {
  if (type_id == 2) {
    if (start + 3 >= end) {
      elog(ERROR, invalid_thrift);
    }
    bool ret = *(start + 3);
    PG_RETURN_BOOL(ret);
  } else if (type_id == 3 || type_id == 11) {
    // byte or string
    if (start + 6 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int32 len = *(start + 3);
    for (int i = 1; i < 4; i++) {
      len = (len << 8) + *(start + i + 3);
    }
    if (start + 6 + len >= end) {
      elog(ERROR, invalid_thrift);
    }
    bytea* ret = palloc(len + VARHDRSZ);
    memcpy(VARDATA(ret), start + 7, len);
    SET_VARSIZE(ret, len + VARHDRSZ);
    PG_RETURN_POINTER(ret);
  } else if (type_id == 4) {
    if (start + 10 >= end) {
      elog(ERROR, invalid_thrift);
    }
    float8 ret;
    memcpy(&ret, start + 3, sizeof(float8));
    if (!is_big_endian()) {
      swap_bytes(&ret, sizeof(float8));
    }
    PG_RETURN_FLOAT8(ret);
  } else if (type_id == 6) {
    if (start + 4 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int16 val = *(start + 3);
    val = (val << 8) + *(start + 4);
    PG_RETURN_INT16(val);
  } else if (type_id == 8) {
    if (start + 6 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int32 val = *(start + 3);
    for (int i = 1; i < 4; i++) {
      val = (val << 8) + *(start + i + 3);
    }
    PG_RETURN_INT32(val);
  } else if (type_id == 10) {
    if (start + 10 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int64 val = *(start + 3);
    for (int i = 1; i < 8; i ++) {
      val = (val << 8) + *(start + i + 3);
    }
    PG_RETURN_INT64(val);
  }
}

uint8* skip_field(uint8* start, uint8* end) {
  return end;
}

Datum thrift_binary_decode(uint8* data, Size size, int32 field_id, int32 type_id) {
  uint8* start = data, *end = data + size;
  while (start < end) {
    if (*start == type_id) {
      if (start + 2 >= end) {
        elog(ERROR, invalid_thrift);
      }
      int32 parsed_id = (*(start + 1) << 8) + *(start + 2);
      if (parsed_id == field_id) {
        return parse_field(start, end, type_id);
      }
    }
    start = skip_field(start, end);
  }
  elog(ERROR, invalid_thrift);
}

Datum thrift_binary_get_bool(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 2);
}

Datum thrift_binary_get_byte(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 3);
}

Datum thrift_binary_get_double(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 4);
}

Datum thrift_binary_get_int16(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 6);
}

Datum thrift_binary_get_int32(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 8);
}

Datum thrift_binary_get_int64(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 10);
}

Datum thrift_binary_get_string(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 11);
}

//TODO: deal with map, set, list and struct
// char* field_types[11] = {"bool", "byte", "double", "int16",
// "int32", "int64", "string", "struct", "map", "set", "list"};
// int32 type_id[11] = {2, 3, 4, 6, 8, 10, 11, 12, 13, 14, 15};
