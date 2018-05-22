#include <postgres.h>
#include <port.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include "pg_thrift.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(thrift_binary_get_bool);
PG_FUNCTION_INFO_V1(thrift_binary_get_byte);
PG_FUNCTION_INFO_V1(thrift_binary_get_double);
PG_FUNCTION_INFO_V1(thrift_binary_get_int16);
PG_FUNCTION_INFO_V1(thrift_binary_get_int32);
PG_FUNCTION_INFO_V1(thrift_binary_get_int64);
PG_FUNCTION_INFO_V1(thrift_binary_get_string);
PG_FUNCTION_INFO_V1(thrift_binary_get_struct_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_list_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_set_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_map_bytea);

Datum thrift_binary_decode(uint8* data, Size size, int16 field_id, int8 type_id);
Datum parse_field(uint8* start, uint8* end, int8 type_id);
uint8* skip_field(uint8* start, uint8* end, int8 type_id);
bool is_big_endian(void);
int64 parse_int_helper(uint8* start, uint8* end, int len);
void swap_bytes(char* bytes, int len);
Datum parse_boolean(uint8* start, uint8* end);
Datum parse_bytes(uint8* start, uint8* end);
Datum parse_int16(uint8* start, uint8* end);
Datum parse_int32(uint8* start, uint8* end);
Datum parse_int64(uint8* start, uint8* end);
Datum parse_double(uint8* start, uint8* end);
Datum parse_struct_bytea(uint8* start, uint8* end);
Datum parse_list_bytea(uint8* start, uint8* end);
Datum parse_map_bytea(uint8* start, uint8* end);

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

int64 parse_int_helper(uint8* start, uint8* end, int len) {
  if (start + len >= end) {
    elog(ERROR, "Invalid thrift format for int");
  }
  int64 val = 0;
  for (int i = 0; i < len; i++) {
    val = (val << 8) + *(start + i);
  }
  return val;
}

Datum parse_boolean(uint8* start, uint8* end) {
  if (start >= end) {
    elog(ERROR, "Invalid thrift format for bool");
  }
  PG_RETURN_BOOL(*start);
}

Datum parse_bytes(uint8* start, uint8* end) {
  int32 len = parse_int_helper(start, end, BYTE_LEN);
  if (start + BYTE_LEN + len - 1 >= end) {
    elog(ERROR, "Invalid thrift format for bytes or string");
  }
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start + BYTE_LEN, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_double(uint8* start, uint8* end) {
  if (start + DOUBLE_LEN - 1 >= end) {
    elog(ERROR, "Invalid thrift format for double");
  }
  float8 ret;
  memcpy(&ret, start, DOUBLE_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)&ret, DOUBLE_LEN);
  }
  PG_RETURN_FLOAT8(ret);
}

Datum parse_int16(uint8* start, uint8* end) {
  PG_RETURN_INT16(parse_int_helper(start, end, INT16_LEN));
}

Datum parse_int32(uint8* start, uint8* end) {
  PG_RETURN_INT32(parse_int_helper(start, end, INT32_LEN));
}

Datum parse_int64(uint8* start, uint8* end) {
  PG_RETURN_INT64(parse_int_helper(start, end, INT64_LEN));
}

Datum parse_struct_bytea(uint8* start, uint8* end) {
  uint8* next_start = skip_field(start, end, PG_THRIFT_TYPE_STRUCT);
  int32 len = next_start - start;
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_list_bytea(uint8* start, uint8* end) {
  if (start + PG_THRIFT_TYPE_LEN + LIST_LEN - 1 >= end) {
    elog(ERROR, "Invalid thrift format for list");
  }
  int8 element_type = *start;
  int32 len = parse_int_helper(start + PG_THRIFT_TYPE_LEN, end, LIST_LEN);
  uint8* curr = start + PG_THRIFT_TYPE_LEN + LIST_LEN;

  Datum ret[THRIFT_RESULT_MAX_FIELDS];
  bool null[THRIFT_RESULT_MAX_FIELDS], typbyval;
  int16 typlen;
  char typalign;
  get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
  for (int i = 0; i < len; i++) {
    uint8* p = skip_field(curr, end, element_type);
    ret[i] = palloc(p - curr + VARHDRSZ);
    null[i] = false;
    memcpy(VARDATA(ret[i]), curr, p - curr);
    SET_VARSIZE(ret[i], p - curr + VARHDRSZ);
    curr = p;
  }
  int dims[MAXDIM], lbs[MAXDIM], ndims = 1;
  dims[0] = len;
  lbs[0] = 1;
  PG_RETURN_POINTER(
    construct_md_array(ret, null, ndims, dims, lbs, BYTEAOID, typlen, typbyval, typalign)
  );
}

Datum parse_map_bytea(uint8* start, uint8* end) {
  if (start + 2*PG_THRIFT_TYPE_LEN + INT32_LEN - 1 >= end) {
    elog(ERROR, "Invalid thrift format for map");
  }
  int32 len = parse_int_helper(start + 2*PG_THRIFT_TYPE_LEN, end, INT32_LEN);
  uint8* curr = start + 2*PG_THRIFT_TYPE_LEN + INT32_LEN;
  Datum ret[THRIFT_RESULT_MAX_FIELDS];
  bool null[THRIFT_RESULT_MAX_FIELDS], typbyval;
  int16 typlen;
  char typalign;
  get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
  for (int i = 0; i < 2 * len; i++) {
    int type_id = (i % 2 == 0? *start : *(start + 1));
    uint8* p = skip_field(curr, end, type_id);
    ret[i] = palloc(p - curr + VARHDRSZ);
    null[i] = false;
    memcpy(VARDATA(ret[i]), curr, p - curr);
    SET_VARSIZE(ret[i], p - curr + VARHDRSZ);
    curr = p;
  }
  int dims[MAXDIM], lbs[MAXDIM], ndims = 1;
  dims[0] = 2*len;
  lbs[0] = 1;
  PG_RETURN_POINTER(
    construct_md_array(ret, null, ndims, dims, lbs, BYTEAOID, typlen, typbyval, typalign)
  );
}

Datum parse_field(uint8* start, uint8* end, int8 type_id) {
  if (type_id == PG_THRIFT_TYPE_BOOL) {
    return parse_boolean(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_BYTE || type_id == PG_THRIFT_TYPE_STRING) {
    return parse_bytes(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_DOUBLE) {
    return parse_double(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_INT16) {
    return parse_int16(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_INT32) {
    return parse_int32(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_INT64) {
    return parse_int64(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_STRUCT) {
    return parse_struct_bytea(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_LIST || type_id == PG_THRIFT_TYPE_SET) {
    return parse_list_bytea(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_TYPE_MAP) {
    return parse_map_bytea(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }
  elog(ERROR, "Unsupported thrift type");
}

// give start of data, end of data and type id,
// return pointer after its end
uint8* skip_field(uint8* start, uint8* end, int8 field_type) {
  uint8* ret = 0;
  if (field_type == PG_THRIFT_TYPE_BOOL) {
    ret = start + BOOL_LEN;
  } else if (field_type == PG_THRIFT_TYPE_BYTE || field_type == PG_THRIFT_TYPE_STRING) {
    int32 len = parse_int_helper(start, end, INT32_LEN);
    ret = start + INT32_LEN + len;
  } else if (field_type == PG_THRIFT_TYPE_DOUBLE) {
    ret = start + DOUBLE_LEN;
  } else if (field_type == PG_THRIFT_TYPE_INT16) {
    ret = start + INT16_LEN;
  } else if (field_type == PG_THRIFT_TYPE_INT32) {
    ret = start + INT32_LEN;
  } else if (field_type == PG_THRIFT_TYPE_INT64) {
    ret = start + INT64_LEN;
  } else if (field_type == PG_THRIFT_TYPE_STRUCT) {
    ret = start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN;
    while (true) {
      if (*ret == 0) break;
      int8 field_type = *ret;
      ret = skip_field(ret, end, field_type);
    }
  } else if (field_type == PG_THRIFT_TYPE_MAP) {
    int8 key_type = *start;
    int8 value_type = *(start + PG_THRIFT_TYPE_LEN);
    int32 len = parse_int_helper(start + 2*PG_THRIFT_TYPE_LEN, end, INT32_LEN);
    ret = start + 2*PG_THRIFT_TYPE_LEN + INT32_LEN;
    for (int i = 0; i < len; i++) {
      ret = skip_field(ret, end, key_type);
      ret = skip_field(ret, end, value_type);
    }
  } else if (field_type == PG_THRIFT_TYPE_SET || field_type == PG_THRIFT_TYPE_LIST) {
    int32 len = parse_int_helper(start + PG_THRIFT_TYPE_LEN, end, INT32_LEN);
    int8 field_type = *start;
    ret = start + PG_THRIFT_TYPE_LEN + INT32_LEN;
    for (int i = 0; i < len; i++) {
      ret = skip_field(ret, end, field_type);
    }
  }

  if (ret > end) {
    elog(ERROR, "Invalid thrift format");
  }

  return ret;
}

Datum thrift_binary_decode(uint8* data, Size size, int16 field_id, int8 type_id) {
  uint8* start = data, *end = data + size;
  while (start < end) {
    if (start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN - 1 >= end) break;
    int16 parsed_field_id = parse_int_helper(start + PG_THRIFT_TYPE_LEN, end, FIELD_LEN);
    if (parsed_field_id == field_id) {
      if (*start == type_id) {
        return parse_field(start, end, type_id);
      }
      break;
    } else {
      int8 type_id = *start;
      start = start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN;
      start = skip_field(start, end, type_id);
    }
  }
  elog(ERROR, "Invalid thrift format");
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

Datum thrift_binary_get_struct_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 12);
}

Datum thrift_binary_get_list_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 15);
}

Datum thrift_binary_get_set_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 14);
}

Datum thrift_binary_get_map_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, 13);
}
