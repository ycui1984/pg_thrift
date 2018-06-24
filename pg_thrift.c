#include <postgres.h>
#include <port.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/json.h>
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

PG_FUNCTION_INFO_V1(thrift_compact_get_bool);
PG_FUNCTION_INFO_V1(thrift_compact_get_byte);
PG_FUNCTION_INFO_V1(thrift_compact_get_double);
PG_FUNCTION_INFO_V1(thrift_compact_get_int16);
PG_FUNCTION_INFO_V1(thrift_compact_get_int32);
PG_FUNCTION_INFO_V1(thrift_compact_get_int64);
PG_FUNCTION_INFO_V1(thrift_compact_get_string);
PG_FUNCTION_INFO_V1(thrift_compact_get_struct_bytea);
PG_FUNCTION_INFO_V1(thrift_compact_get_list_bytea);
PG_FUNCTION_INFO_V1(thrift_compact_get_set_bytea);
PG_FUNCTION_INFO_V1(thrift_compact_get_map_bytea);

PG_FUNCTION_INFO_V1(parse_thrift_binary_boolean);
PG_FUNCTION_INFO_V1(parse_thrift_binary_string);
PG_FUNCTION_INFO_V1(parse_thrift_binary_bytes);
PG_FUNCTION_INFO_V1(parse_thrift_binary_int16);
PG_FUNCTION_INFO_V1(parse_thrift_binary_int32);
PG_FUNCTION_INFO_V1(parse_thrift_binary_int64);
PG_FUNCTION_INFO_V1(parse_thrift_binary_double);
PG_FUNCTION_INFO_V1(parse_thrift_binary_list_bytea);
PG_FUNCTION_INFO_V1(parse_thrift_binary_map_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_in);
PG_FUNCTION_INFO_V1(thrift_binary_out);

PG_FUNCTION_INFO_V1(parse_thrift_compact_boolean);
PG_FUNCTION_INFO_V1(parse_thrift_compact_string);
PG_FUNCTION_INFO_V1(parse_thrift_compact_bytes);
PG_FUNCTION_INFO_V1(parse_thrift_compact_int16);
PG_FUNCTION_INFO_V1(parse_thrift_compact_int32);
PG_FUNCTION_INFO_V1(parse_thrift_compact_int64);
PG_FUNCTION_INFO_V1(parse_thrift_compact_double);
PG_FUNCTION_INFO_V1(parse_thrift_compact_list_bytea);
PG_FUNCTION_INFO_V1(parse_thrift_compact_map_bytea);

Datum thrift_binary_decode(uint8* data, Size size, int16 field_id, int8 type_id);
Datum thrift_compact_decode(uint8* data, Size size, int16 field_id, int8 type_id);
Datum parse_binary_field(uint8* start, uint8* end, int8 type_id);
Datum parse_compact_field(uint8* start, uint8* end, int8 type_id);
uint8* skip_binary_field(uint8* start, uint8* end, int8 type_id);
uint8* skip_compact_field(uint8* start, uint8* end, int8 type_id);

Datum parse_thrift_binary_boolean_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_string_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_bytes_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_int16_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_int32_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_int64_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_double_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_struct_bytea_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_list_bytea_internal(uint8* start, uint8* end);
Datum parse_thrift_binary_map_bytea_internal(uint8* start, uint8* end);

Datum parse_thrift_compact_string_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_bytes_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_int16_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_int32_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_int64_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_struct_bytea_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_list_bytea_internal(uint8* start, uint8* end);
Datum parse_thrift_compact_map_bytea_internal(uint8* start, uint8* end);

uint8* encode_json_to_byte(char* input, int32* plen);
uint8* encode_bool(char* value);
uint8* encode_int16(char* value);
uint8* encode_int32(char* value);
uint8* encode_int64(char* value);
uint8* encode_double(char* value);
uint8* encode_string(char* value);
uint8* encode_byte(char* value);
Datum bytea_to_json(int type, uint8* start, uint8* end);
char* parse_json_type(char* input);
char* parse_json_value(char* input);
uint8 char_to_int8(char c);
uint8* string_to_bytes(char* value);
char convert_int8_to_char(uint8 value, bool first_half);
char* bytes_to_string(uint8* start, int32 len);
bool is_big_endian(void);
int64 parse_int_helper(uint8* start, uint8* end, int len);
void swap_bytes(char* bytes, int len);
int64 parse_varint_helper(uint8* start, uint8* end, int64* len_description);
uint8 compact_list_type_to_struct_type(uint8 element_type);

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

// returns value from varint encoded zigzag int
int64 parse_varint_helper(uint8* start, uint8* end, int64* len_length) {
  uint8* p = start;
  int64 val = 0;
  while (p < end) {
    val = (((*p) & 0x7f) << 7*(p - start)) + val;
    if ((*p) & 0x80) p++;
    else break;
  }
  *len_length = p - start + 1;
  return (val >> 1) ^ -(val & 1);
}

// skip field is needed in list(set, map) and struct,
// but types are different, this helper does the mapping
uint8 compact_list_type_to_struct_type(uint8 element_type) {
  if (element_type == 2) {
    return PG_THRIFT_COMPACT_BOOL;
  }

  if (element_type == 3) {
    return PG_THRIFT_COMPACT_BYTE;
  }

  if (element_type == 4) {
    return PG_THRIFT_COMPACT_DOUBLE;
  }

  if (element_type == 6) {
    return PG_THRIFT_COMPACT_INT16;
  }

  if (element_type == 8) {
    return PG_THRIFT_COMPACT_INT32;
  }

  if (element_type == 10) {
    return PG_THRIFT_COMPACT_INT64;
  }

  if (element_type == 11) {
    return PG_THRIFT_COMPACT_STRING;
  }

  if (element_type == 12) {
    return PG_THRIFT_COMPACT_STRUCT;
  }

  if (element_type == 13) {
    return PG_THRIFT_COMPACT_MAP;
  }

  if (element_type == 14) {
    return PG_THRIFT_COMPACT_SET;
  }

  if (element_type == 15) {
    return PG_THRIFT_COMPACT_LIST;
  }
  elog(ERROR, "Invalid thrift compact element type");
}

Datum parse_thrift_binary_boolean(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_boolean_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_boolean_internal(uint8* start, uint8* end) {
  if (start >= end) {
    elog(ERROR, "Invalid thrift binary format for bool");
  }
  PG_RETURN_BOOL(*start);
}

Datum parse_thrift_binary_string(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_bytes_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_bytes(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_bytes_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_bytes_internal(uint8* start, uint8* end) {
  int32 len = parse_int_helper(start, end, BYTE_LEN);
  if (start + BYTE_LEN + len - 1 >= end) {
    elog(ERROR, "Invalid thrift format for bytes");
  }
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start + BYTE_LEN, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_thrift_compact_string(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_bytes_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_bytes(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_bytes_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_bytes_internal(uint8* start, uint8* end) {
  int64 len_length = 0;
  int64 len = parse_varint_helper(start, end, &len_length);
  if (start + len_length + len - 1 >= end) {
    elog(ERROR, "Invalid thrift compact format for bytes");
  }
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start + len_length, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_thrift_binary_string_internal(uint8* start, uint8* end) {
  int32 len = parse_int_helper(start, end, BYTE_LEN);
  if (start + BYTE_LEN + len - 1 >= end) {
    elog(ERROR, "Invalid thrift format for string");
  }
  char* ret = palloc(len + 1);
  memset(ret, 0, len + 1);
  memcpy(ret, start + BYTE_LEN, len);
  PG_RETURN_CSTRING(ret);
}

Datum parse_thrift_binary_double(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_double_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_double(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  // binary and compact are same for double
  return parse_thrift_binary_double_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_double_internal(uint8* start, uint8* end) {
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

Datum parse_thrift_binary_int16(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_int16_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_int16_internal(uint8* start, uint8* end) {
  PG_RETURN_INT16(parse_int_helper(start, end, INT16_LEN));
}

Datum parse_thrift_compact_int16(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_int16_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_int16_internal(uint8* start, uint8* end) {
  int64 len = 0;
  PG_RETURN_INT16(parse_varint_helper(start, end, &len));
}

Datum parse_thrift_binary_int32(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_int32_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_int32_internal(uint8* start, uint8* end) {
  PG_RETURN_INT32(parse_int_helper(start, end, INT32_LEN));
}

Datum parse_thrift_compact_int32(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_int32_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_int32_internal(uint8* start, uint8* end) {
  int64 len = 0;
  PG_RETURN_INT32(parse_varint_helper(start, end, &len));
}

Datum parse_thrift_binary_int64(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_int64_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_int64_internal(uint8* start, uint8* end) {
  PG_RETURN_INT64(parse_int_helper(start, end, INT64_LEN));
}

Datum parse_thrift_compact_int64(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_int64_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_int64_internal(uint8* start, uint8* end) {
  int64 len = 0;
  PG_RETURN_INT64(parse_varint_helper(start, end, &len));
}

Datum parse_thrift_binary_struct_bytea_internal(uint8* start, uint8* end) {
  uint8* next_start = skip_binary_field(start, end, PG_THRIFT_BINARY_STRUCT);
  int32 len = next_start - start;
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_thrift_compact_struct_bytea_internal(uint8* start, uint8* end) {
  uint8* next_start = skip_compact_field(start, end, PG_THRIFT_COMPACT_STRUCT);
  int32 len = next_start - start;
  bytea* ret = palloc(len + VARHDRSZ);
  memcpy(VARDATA(ret), start, len);
  SET_VARSIZE(ret, len + VARHDRSZ);
  PG_RETURN_POINTER(ret);
}

Datum parse_thrift_binary_list_bytea(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_list_bytea_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_list_bytea_internal(uint8* start, uint8* end) {
  if (start + PG_THRIFT_TYPE_LEN + LIST_LEN - 1 >= end) {
    elog(ERROR, "Invalid thrift binary format for list");
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
    uint8* p = skip_binary_field(curr, end, element_type);
    ret[i] = PointerGetDatum(palloc(p - curr + VARHDRSZ));
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

Datum parse_thrift_compact_list_bytea(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_list_bytea_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_list_bytea_internal(uint8* start, uint8* end) {
  if (start + PG_THRIFT_TYPE_LEN >= end) {
    elog(ERROR, "Invalid thrift compact format for list");
  }
  uint8 size_type_id = parse_int_helper(start, end, PG_THRIFT_TYPE_LEN);
  uint8 type_id = size_type_id & 0x0f;
  uint32 len = (size_type_id & 0xf0) >> 4;
  uint8* curr = start;
  if (len == 0xf) {
    int64 size_len = 0;
    len = parse_varint_helper(start + PG_THRIFT_TYPE_LEN, end, &size_len);
    curr = start + PG_THRIFT_TYPE_LEN + size_len;
  } else {
    curr = start + PG_THRIFT_TYPE_LEN;
  }
  Datum ret[THRIFT_RESULT_MAX_FIELDS];
  bool null[THRIFT_RESULT_MAX_FIELDS], typbyval;
  int16 typlen;
  char typalign;
  get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
  for (int i = 0; i < len; i++) {
    uint8* p = skip_compact_field(curr, end, compact_list_type_to_struct_type(type_id));
    ret[i] = PointerGetDatum(palloc(p - curr + VARHDRSZ));
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

Datum parse_thrift_binary_map_bytea(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_binary_map_bytea_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_binary_map_bytea_internal(uint8* start, uint8* end) {
  if (start + 2*PG_THRIFT_TYPE_LEN + INT32_LEN - 1 >= end) {
    elog(ERROR, "Invalid thrift binary format for map");
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
    uint8* p = skip_binary_field(curr, end, type_id);
    ret[i] = PointerGetDatum(palloc(p - curr + VARHDRSZ));
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

Datum parse_thrift_compact_map_bytea(PG_FUNCTION_ARGS) {
  bytea* data = PG_GETARG_BYTEA_P(0);
  return parse_thrift_compact_map_bytea_internal((uint8*)VARDATA(data), (uint8*)VARDATA(data) + VARSIZE(data));
}

Datum parse_thrift_compact_map_bytea_internal(uint8* start, uint8* end) {
  if (start >= end) {
    elog(ERROR, "Invalid thrift compact format for map");
  }
  int64 size_len = 0;
  int32 len = parse_varint_helper(start, end, &size_len);
  uint8* curr = start + size_len;
  Datum ret[THRIFT_RESULT_MAX_FIELDS];
  bool null[THRIFT_RESULT_MAX_FIELDS], typbyval;
  int16 typlen;
  char typalign;
  get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
  uint8 type_id = *curr;
  uint8 key_type_id = compact_list_type_to_struct_type((type_id & 0xf0) >> 4);
  uint8 value_type_id = compact_list_type_to_struct_type(type_id & 0xf0);
  curr = curr + PG_THRIFT_TYPE_LEN;
  for (int i = 0; i < 2 * len; i++) {
    int type_id = (i % 2 == 0? key_type_id : value_type_id);
    uint8* p = skip_compact_field(curr, end, type_id);
    ret[i] = PointerGetDatum(palloc(p - curr + VARHDRSZ));
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

Datum parse_binary_field(uint8* start, uint8* end, int8 type_id) {
  if (type_id == PG_THRIFT_BINARY_BOOL) {
    return parse_thrift_binary_boolean_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_BYTE || type_id == PG_THRIFT_BINARY_STRING) {
    return parse_thrift_binary_bytes_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_DOUBLE) {
    return parse_thrift_binary_double_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_INT16) {
    return parse_thrift_binary_int16_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_INT32) {
    return parse_thrift_binary_int32_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_INT64) {
    return parse_thrift_binary_int64_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_STRUCT) {
    return parse_thrift_binary_struct_bytea_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_LIST || type_id == PG_THRIFT_BINARY_SET) {
    return parse_thrift_binary_list_bytea_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }

  if (type_id == PG_THRIFT_BINARY_MAP) {
    return parse_thrift_binary_map_bytea_internal(start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end);
  }
  elog(ERROR, "Unsupported thrift binary type");
}

Datum parse_compact_field(uint8* start, uint8* end, int8 type_id) {
  if (type_id == PG_THRIFT_COMPACT_BYTE) {
    return parse_thrift_compact_bytes_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_INT16) {
    return parse_thrift_compact_int16_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_INT32) {
    return parse_thrift_compact_int32_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_INT64) {
    return parse_thrift_compact_int64_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_DOUBLE) {
    // double is same for binary and compact
    return parse_thrift_binary_double_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_STRING) {
    return parse_thrift_compact_bytes_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_LIST || type_id == PG_THRIFT_COMPACT_SET) {
    return parse_thrift_compact_list_bytea_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_MAP) {
    return parse_thrift_compact_map_bytea_internal(start, end);
  }

  if (type_id == PG_THRIFT_COMPACT_STRUCT) {
    return parse_thrift_compact_struct_bytea_internal(start, end);
  }

  elog(ERROR, "Unsupported thrift compact type");
}

// give start of data, end of data and type id,
// return pointer after its end
uint8* skip_binary_field(uint8* start, uint8* end, int8 field_type) {
  uint8* ret = 0;
  if (field_type == PG_THRIFT_BINARY_BOOL) {
    ret = start + BOOL_LEN;
  } else if (field_type == PG_THRIFT_BINARY_BYTE || field_type == PG_THRIFT_BINARY_STRING) {
    int32 len = parse_int_helper(start, end, INT32_LEN);
    ret = start + INT32_LEN + len;
  } else if (field_type == PG_THRIFT_BINARY_DOUBLE) {
    ret = start + DOUBLE_LEN;
  } else if (field_type == PG_THRIFT_BINARY_INT16) {
    ret = start + INT16_LEN;
  } else if (field_type == PG_THRIFT_BINARY_INT32) {
    ret = start + INT32_LEN;
  } else if (field_type == PG_THRIFT_BINARY_INT64) {
    ret = start + INT64_LEN;
  } else if (field_type == PG_THRIFT_BINARY_STRUCT) {
    ret = start;
    while (true) {
      if (*ret == 0) break;
      int8 field_type = *ret;
      ret = skip_binary_field(ret + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN, end, field_type);
    }
  } else if (field_type == PG_THRIFT_BINARY_MAP) {
    int8 key_type = *start;
    int8 value_type = *(start + PG_THRIFT_TYPE_LEN);
    int32 len = parse_int_helper(start + 2*PG_THRIFT_TYPE_LEN, end, INT32_LEN);
    ret = start + 2*PG_THRIFT_TYPE_LEN + INT32_LEN;
    for (int i = 0; i < len; i++) {
      ret = skip_binary_field(ret, end, key_type);
      ret = skip_binary_field(ret, end, value_type);
    }
  } else if (field_type == PG_THRIFT_BINARY_SET || field_type == PG_THRIFT_BINARY_LIST) {
    int32 len = parse_int_helper(start + PG_THRIFT_TYPE_LEN, end, INT32_LEN);
    int8 field_type = *start;
    ret = start + PG_THRIFT_TYPE_LEN + INT32_LEN;
    for (int i = 0; i < len; i++) {
      ret = skip_binary_field(ret, end, field_type);
    }
  }

  if (ret > end) {
    elog(ERROR, "Invalid thrift format");
  }

  return ret;
}

uint8* skip_compact_field(uint8* start, uint8* end, int8 field_type) {
  uint8* ret = 0;
  if (field_type == PG_THRIFT_COMPACT_BOOL) {
    ret = start + BOOL_LEN;
  } else if (field_type == PG_THRIFT_COMPACT_BYTE || field_type == PG_THRIFT_COMPACT_STRING) {
    int64 len_length = 0;
    int32 len = parse_varint_helper(start, end, &len_length);
    ret = start + len_length + len;
  } else if (field_type == PG_THRIFT_COMPACT_DOUBLE) {
    ret = start + DOUBLE_LEN;
  } else if (
    field_type == PG_THRIFT_COMPACT_INT16 ||
    field_type == PG_THRIFT_COMPACT_INT32 ||
    field_type == PG_THRIFT_COMPACT_INT64
  ) {
    int64 len = 0;
    parse_varint_helper(start, end, &len);
    ret = start + len;
  } else if (
    field_type == PG_THRIFT_COMPACT_SET ||
    field_type == PG_THRIFT_BINARY_LIST
  ) {
    uint8 len_type_id = parse_int_helper(start, end, PG_THRIFT_TYPE_LEN);
    uint32 len = (len_type_id & 0xf0) >> 4;
    uint8 type_id = len_type_id & 0x0f;
    if (len == 0x0f) {
      int64 len_length = 0;
      len = parse_varint_helper(start + PG_THRIFT_TYPE_LEN, end, &len_length);
      ret = start + PG_THRIFT_TYPE_LEN + len_length;
    } else {
      ret = start + PG_THRIFT_TYPE_LEN;
    }
    for (int i = 0; i < len; i++) {
      ret = skip_compact_field(ret, end, compact_list_type_to_struct_type(type_id));
    }
  } else if (field_type == PG_THRIFT_COMPACT_MAP) {
      int64 len_length = 0;
      int32 len = parse_varint_helper(start, end, &len_length);
      uint8 key_value_type_id = parse_int_helper(start + len_length, end, PG_THRIFT_TYPE_LEN);
      uint8 key_type = (key_value_type_id & 0xf0) >> 4;
      uint8 value_type = (key_value_type_id & 0x0f);
      ret = start + len_length + PG_THRIFT_TYPE_LEN;
      for (int i = 0; i < len; i++) {
        ret = skip_compact_field(ret, end, compact_list_type_to_struct_type(key_type));
        ret = skip_compact_field(ret, end, compact_list_type_to_struct_type(value_type));
      }
  } else if (field_type == PG_THRIFT_COMPACT_STRUCT) {
    ret = start;
    while (true) {
      if (*ret == 0) break;
      uint8 field_type_id = parse_int_helper(ret, end, PG_THRIFT_TYPE_LEN);
      if ((field_type_id & 0xf0) == 0) {
        ret = start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN;
      } else {
        ret = start + PG_THRIFT_TYPE_LEN;
      }
      ret = skip_compact_field(ret, end, field_type_id);
    }
  } else {
    elog(ERROR, "Invalid thrift compact field type");
  }

  if (ret > end) {
    elog(ERROR, "Invalid thrift compact format");
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
        return parse_binary_field(start, end, type_id);
      }
      break;
    } else {
      int8 target_type_id = *start;
      start = start + PG_THRIFT_TYPE_LEN + PG_THRIFT_FIELD_LEN;
      start = skip_binary_field(start, end, target_type_id);
    }
  }
  elog(ERROR, "Invalid thrift format");
}

Datum thrift_compact_decode(uint8* data, Size size, int16 field_id, int8 type_id) {
  uint8* start = data, *end = data + size;
  int16 current_field_id = 0;
  while (start < end) {
    if (start + PG_THRIFT_TYPE_LEN >= end) break;
    int8 field_type_id = parse_int_helper(start, end, PG_THRIFT_TYPE_LEN);
    int8 field_delta = (field_type_id >> 4) & 0x0f;
    int8 parsed_type_id = field_type_id & 0x0f;
    if (field_delta != 0) {
      current_field_id += field_delta;
    } else {
      current_field_id = parse_int_helper(start + PG_THRIFT_TYPE_LEN, end, FIELD_LEN);
    }
    if (current_field_id == field_id) {
      if (type_id == PG_THRIFT_COMPACT_BOOL) {
        if (parsed_type_id == 1) {
          PG_RETURN_BOOL(1);
        } else if (parsed_type_id == 2) {
          PG_RETURN_BOOL(0);
        } else {
          elog(ERROR, "Invalid parsed type id for compact bool");
        }
      }
      if (parsed_type_id == type_id) {
        return parse_compact_field(start + PG_THRIFT_TYPE_LEN, end, type_id);
      }
      break;
    } else {
      start += PG_THRIFT_TYPE_LEN;
      if (field_delta == 0) {
        start += PG_THRIFT_FIELD_LEN;
      }
      start = skip_compact_field(start, end, parsed_type_id);
    }
  }
  elog(ERROR, "Invalid thrift compact format");
}

Datum thrift_binary_get_bool(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_BOOL);
}

Datum thrift_compact_get_bool(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_BOOL);
}

Datum thrift_binary_get_byte(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_BYTE);
}

Datum thrift_compact_get_byte(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_BYTE);
}

Datum thrift_binary_get_double(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_DOUBLE);
}

Datum thrift_compact_get_double(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_DOUBLE);
}

Datum thrift_binary_get_int16(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_INT16);
}

Datum thrift_compact_get_int16(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_INT16);
}

Datum thrift_binary_get_int32(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_INT32);
}

Datum thrift_compact_get_int32(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_INT32);
}

Datum thrift_binary_get_int64(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_INT64);
}

Datum thrift_compact_get_int64(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_INT64);
}

Datum thrift_binary_get_string(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_STRING);
}

Datum thrift_compact_get_string(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_STRING);
}

Datum thrift_binary_get_struct_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_STRUCT);
}

Datum thrift_compact_get_struct_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_STRUCT);
}

Datum thrift_binary_get_list_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_LIST);
}

Datum thrift_compact_get_list_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_LIST);
}

Datum thrift_binary_get_set_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_SET);
}

Datum thrift_compact_get_set_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_SET);
}

Datum thrift_binary_get_map_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_binary_decode(data, size, field_id, PG_THRIFT_BINARY_MAP);
}

Datum thrift_compact_get_map_bytea(PG_FUNCTION_ARGS) {
  bytea* thrift_bytea = PG_GETARG_BYTEA_P(0);
  int32 field_id = PG_GETARG_INT32(1);
  uint8* data = (uint8*)VARDATA(thrift_bytea);
  Size size = VARSIZE(thrift_bytea) - VARHDRSZ;
  return thrift_compact_decode(data, size, field_id, PG_THRIFT_COMPACT_MAP);
}

uint8* encode_bool(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + BOOL_LEN);
  *ret = PG_THRIFT_BINARY_BOOL;
  bool v = atoi(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &v, BOOL_LEN);
  return ret;
}

uint8* encode_int16(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + INT16_LEN);
  *ret = PG_THRIFT_BINARY_INT16;
  int16 v = atoi(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &v, INT16_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), INT16_LEN);
  }
  return ret;
}

uint8* encode_int32(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + INT32_LEN);
  *ret = PG_THRIFT_BINARY_INT32;
  int32 v = atoi(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &v, INT32_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), INT32_LEN);
  }
  return ret;
}

uint8* encode_int64(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + INT64_LEN);
  *ret = PG_THRIFT_BINARY_INT64;
  int64 v = atol(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &v, INT64_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), INT64_LEN);
  }
  return ret;
}

uint8* encode_double(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + DOUBLE_LEN);
  *ret = PG_THRIFT_BINARY_DOUBLE;
  float8 v = atof(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &v, DOUBLE_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), DOUBLE_LEN);
  }
  return ret;
}

uint8* encode_string(char* value) {
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + BYTE_LEN + strlen(value));
  *ret = PG_THRIFT_BINARY_STRING;
  int32 len = strlen(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN, &len, INT32_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), INT32_LEN);
  }
  memcpy(ret + PG_THRIFT_TYPE_LEN + BYTE_LEN, value, strlen(value));
  return ret;
}

uint8 char_to_int8(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  elog(ERROR, "Unable to parse invalid char");
}

char convert_int8_to_char(uint8 value, bool first_half) {
  uint8 half;
  if (first_half) {
    half = (value & 0xF0) >> 4;
  } else {
    half = value & 0x0F;
  }
  if (half >= 0 && half <= 9) return '0' + half;
  return 'A' + half - 10;
}

uint8* string_to_bytes(char* value) {
  int32 bytes = strlen(value) / 2;
  uint8* ret = palloc(bytes);
  for (int i = 0; i < bytes; i++) {
    ret[i] = (char_to_int8(value[2*i]) << 4) + char_to_int8(value[2*i + 1]);
  }
  return ret;
}

char* bytes_to_string(uint8* start, int32 len) {
  char* ret = palloc(2*len + 1);
  memset(ret, 0, 2*len + 1);
  for (int i = 0; i < 2*len; i++) {
    ret[i] = convert_int8_to_char(*(start + i/2), i % 2 == 0);
  }
  return ret;
}

uint8* encode_byte(char* value) {
  int32 bytes = strlen(value) / 2;
  uint8* ret = palloc(PG_THRIFT_TYPE_LEN + BYTE_LEN + bytes);
  *ret = PG_THRIFT_BINARY_BYTE;
  memcpy(ret + PG_THRIFT_TYPE_LEN, &bytes, INT32_LEN);
  if (!is_big_endian()) {
    swap_bytes((char*)(ret + PG_THRIFT_TYPE_LEN), INT32_LEN);
  }
  uint8* data = string_to_bytes(value);
  memcpy(ret + PG_THRIFT_TYPE_LEN + BYTE_LEN, data, bytes);
  return ret;
}

//TODO: implement actual json parsing
char* parse_json_type(char* input) {
  return "byte";
}

//TODO: implement actual json parsing
char* parse_json_value(char* input) {
  return "ABCD";
}

uint8* encode_json_to_byte(char* input, int32* plen) {
  char* type = parse_json_type(input);
  char* value = parse_json_value(input);
  if (0 == strcmp("bool", type)) {
    *plen = BOOL_LEN;
    return encode_bool(value);
  }

  if (0 == strcmp("int16", type)) {
    *plen = INT16_LEN;
    return encode_int16(value);
  }

  if (0 == strcmp("int32", type)) {
    *plen = INT32_LEN;
    return encode_int32(value);
  }

  if (0 == strcmp("int64", type)) {
    *plen = INT64_LEN;
    return encode_int64(value);
  }

  if (0 == strcmp("double", type)) {
    *plen = DOUBLE_LEN;
    return encode_double(value);
  }

  if (0 == strcmp("string", type)) {
    *plen = BYTE_LEN + strlen(value);
    return encode_string(value);
  }

  if (0 == strcmp("byte", type)) {
    if (strlen(value) % 2 != 0) {
      elog(ERROR, "Invalid byte format");
    }
    *plen = BYTE_LEN + strlen(value) / 2;
    return encode_byte(value);
  }

  // if (0 == strcmp("list", type) || 0 == strcmp("set", type)) {
  //   int32 len = 0;
  //   char*[] list = parse_list(value, &len);
  //   if (len == 0) {
  //     elog(ERROR, "array length is 0");
  //   }
  //   int32 size = 2*PG_THRIFT_TYPE_LEN + INT32_LEN;
  //   uint8* ret = palloc(size);
  //   *ret = PG_THRIFT_BINARY_LIST;
  //   memcpy(ret + 2*PG_THRIFT_TYPE_LEN, &len, INT32_LEN);
  //   if (!is_big_endian()) {
  //     swap_bytes((char*)(ret + 2*PG_THRIFT_TYPE_LEN), INT32_LEN);
  //   }
  //   for (int i = 0; i < len; i++) {
  //     int32 element_len = 0;
  //     uint8* element = encode_json_to_byte(list[i], &element_len);
  //     repalloc(ret, size + element_len);
  //     memcpy(ret + size, element + PG_THRIFT_TYPE_LEN, element_len);
  //     size += element_len;
  //     if (i == 0) {
  //       ret[i + PG_THRIFT_TYPE_LEN] = element[0];
  //     }
  //   }
  //   return ret;
  // }
  //
  // if (0 == strcmp("map", type)) {
  //   int32 len = 0;
  //   char*[] list = parse_list(value, &len);
  //   if (len == 0 || len % 2 == 1) {
  //     elog(ERROR, "Invalid map element count");
  //   }
  //   int32 size = 3*PG_THRIFT_TYPE_LEN + INT32_LEN;
  //   uint8* ret = palloc(size);
  //   *ret = PG_THRIFT_BINARY_MAP;
  //   memcpy(ret + 3*PG_THRIFT_TYPE_LEN, &len, INT32_LEN);
  //   if (!is_big_endian()) {
  //     swap_bytes((char*)(ret + 3*PG_THRIFT_TYPE_LEN), INT32_LEN);
  //   }
  //   for (int i = 0; i < len; i++) {
  //     int32 element_len = 0;
  //     uint8* element = encode_json_to_byte(list[i], &element_len);
  //     repalloc(ret, size + element_len);
  //     memcpy(ret + size, element + PG_THRIFT_TYPE_LEN, element_len);
  //     size += element_len;
  //     if (i == 0 || i == 1) {
  //       ret[i + PG_THRIFT_TYPE_LEN] = element[0];
  //     }
  //   }
  //   return ret;
  // }
  //
  // if (0 == strcmp("struct", type)) {
  //   int32 len = 0, size = PG_THRIFT_TYPE_LEN;
  //   char*[] list = parse_map(value, &len);
  //   uint8* ret = palloc(size + 1);
  //   *ret = PG_THRIFT_BINARY_STRUCT;
  //   for (int i = 0; i < len; i++) {
  //     int16 field_id = -1;
  //     uint8* field_json = parse_map_element(list[i], &field_id);
  //     int32 field_len = 0;
  //     uint8* encoded_field = encode_json_to_byte(field_json, &field_len);
  //     uint8* field_buf = palloc(PG_THRIFT_TYPE_LEN + FIELD_LEN + field_len);
  //     *field_buf = *encode_field;
  //     memcpy(field_buf + PG_THRIFT_TYPE_LEN, &field_id, FIELD_LEN);
  //     if (!is_big_endian()) {
  //       swap_bytes((char*)(field_buf + PG_THRIFT_TYPE_LEN), FIELD_LEN);
  //     }
  //     memcpy(field_buf + PG_THRIFT_TYPE_LEN + FIELD_LEN, encoded_field + PG_THRIFT_TYPE_LEN, field_len);
  //     repalloc(ret, size + PG_THRIFT_TYPE_LEN + FIELD_LEN + field_len);
  //     memcpy(ret + size, field_buf, PG_THRIFT_TYPE_LEN + FIELD_LEN + field_len);
  //     size += PG_THRIFT_TYPE_LEN + FIELD_LEN + field_len;
  //   }
  //   *ret[size] = 0;
  //   return ret;
  // }
  elog(ERROR, "Unsupported thrift type");
}

/*
 * NOTE: format is first byte stores type, then comes data
 * otherwise hard to recover by just using raw bytes
 */
Datum thrift_binary_in(PG_FUNCTION_ARGS) {
  char* input = PG_GETARG_CSTRING(0);
  int32 len = 0;
  uint8* data = encode_json_to_byte(input, &len);
  bytea* ret = palloc(PG_THRIFT_TYPE_LEN + len + VARHDRSZ);
  memcpy(VARDATA(ret), data, PG_THRIFT_TYPE_LEN + len);
  SET_VARSIZE(ret, PG_THRIFT_TYPE_LEN + len + VARHDRSZ);
  PG_RETURN_BYTEA_P(ret);
}

Datum bytea_to_json(int type, uint8* start, uint8* end) {
  char* typeStr = "";
  if (type == PG_THRIFT_BINARY_BOOL) {
    typeStr = "bool";
    int64 value = DatumGetBool(parse_thrift_binary_boolean_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%ld}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_INT16) {
    typeStr = "int16";
    int16 value = DatumGetInt16(parse_thrift_binary_int16_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%hd}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_INT32) {
    typeStr = "int32";
    int32 value = DatumGetInt32(parse_thrift_binary_int32_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%d}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_INT64) {
    typeStr = "int64";
    int64 value = DatumGetInt64(parse_thrift_binary_int64_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%ld}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_DOUBLE) {
    typeStr = "double";
    float8 value = DatumGetFloat8(parse_thrift_binary_double_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%f}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_STRING) {
    typeStr = "string";
    char* value = DatumGetCString(parse_thrift_binary_string_internal(start, end));
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%s}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  if (type == PG_THRIFT_BINARY_BYTE) {
    typeStr = "byte";
    uint8* byte = (uint8*)DatumGetPointer(parse_thrift_binary_bytes_internal(start, end));
    uint8* data = (uint8*)VARDATA(byte);
    uint8 size = VARSIZE(byte);
    char* value = bytes_to_string(data, size - BYTE_LEN);
    char* retStr = palloc(MAX_JSON_STRING_SIZE);
    sprintf(retStr, "{\"type\":\"%s\",\"value\":%s}", typeStr, value);
    return CStringGetDatum(retStr);
  }

  // if (type == PG_THRIFT_BINARY_LIST || type == PG_THRIFT_BINARY_SET) {
  //   typeStr = "list";
  //   ArrayType  *val = DatumGetArrayTypeP(parse_thrift_binary_list_bytea_internal(start, end));
  // }

  elog(ERROR, "Unsupported type");
}


Datum thrift_binary_out(PG_FUNCTION_ARGS) {
  bytea* thrift_bytes = PG_GETARG_BYTEA_P(0);
  uint8* data = (uint8*)VARDATA(thrift_bytes);
  int size = VARSIZE(thrift_bytes);
  int type = *data;
  return bytea_to_json(type, data + PG_THRIFT_TYPE_LEN, data + size);
}
