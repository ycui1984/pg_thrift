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
PG_FUNCTION_INFO_V1(thrift_binary_get_struct_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_list_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_set_bytea);
PG_FUNCTION_INFO_V1(thrift_binary_get_map_bytea);

static const char* invalid_thrift = "Invalid thrift data format";
static const char* invalid_field_type = "Invalid thrift field type";

Datum thrift_binary_decode(uint8* data, Size size, int16 field_id, int8 type_id);
Datum parse_field(uint8* start, uint8* end, int8 type_id);
uint8* skip_field(uint8* start, uint8* end, int8 type_id);
bool is_big_endian(void);
int64 parse_int(uint8* start, uint8* end, int len);
void swap_bytes(char* bytes, int len);

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

int64 parse_int(uint8* start, uint8* end, int len) {
  if (start + len >= end) {
    elog(ERROR, invalid_thrift);
  }
  int64 val = 0;
  for (int i = 0; i < len; i++) {
    val = (val << 8) + *(start + i);
  }
  return val;
}

Datum parse_field(uint8* start, uint8* end, int8 type_id) {
  // bool
  if (type_id == 2) {
    PG_RETURN_BOOL(*(start + 3));
  }
  // byte or string
  if (type_id == 3 || type_id == 11) {
    int32 len = parse_int(start + 3, end, 4);
    if (start + 6 + len >= end) {
      elog(ERROR, invalid_thrift);
    }
    bytea* ret = palloc(len + VARHDRSZ);
    memcpy(VARDATA(ret), start + 7, len);
    SET_VARSIZE(ret, len + VARHDRSZ);
    PG_RETURN_POINTER(ret);
  }
  // float8
  if (type_id == 4) {
    if (start + 10 >= end) {
      elog(ERROR, invalid_thrift);
    }
    float8 ret;
    memcpy(&ret, start + 3, sizeof(float8));
    if (!is_big_endian()) {
      swap_bytes((char*)&ret, sizeof(float8));
    }
    PG_RETURN_FLOAT8(ret);
  }
  // int16
  if (type_id == 6) {
    PG_RETURN_INT16(parse_int(start + 3, end, 2));
  }
  // int32
  if (type_id == 8) {
    PG_RETURN_INT32(parse_int(start + 3, end, 4));
  }
  // int64
  if (type_id == 10) {
    PG_RETURN_INT64(parse_int(start + 3, end, 8));
  }
  // struct
  if (type_id == 12) {
    uint8* next_start = skip_field(start + 3, end, 12);
    int32 len = next_start - (start + 3);
    bytea* ret = palloc(len + VARHDRSZ);
    memcpy(VARDATA(ret), start + 3, len);
    SET_VARSIZE(ret, len + VARHDRSZ);
    PG_RETURN_POINTER(ret);
  }
  // list or set
  if (type_id == 14 || type_id == 15) {
    if (start + 7 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int8 element_type = *(start + 3);
    int32 len = parse_int(start + 4, end, 4);
    uint8* curr = start + 8;
    // prepare array
    Datum ret[256];
    bool nulls[256], typbyval;
    int16 typlen;
    char typalign;
    get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
    for (int i = 0; i < len; i++) {
      uint8* p = skip_field(curr, end, element_type);
      ret[i] = palloc(p - curr + VARHDRSZ);
      nulls[i] = false;
      memcpy(VARDATA(ret[i]), curr, p - curr);
      SET_VARSIZE(ret[i], p - curr + VARHDRSZ);
      curr = p;
    }
    int dims[MAXDIM], lbs[MAXDIM], ndims = 1;
    dims[0] = len; /* number of elements */
    lbs[0] = 1; /* lower bound is 1 */
    PG_RETURN_POINTER(
      construct_md_array(ret, nulls, ndims, dims, lbs, BYTEAOID, typlen, typbyval, typalign)
    );
  }
  // map
  if (type_id == 13) {
    if (start + 5 >= end) {
      elog(ERROR, invalid_thrift);
    }
    int32 len = *(start + 2);
    for (int i = 1; i < 4; i++) {
      len = (len << 8) + *(start + i + 2);
    }
    uint8* curr = start + 6;

    Datum ret[256];
    bool nulls[256], typbyval;
    int16 typlen;
    char typalign;
    get_typlenbyvalalign(BYTEAOID, &typlen, &typbyval, &typalign);
    for (int i = 0; i < 2 * len; i++) {
      int type_id = (i % 2 == 0? *start : *(start + 1));
      uint8* p = skip_field(curr, end, type_id);
      ret[i] = palloc(p - curr + VARHDRSZ);
      nulls[i] = false;
      memcpy(VARDATA(ret[i]), curr, p - curr);
      SET_VARSIZE(ret[i], p - curr + VARHDRSZ);
      curr = p;
    }
    int dims[MAXDIM], lbs[MAXDIM], ndims = 1;
    dims[0] = 2*len; /* number of elements */
    lbs[0] = 1; /* lower bound is 1 */
    PG_RETURN_POINTER(
      construct_md_array(ret, nulls, ndims, dims, lbs, BYTEAOID, typlen, typbyval, typalign)
    );
  }
  elog(ERROR, invalid_field_type);
}

// give start of data, end of data and type id,
// return pointer after its end
uint8* skip_field(uint8* start, uint8* end, int8 field_type) {
  uint8* ret = 0;
  if (field_type == 2) {
    ret = start + 1;
  } else if (field_type == 3 || field_type == 11) {
    int32 len = parse_int(start, end, 4);
    ret = start + sizeof(int32) + len;
  } else if (field_type == 4) {
    ret = start + sizeof(float8);
  } else if (field_type == 6) {
    ret = start + sizeof(int16);
  } else if (field_type == 8) {
    ret = start + sizeof(int32);
  } else if (field_type == 10) {
    ret = start + sizeof(int64);
  } else if (field_type == 12) {
    ret = start + 3;
    while (true) {
      if (*ret == 0) break;
      int8 field_type = *ret;
      ret = skip_field(ret, end, field_type);
    }
  } else if (field_type == 13) {
    int8 key_type = *start;
    int8 value_type = *(start + 1);
    int32 len = parse_int(start + 2, end, 4);
    ret = start + 6;
    for (int i = 0; i < len; i++) {
      ret = skip_field(ret, end, key_type);
      ret = skip_field(ret, end, value_type);
    }
  } else if (field_type == 14 || field_type == 15) {
    // set or list
    int32 len = parse_int(start + 1, end, 4);
    int8 field_type = *start;
    ret = start + 5;
    for (int i = 0; i < len; i++) {
      ret = skip_field(ret, end, field_type);
    }
  }

  if (ret > end) {
    elog(ERROR, invalid_thrift);
  }

  return ret;
}

Datum thrift_binary_decode(uint8* data, Size size, int16 field_id, int8 type_id) {
  uint8* start = data, *end = data + size;
  while (start < end) {
    if (start + 2 >= end) break;
    int16 parsed_field_id = parse_int(start + 1, end, 2);
    if (parsed_field_id == field_id) {
      if (*start == type_id) {
        return parse_field(start, end, type_id);
      }
      break;
    } else {
      int8 type_id = *start;
      start = start + 3;
      start = skip_field(start, end, type_id);
    }
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
