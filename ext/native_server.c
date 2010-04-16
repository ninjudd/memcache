#include "ruby.h"
#include <libmemcached/memcached.h>
#include <ctype.h>

VALUE cMemcache;
VALUE cMemcacheBase;
VALUE cNativeServer;
VALUE cMemcacheError;
VALUE cMemcacheServerError;
VALUE cMemcacheClientError;
VALUE cMemcacheConnectionError;
VALUE sym_host;
VALUE sym_port;
VALUE sym_weight;
VALUE sym_prefix;
VALUE sym_hash;
VALUE sym_hash_with_prefix;
VALUE sym_distribution;
VALUE sym_binary;
VALUE sym_servers;
VALUE sym_ketama;
VALUE sym_ketama_weighted;

ID id_default;
ID id_md5;
ID id_crc;
ID id_fnv1_64;
ID id_fnv1a_64;
ID id_fnv1_32;
ID id_fnv1a_32;
ID id_jenkins;
ID id_hsieh;
ID id_murmur;
ID id_modula;
ID id_consistent;
ID id_ketama;
ID id_ketama_spy;

static ID iv_memcache_flags, iv_memcache_cas;

static void mc_free(void *p) {
  memcached_free(p);
}

static VALUE mc_alloc(VALUE klass) {
  memcached_st *mc;
  VALUE obj;

  mc = memcached_create(NULL);
  memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_CACHE_LOOKUPS, true);

  obj = Data_Wrap_Struct(klass, 0, mc_free, mc);
  return obj;
}

static VALUE throw_error(memcached_return_t *error) {
  switch(*error) {
  case MEMCACHED_SERVER_ERROR: rb_raise(cMemcacheServerError, "Server error");
  case MEMCACHED_CLIENT_ERROR: rb_raise(cMemcacheClientError, "Client error");
  case MEMCACHED_CONNECTION_FAILURE:
  case MEMCACHED_CONNECTION_BIND_FAILURE:
  case MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE:
    rb_raise(cMemcacheConnectionError, "Connection error");
  default:
    rb_raise(cMemcacheError, "Memcache error: %s", memcached_strerror(NULL, *error));
  }
  return Qnil;
}

static memcached_hash_t hash_behavior(VALUE sym) {
  ID id = SYM2ID(sym);

  if (id == id_default  ) return MEMCACHED_HASH_DEFAULT;
  if (id == id_md5      ) return MEMCACHED_HASH_MD5;
  if (id == id_crc      ) return MEMCACHED_HASH_CRC;
  if (id == id_fnv1_64  ) return MEMCACHED_HASH_FNV1_64;
  if (id == id_fnv1a_64 ) return MEMCACHED_HASH_FNV1A_64;
  if (id == id_fnv1_32  ) return MEMCACHED_HASH_FNV1_32;
  if (id == id_fnv1a_32 ) return MEMCACHED_HASH_FNV1A_32;
  if (id == id_jenkins  ) return MEMCACHED_HASH_JENKINS;
  if (id == id_hsieh    ) return MEMCACHED_HASH_HSIEH;
  if (id == id_murmur   ) return MEMCACHED_HASH_MURMUR;
  rb_raise(cMemcacheError, "Invalid hash behavior");
}

static memcached_hash_t distribution_behavior(VALUE sym) {
  ID id = SYM2ID(sym);

  if (id == id_modula     ) return MEMCACHED_DISTRIBUTION_MODULA;
  if (id == id_consistent ) return MEMCACHED_DISTRIBUTION_CONSISTENT;
  if (id == id_ketama     ) return MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA;
  if (id == id_ketama_spy ) return MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY;
  rb_raise(cMemcacheError, "Invalid distribution behavior");
}

static VALUE mc_initialize(VALUE self, VALUE opts) {
  memcached_st *mc;
  VALUE servers_aryv, prefixv, hashv, distributionv;

  Data_Get_Struct(self, memcached_st, mc);
  hashv         = rb_hash_aref(opts, sym_hash);
  distributionv = rb_hash_aref(opts, sym_distribution);
  prefixv       = rb_hash_aref(opts, sym_prefix);
  servers_aryv  = rb_hash_aref(opts, sym_servers);

  if (!NIL_P(hashv)) {
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_HASH,        hash_behavior(hashv));
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_KETAMA_HASH, hash_behavior(hashv));
  }

  if (!NIL_P(distributionv))
    memcached_behavior_set_distribution(mc, distribution_behavior(distributionv));

  if (RTEST( rb_hash_aref(opts, sym_ketama) ))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_KETAMA, true);

  if (RTEST( rb_hash_aref(opts, sym_ketama_weighted) ))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED, true);

  if (RTEST( rb_hash_aref(opts, sym_hash_with_prefix) ))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_HASH_WITH_PREFIX_KEY, true);

  if (RTEST( rb_hash_aref(opts, sym_binary) ))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, true);

  if (!NIL_P(prefixv))
    memcached_callback_set(mc, MEMCACHED_CALLBACK_PREFIX_KEY, STR2CSTR(prefixv));

  if (!NIL_P(servers_aryv)) {
    char* server;
    int i;

    for (i = 0; i < RARRAY_LEN(servers_aryv); i++) {
      server    = StringValuePtr(RARRAY_PTR(servers_aryv)[i]);
      memcached_server_push(mc, memcached_servers_parse(server));
    }
  } else {
    VALUE hostv, portv, weightv;
    char* host;
    int   port, weight;

    hostv   = rb_hash_aref(opts, sym_host);
    portv   = rb_hash_aref(opts, sym_port);
    weightv = rb_hash_aref(opts, sym_weight);
    host    = StringValuePtr(hostv);
    port    = NIL_P(portv) ? MEMCACHED_DEFAULT_PORT : NUM2INT(portv);
    weight  = NIL_P(weightv) ? 0 : NUM2INT(weightv);

    memcached_server_add_with_weight(mc, StringValuePtr(hostv), port, weight);
  }

  return self;
}

#ifdef RUBY_19
#define RSTRING_SET_LEN(str, newlen) (rb_str_set_len(str, new_len))
#else
#define RSTRING_SET_LEN(str, newlen) (RSTRING(str)->len = new_len)
#endif

static VALUE escape_key(VALUE key, bool* escaped) {
  char*    str = RSTRING_PTR(key);
  uint16_t len = RSTRING_LEN(key);
  char*    new_str = NULL;
  uint16_t new_len = len;
  uint16_t i, j;

  for (i = 0; i < len; i++) {
    if (isspace(str[i]) || str[i] == '\\') new_len++;
  }

  if (new_len == len) {
    if (escaped) *escaped = false;
    return key;
  } else {
    if (escaped) *escaped = true;
    key = rb_str_buf_new(new_len);
    RSTRING_SET_LEN(key, new_len);
    new_str = RSTRING_PTR(key);

    for (i = 0, j = 0; i < len; i++, j++) {
      if (isspace(str[i]) || str[i] == '\\') {
        new_str[j] = '\\';
        switch (str[i]) {
        case ' '  : new_str[++j] = 's';  break;
        case '\t' : new_str[++j] = 't';  break;
        case '\n' : new_str[++j] = 'n';  break;
        case '\v' : new_str[++j] = 'v';  break;
        case '\f' : new_str[++j] = 'f';  break;
        case '\\' : new_str[++j] = '\\'; break;
        }
      } else {
        new_str[j] = str[i];
      }
    }
    return key;
  }
}

static VALUE unescape_key(const char* str, uint16_t len) {
  uint16_t i,j;
  VALUE    key;
  char*    new_str;
  uint16_t new_len = len;

  for (i = 0; i < len; i++) {
    if (str[i] == '\\') {
      new_len--;
      i++;
    }
  }

  if (new_len == len) {
    key = rb_str_new(str, len);
  } else {
    key = rb_str_buf_new(new_len);
    RSTRING_SET_LEN(key, new_len);
    new_str = RSTRING_PTR(key);

    for (i = 0, j = 0; i < len; j++, i++) {
      if (str[i] == '\\') {
        switch (str[++i]) {
        case 's'  : new_str[j] = ' ';  break;
        case 't'  : new_str[j] = '\t'; break;
        case 'n'  : new_str[j] = '\n'; break;
        case 'v'  : new_str[j] = '\v'; break;
        case 'f'  : new_str[j] = '\f'; break;
        case '\\' : new_str[j] = '\\'; break;
        }
      } else {
        new_str[j] = str[i];
      }
    }
  }
  return key;
}

static bool use_binary(memcached_st* mc) {
  return memcached_behavior_get(mc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) != 0;
}

static VALUE mc_get(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE cas, keys, results, key, value;
  VALUE scalar_key = Qnil;
  memcached_return status;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &keys, &cas);
  memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, RTEST(cas) ? 1 : 0);

  if (RTEST(cas) && TYPE(keys) != T_ARRAY) {
    scalar_key = keys;
    keys = rb_ary_new4(1, &keys);
  }

  if (TYPE(keys) != T_ARRAY) {
    char*    str;
    size_t   len;
    uint32_t flags;

    key = use_binary(mc) ? keys : escape_key(keys, NULL);
    str = memcached_get(mc, RSTRING_PTR(key), RSTRING_LEN(key), &len, &flags, &status);
    if (str == NULL) return Qnil;

    if (status == MEMCACHED_SUCCESS) {
      value = rb_str_new(str, len);
      rb_ivar_set(value, iv_memcache_flags, INT2NUM(flags));
      free(str);
      return value;
    } else {
      printf("Memcache read error: %s %u\n", memcached_strerror(mc, status), status);
      return Qnil;
    }
  } else {
    memcached_result_st* result;
    size_t       num_keys, i;
    const char** key_strings;
    size_t*      key_lengths;
    bool         escaped;

    results = rb_hash_new();
    num_keys = RARRAY_LEN(keys);
    if (num_keys == 0) return results;

    key_strings = (const char**) malloc(num_keys * sizeof(char *));
    key_lengths = (size_t *) malloc(num_keys * sizeof(size_t));
    for (i = 0; i < RARRAY_LEN(keys); i++) {
      key = RARRAY_PTR(keys)[i];
      if (!use_binary(mc)) key = escape_key(key, &escaped);

      key_lengths[i] = RSTRING_LEN(key);
      key_strings[i] = RSTRING_PTR(key);
    }

    memcached_mget(mc, key_strings, key_lengths, num_keys);

    while (result = memcached_fetch_result(mc, NULL, &status)) {
      if (escaped) {
        key = unescape_key(memcached_result_key_value(result), memcached_result_key_length(result));
      } else {
        key = rb_str_new(memcached_result_key_value(result), memcached_result_key_length(result));
      }

      if (status == MEMCACHED_SUCCESS) {
        value = rb_str_new(memcached_result_value(result), memcached_result_length(result));
        rb_ivar_set(value, iv_memcache_flags, INT2NUM(memcached_result_flags(result)));
        if (RTEST(cas)) rb_ivar_set(value, iv_memcache_cas, ULL2NUM(memcached_result_cas(result)));
        memcached_result_free(result);
        rb_hash_aset(results, key, value);
      } else {
        printf("Memcache read error: %s %u\n", memcached_strerror(mc, status), status);
      }
    }
    free(key_strings);
    free(key_lengths);
    if (!NIL_P(scalar_key)) return rb_hash_aref(results, scalar_key);
    return results;
  }
}

VALUE mc_set(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_set(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                        RTEST(expiry) ? NUM2UINT(expiry) : 0,
                        RTEST(flags)  ? NUM2UINT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else {
    return throw_error(&result);
  }
}

static VALUE mc_cas(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, cas, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "32", &key, &value, &cas, &expiry, &flags);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_cas(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                        RTEST(expiry) ? NUM2UINT(expiry) : 0,
                        RTEST(flags)  ? NUM2UINT(flags)  : 0,
                        NUM2ULL(cas));

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if (result == MEMCACHED_NOTFOUND || result == MEMCACHED_DATA_EXISTS) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_incr(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, amount;
  static memcached_return_t result;
  unsigned int offset;
  uint64_t value;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key, &amount);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  offset = RTEST(amount) ? NUM2INT(amount) : 1;

  result = memcached_increment(mc, RSTRING_PTR(key), RSTRING_LEN(key), offset, &value);

  if (result == MEMCACHED_SUCCESS) {
    return LONG2NUM(value);
  } else if (result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_decr(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, amount;
  static memcached_return_t result;
  unsigned int offset;
  uint64_t value;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key, &amount);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  offset = RTEST(amount) ? NUM2INT(amount) : 1;

  result = memcached_decrement(mc, RSTRING_PTR(key), RSTRING_LEN(key), offset, &value);

  if (result == MEMCACHED_SUCCESS) {
    return LONG2NUM(value);
  } else if (result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_delete(VALUE self, VALUE key) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  result = memcached_delete(mc, RSTRING_PTR(key), RSTRING_LEN(key), 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_add(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_add(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                         RTEST(expiry) ? NUM2UINT(expiry) : 0,
                         RTEST(flags)  ? NUM2UINT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_replace(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_replace(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                         RTEST(expiry) ? NUM2UINT(expiry) : 0,
                         RTEST(flags)  ? NUM2UINT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_append(VALUE self, VALUE key, VALUE value) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_append(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value), 0, 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qfalse;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_prepend(VALUE self, VALUE key, VALUE value) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  key = StringValue(key);
  if (!use_binary(mc)) key = escape_key(key, NULL);
  value = StringValue(value);

  result = memcached_prepend(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value), 0, 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qfalse;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_flush_all(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE delay;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "01", &delay);

  result = memcached_flush(mc, RTEST(delay) ? NUM2UINT(delay) : 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE mc_set_prefix(VALUE self, VALUE prefix) {
  memcached_st *mc;
  static memcached_return_t result;
  Data_Get_Struct(self, memcached_st, mc);

  if (NIL_P(prefix)) {
    result = memcached_callback_set(mc, MEMCACHED_CALLBACK_PREFIX_KEY, NULL);
  } else {
    prefix = StringValue(prefix);
    result = memcached_callback_set(mc, MEMCACHED_CALLBACK_PREFIX_KEY, STR2CSTR(prefix));
  }
  return prefix;
}

VALUE mc_get_prefix(VALUE self) {
  memcached_st *mc;
  static memcached_return_t result;
  char* prefix;

  Data_Get_Struct(self, memcached_st, mc);
  prefix = (char*) memcached_callback_get(mc, MEMCACHED_CALLBACK_PREFIX_KEY, &result);

  return prefix ? rb_str_new2(prefix) : Qnil;
}

VALUE mc_close(VALUE self) {
  memcached_st *mc;
  Data_Get_Struct(self, memcached_st, mc);
  memcached_quit(mc);
  return Qnil;
}

void Init_native_server() {
  sym_host             = ID2SYM(rb_intern("host"));
  sym_port             = ID2SYM(rb_intern("port"));
  sym_weight           = ID2SYM(rb_intern("weight"));
  sym_prefix           = ID2SYM(rb_intern("prefix"));
  sym_hash             = ID2SYM(rb_intern("hash"));
  sym_hash_with_prefix = ID2SYM(rb_intern("hash_with_prefix"));
  sym_distribution     = ID2SYM(rb_intern("distribution"));
  sym_binary           = ID2SYM(rb_intern("binary"));
  sym_servers          = ID2SYM(rb_intern("servers"));
  sym_ketama           = ID2SYM(rb_intern("ketama"));
  sym_ketama_weighted  = ID2SYM(rb_intern("ketama_weighted"));

  iv_memcache_flags = rb_intern("@memcache_flags");
  iv_memcache_cas   = rb_intern("@memcache_cas");

  id_default    = rb_intern("default");
  id_md5        = rb_intern("md5");
  id_crc        = rb_intern("crc");
  id_fnv1_64    = rb_intern("fnv1_64");
  id_fnv1a_64   = rb_intern("fnv1a_64");
  id_fnv1_32    = rb_intern("fnv1_32");
  id_fnv1a_32   = rb_intern("fnv1a_32");
  id_jenkins    = rb_intern("jenkins");
  id_hsieh      = rb_intern("hsieh");
  id_murmur     = rb_intern("murmur");
  id_modula     = rb_intern("modula");
  id_consistent = rb_intern("consistent");
  id_ketama     = rb_intern("ketama");
  id_ketama_spy = rb_intern("ketama_spy");

  cMemcache = rb_define_class("Memcache", rb_cObject);

  cMemcacheError           = rb_define_class_under(cMemcache, "Error",           rb_eStandardError);
  cMemcacheServerError     = rb_define_class_under(cMemcache, "ServerError",     cMemcacheError);
  cMemcacheClientError     = rb_define_class_under(cMemcache, "ClientError",     cMemcacheError);
  cMemcacheConnectionError = rb_define_class_under(cMemcache, "ConnectionError", cMemcacheError);

  cMemcacheBase = rb_define_class_under(cMemcache, "Base", rb_cObject);
  cNativeServer = rb_define_class_under(cMemcache, "NativeServer", cMemcacheBase);
  rb_define_alloc_func(cNativeServer, mc_alloc);
  rb_define_method(cNativeServer, "initialize", mc_initialize, 1);

  rb_define_method(cNativeServer, "get",       mc_get,       -1);
  rb_define_method(cNativeServer, "set",       mc_set,       -1);
  rb_define_method(cNativeServer, "add",       mc_add,       -1);
  rb_define_method(cNativeServer, "cas",       mc_cas,       -1);
  rb_define_method(cNativeServer, "replace",   mc_replace,   -1);
  rb_define_method(cNativeServer, "incr",      mc_incr,      -1);
  rb_define_method(cNativeServer, "decr",      mc_decr,      -1);
  rb_define_method(cNativeServer, "append",    mc_append,     2);
  rb_define_method(cNativeServer, "prepend",   mc_prepend,    2);
  rb_define_method(cNativeServer, "delete",    mc_delete,     1);
  rb_define_method(cNativeServer, "close",     mc_close,      0);
  rb_define_method(cNativeServer, "flush_all", mc_flush_all, -1);

  rb_define_method(cNativeServer, "prefix=", mc_set_prefix, 1);
  rb_define_method(cNativeServer, "prefix",  mc_get_prefix, 0);
}
