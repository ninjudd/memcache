#include "ruby.h"
#include <libmemcached/memcached.h>

VALUE cMemcache;
VALUE cMemcacheBase;
VALUE cNativeServer;
VALUE cMemcacheError;
VALUE cMemcacheServerError;
VALUE cMemcacheClientError;
VALUE cMemcacheConnectionError;
VALUE sym_host;
VALUE sym_port;
VALUE sym_prefix;
VALUE sym_hash;
VALUE sym_hash_with_prefix;
VALUE sym_servers;

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

static ID iv_memcache_flags, iv_memcache_cas;

static void ns_free(void *p) {
  memcached_free(p);
}

static VALUE ns_alloc(VALUE klass) {
  memcached_st *ns;
  VALUE obj;

  ns  = memcached_create(NULL);
  obj = Data_Wrap_Struct(klass, 0, ns_free, ns);
  return obj;
}

VALUE throw_error(memcached_return_t *error) {
  memcached_st *mc;
  printf("ERROR: %s\n", memcached_strerror(mc, *error));
  switch(*error) {

    case MEMCACHED_SERVER_ERROR:
      rb_raise(cMemcacheServerError, "Server error");

    case MEMCACHED_CLIENT_ERROR:
      rb_raise(cMemcacheClientError, "Client error");

    case MEMCACHED_CONNECTION_FAILURE:
    case MEMCACHED_CONNECTION_BIND_FAILURE:
    case MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE:
      rb_raise(cMemcacheConnectionError, "Connection error");

    default:
      rb_raise(cMemcacheError, "Memcache error");
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

static VALUE ns_initialize(VALUE self, VALUE opts) {
  memcached_st *mc;
  VALUE hostv, portv, servers_aryv, prefixv, hashv;
  char* host;
  char* server;
  char* hashkit;
  int   port, i;

  Data_Get_Struct(self, memcached_st, mc);
  hashv        = rb_hash_aref(opts, sym_hash);
  prefixv      = rb_hash_aref(opts, sym_prefix);
  servers_aryv = rb_hash_aref(opts, sym_servers);

  if (!NIL_P(hashv))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_HASH, hash_behavior(hashv));

  if (RTEST( rb_hash_aref(opts, sym_hash_with_prefix) ))
    memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_HASH_WITH_PREFIX_KEY, true);

  if (!NIL_P(prefixv))
    memcached_callback_set(mc, MEMCACHED_CALLBACK_PREFIX_KEY, STR2CSTR(prefixv));

  if (!NIL_P(servers_aryv)) {
    for (i = 0; i < RARRAY(servers_aryv)->len; i++) {
      server    = StringValuePtr(RARRAY(servers_aryv)->ptr[i]);
      memcached_server_push(mc, memcached_servers_parse(server));
    }
  } else {
    hostv = rb_hash_aref(opts, sym_host);
    portv = rb_hash_aref(opts, sym_port);
    host  = StringValuePtr(hostv);
    port  = NIL_P(portv) ? MEMCACHED_DEFAULT_PORT : NUM2INT(portv);

    memcached_server_add(mc, host, port);
  }

  return self;
}

static VALUE ns_get(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key_or_keys, cas, keys, results, key, value;
  memcached_return error;
  static memcached_result_st result;
  size_t       num_keys, i;
  const char** key_strings;
  size_t*      key_lengths;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key_or_keys, &cas);

  keys = TYPE(key_or_keys) == T_ARRAY ? key_or_keys : rb_ary_new4(1, &key_or_keys);

  num_keys = RARRAY_LEN(keys);
  key_strings = (const char**) malloc(num_keys * sizeof(char *));
  key_lengths = (size_t *) malloc(num_keys * sizeof(size_t));

  if (num_keys == 0) return rb_hash_new();

  for (i = 0; i < RARRAY(keys)->len; i++) {
    key = StringValue(RARRAY(keys)->ptr[i]);
    key_lengths[i] = RSTRING_LEN(key);
    key_strings[i] = RSTRING_PTR(key);
  }

  memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, RTEST(cas) ? 1 : 0);
  memcached_mget(mc, key_strings, key_lengths, num_keys);
  memcached_result_create(mc, &result);

  if (keys == key_or_keys) results = rb_hash_new();

  while (memcached_fetch_result(mc, &result, &error)) {
    key   = rb_str_new(memcached_result_key_value(&result), memcached_result_key_length(&result));
    value = rb_str_new(memcached_result_value(&result),     memcached_result_length(&result));
    rb_ivar_set(value, iv_memcache_flags, INT2NUM(memcached_result_flags(&result)));
    if (RTEST(cas)) rb_ivar_set(value, iv_memcache_cas, INT2NUM(memcached_result_cas(&result)));

    if (keys != key_or_keys) return value;

    rb_hash_aset(results, key, value);
  }

  if (error != MEMCACHED_END) {
    printf("Memcache read error: %s %u\n", memcached_strerror(mc, error), error);
  }

  if (keys != key_or_keys) {
    return Qnil;
  } else {
    return results;
  }
}

VALUE ns_set(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key   = StringValue(key);
  value = StringValue(value);

  result = memcached_set(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                        RTEST(expiry) ? NUM2INT(expiry) : 0,
                        RTEST(flags)  ? NUM2INT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else {
    return throw_error(&result);
  }
}

static VALUE ns_cas(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, cas, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "32", &key, &value, &cas, &expiry, &flags);

  key   = StringValue(key);
  value = StringValue(value);

  result = memcached_cas(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                        RTEST(expiry) ? NUM2INT(expiry) : 0,
                        RTEST(flags)  ? NUM2INT(flags)  : 0,
                        NUM2INT(cas));

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if (result == MEMCACHED_NOTFOUND || result == MEMCACHED_DATA_EXISTS) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_incr(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, amount;
  memcached_return error;
  static memcached_return_t result;
  uint64_t *value;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key, &amount);

  key    = StringValue(key);
  amount = RTEST(amount) ? NUM2INT(amount) : 1;

  result = memcached_increment(mc, RSTRING_PTR(key), RSTRING_LEN(key), amount, value);

  if (result == MEMCACHED_SUCCESS) {
    return LONG2NUM(*value);
  } else if (result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_decr(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, amount;
  memcached_return error;
  static memcached_return_t result;
  uint64_t *value;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "11", &key, &amount);

  key    = StringValue(key);
  amount = RTEST(amount) ? NUM2INT(amount) : 1;

  result = memcached_decrement(mc, RSTRING_PTR(key), RSTRING_LEN(key), amount, value);

  if (result == MEMCACHED_SUCCESS) {
    return LONG2NUM(*value);
  } else if (result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_delete(VALUE self, VALUE key) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  result = memcached_delete(mc, RSTRING_PTR(key), RSTRING_LEN(key), 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTFOUND) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_add(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key   = StringValue(key);
  value = StringValue(value);

  result = memcached_add(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                         RTEST(expiry) ? NUM2INT(expiry) : 0,
                         RTEST(flags)  ? NUM2INT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_replace(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE key, value, expiry, flags;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "22", &key, &value, &expiry, &flags);

  key   = StringValue(key);
  value = StringValue(value);

  result = memcached_replace(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value),
                         RTEST(expiry) ? NUM2INT(expiry) : 0,
                         RTEST(flags)  ? NUM2INT(flags)  : 0);

  if (result == MEMCACHED_SUCCESS) {
    return value;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_append(VALUE self, VALUE key, VALUE value) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  result = memcached_append(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value), 0, 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qfalse;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_prepend(VALUE self, VALUE key, VALUE value) {
  memcached_st *mc;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);

  result = memcached_prepend(mc, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value), 0, 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qtrue;
  } else if(result == MEMCACHED_NOTSTORED) {
    return Qfalse;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_flush_all(int argc, VALUE *argv, VALUE self) {
  memcached_st *mc;
  VALUE delay;
  static memcached_return_t result;

  Data_Get_Struct(self, memcached_st, mc);
  rb_scan_args(argc, argv, "01", &delay);

  result = memcached_flush(mc, RTEST(delay) ? NUM2INT(delay) : 0);

  if (result == MEMCACHED_SUCCESS) {
    return Qnil;
  } else {
    return throw_error(&result);
  }
}

VALUE ns_set_prefix(VALUE self, VALUE prefix) {
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

VALUE ns_get_prefix(VALUE self) {
  memcached_st *mc;
  static memcached_return_t result;
  char* prefix;

  Data_Get_Struct(self, memcached_st, mc);
  prefix = (char*) memcached_callback_get(mc, MEMCACHED_CALLBACK_PREFIX_KEY, &result);

  return prefix ? rb_str_new2(prefix) : Qnil;
}

void Init_native_server() {
  sym_host             = ID2SYM(rb_intern("host"));
  sym_port             = ID2SYM(rb_intern("port"));
  sym_prefix           = ID2SYM(rb_intern("prefix"));
  sym_hash             = ID2SYM(rb_intern("hash"));
  sym_hash_with_prefix = ID2SYM(rb_intern("hash_with_prefix"));
  sym_servers          = ID2SYM(rb_intern("servers"));

  iv_memcache_flags = rb_intern("@memcache_flags");
  iv_memcache_cas   = rb_intern("@memcache_cas");

  id_default  = rb_intern("default");
  id_md5      = rb_intern("md5");
  id_crc      = rb_intern("crc");
  id_fnv1_64  = rb_intern("fnv1_64");
  id_fnv1a_64 = rb_intern("fnv1a_64");
  id_fnv1_32  = rb_intern("fnv1_32");
  id_fnv1a_32 = rb_intern("fnv1a_32");
  id_jenkins  = rb_intern("jenkins");
  id_hsieh    = rb_intern("hsieh");
  id_murmur   = rb_intern("murmur");

  cMemcache = rb_define_class("Memcache", rb_cObject);

  cMemcacheError           = rb_define_class_under(cMemcache, "Error",           rb_eStandardError);
  cMemcacheServerError     = rb_define_class_under(cMemcache, "ServerError",     cMemcacheError);
  cMemcacheClientError     = rb_define_class_under(cMemcache, "ClientError",     cMemcacheError);
  cMemcacheConnectionError = rb_define_class_under(cMemcache, "ConnectionError", cMemcacheError);

  cMemcacheBase = rb_define_class_under(cMemcache, "Base", rb_cObject);
  cNativeServer = rb_define_class_under(cMemcache, "NativeServer", cMemcacheBase);
  rb_define_alloc_func(cNativeServer, ns_alloc);
  rb_define_method(cNativeServer, "initialize", ns_initialize, 1);

  rb_define_method(cNativeServer, "get",       ns_get,       -1);
  rb_define_method(cNativeServer, "set",       ns_set,       -1);
  rb_define_method(cNativeServer, "add",       ns_add,       -1);
  rb_define_method(cNativeServer, "cas",       ns_cas,       -1);
  rb_define_method(cNativeServer, "replace",   ns_replace,   -1);
  rb_define_method(cNativeServer, "incr",      ns_incr,      -1);
  rb_define_method(cNativeServer, "decr",      ns_decr,      -1);
  rb_define_method(cNativeServer, "append",    ns_append,     2);
  rb_define_method(cNativeServer, "prepend",   ns_prepend,    2);
  rb_define_method(cNativeServer, "delete",    ns_delete,     1);
  rb_define_method(cNativeServer, "flush_all", ns_flush_all, -1);

  rb_define_method(cNativeServer, "prefix=", ns_set_prefix, 1);
  rb_define_method(cNativeServer, "prefix",  ns_get_prefix, 0);
}
