/* Copyright (c) 2013, 2020, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, Huawei Technologies Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/pq_condition.h"
#include "sql/item_strfunc.h"
#include "sql/item_sum.h"
#include "sql/mysqld.h"
#include "sql/opt_range.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_parallel.h"
#include "sql/sql_tmp_table.h"

const enum_field_types NO_PQ_SUPPORTED_FIELD_TYPES[] = {
    MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_BLOB,
    MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_JSON,        MYSQL_TYPE_GEOMETRY};

const Item_sum::Sumfunctype NO_PQ_SUPPORTED_AGG_FUNC_TYPES[] = {
    Item_sum::COUNT_DISTINCT_FUNC,
    Item_sum::SUM_DISTINCT_FUNC,
    Item_sum::AVG_DISTINCT_FUNC,
    Item_sum::GROUP_CONCAT_FUNC,
    Item_sum::JSON_AGG_FUNC,
    Item_sum::UDF_SUM_FUNC,
    Item_sum::STD_FUNC,
    Item_sum::VARIANCE_FUNC,
    Item_sum::SUM_BIT_FUNC};

const Item_func::Functype NO_PQ_SUPPORTED_FUNC_TYPES[] = {
    Item_func::MATCH_FUNC, Item_func::SUSERVAR_FUNC, Item_func::FUNC_SP,
    Item_func::JSON_FUNC,  Item_func::SUSERVAR_FUNC, Item_func::UDF_FUNC,
    Item_func::XML_FUNC};

const char *NO_PQ_SUPPORTED_FUNC_ARGS[] = {
    "rand",          "json_valid",         "json_length",
    "json_type",     "json_contains_path", "json_unquote",
    "st_distance",   "get_lock",           "is_free_lock",
    "is_used_lock",  "release_lock",       "sleep",
    "xml_str",       "json_func",
    "weight_string",  // Data truncation (MySQL BUG)
    "des_decrypt"     // Data truncation
};

const char *NO_PQ_SUPPORTED_FUNC_NO_ARGS[] = {"release_all_locks"};

/**
 * return true when type is a not_supported_field; return false otherwise.
 */
bool pq_not_support_datatype(enum_field_types type) {
  for (const enum_field_types &field_type : NO_PQ_SUPPORTED_FIELD_TYPES) {
    if (type == field_type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported function type
 */
bool pq_not_support_functype(Item_func::Functype type) {
  for (const Item_func::Functype &func_type : NO_PQ_SUPPORTED_FUNC_TYPES) {
    if (type == func_type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported function
 */
bool pq_not_support_func(Item_func *func) {
  if (pq_not_support_functype(func->functype())) {
    return true;
  }

  for (const char *funcname : NO_PQ_SUPPORTED_FUNC_ARGS) {
    if (!strcmp(func->func_name(), funcname) && func->arg_count != 0) {
      return true;
    }
  }

  for (const char *funcname : NO_PQ_SUPPORTED_FUNC_NO_ARGS) {
    if (!strcmp(func->func_name(), funcname)) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ support aggregation function
 */
bool pq_not_support_aggr_functype(Item_sum::Sumfunctype type) {
  for (const Item_sum::Sumfunctype &sum_func_type :
       NO_PQ_SUPPORTED_AGG_FUNC_TYPES) {
    if (type == sum_func_type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported ref function
 */
bool pq_not_support_ref(Item_ref *ref, bool having) {
  Item_ref::Ref_Type type = ref->ref_type();
  if (type == Item_ref::OUTER_REF) {
    return true;
  }
  /**
   * Now, when the sql contains a aggregate function after the 'having',
   * we do not support parallel query. For example:
   * select t1.col1 from t1 group by t1.col1 having avg(t1.col1) > 0;
   * So, we disable the sql;
   */
  if (having && type == Item_ref::AGGREGATE_REF) {
    return true;
  }

  return false;
}

typedef bool (*PQ_CHECK_ITEM_FUN)(Item *item, bool having);

struct PQ_CHECK_ITEM_TYPE {
  Item::Type item_type;
  PQ_CHECK_ITEM_FUN fun_ptr;
};

bool check_pq_support_fieldtype(Item *item, bool having);

bool check_pq_support_fieldtype_of_field_item(Item *item,
                                              bool MY_ATTRIBUTE((unused))) {
  Field *field = static_cast<Item_field *>(item)->field;
  DBUG_ASSERT(field);
  // not supported for generated column
  if (field && (field->is_gcol() || pq_not_support_datatype(field->type()))) {
    return false;
  }

  return true;
}

bool check_pq_support_fieldtype_of_func_item(Item *item, bool having) {
  Item_func *func = static_cast<Item_func *>(item);
  DBUG_ASSERT(func);

  // check func type
  if (pq_not_support_func(func)) {
    return false;
  }

  // the case of Item_func_make_set
  if (!strcmp(func->func_name(), "make_set")) {
    Item *arg_item = down_cast<Item_func_make_set *>(func)->item;
    if (arg_item && !check_pq_support_fieldtype(arg_item, having)) {
      return false;
    }
  }

  // check func args type
  for (uint i = 0; i < func->arg_count; i++) {
    // c: args contain unsupported fields
    Item *arg_item = func->arguments()[i];
    if (arg_item == nullptr ||
        !check_pq_support_fieldtype(arg_item, having)) {  // c
      return false;
    }
  }

  // the case of Item_equal
  if (func->functype() == Item_func::MULT_EQUAL_FUNC) {
    Item_equal *item_equal = down_cast<Item_equal *>(item);
    DBUG_ASSERT(item_equal);

    // check const_item
    Item *const_item = item_equal->get_const();
    if (const_item &&
        (const_item->type() == Item::SUM_FUNC_ITEM ||         // c1
         !check_pq_support_fieldtype(const_item, having))) {  // c2
      return false;
    }

    // check fields
    Item *field_item = nullptr;
    List<Item_field> fields = item_equal->get_fields();
    List_iterator_fast<Item_field> it(fields);
    for (size_t i = 0; (field_item = it++); i++) {
      if (!check_pq_support_fieldtype(field_item, having)) {
        return false;
      }
    }
  }

  return true;
}
