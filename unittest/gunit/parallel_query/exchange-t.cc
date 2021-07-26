/* Copyright (c) 2021, Huawei and/or its affiliates. All rights reserved.

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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>

#include "sql/sql_parallel.h"
#include "sql/exchange.h"


#include "unittest/gunit/test_utils.h"
#include "unittest/gunit/dd.h"
#include "unittest/gunit/parsertest.h"
#include "unittest/gunit/base_mock_field.h"
#include "unittest/gunit/fake_table.h"

using namespace std;
using my_testing::Server_initializer;
using dd_unittest::Mock_dd_field_longlong;
using dd_unittest::Mock_dd_field_varstring;

namespace Exchange_unittest {

class ExchangeNoSortTest :  public ::testing::Test {
protected:
  virtual void SetUp() { 
      initializer.SetUp();
      thd = initializer.thd();
    }
  virtual void TearDown() { initializer.TearDown(); }

  Server_initializer initializer;
  THD *thd;
};

TEST_F(ExchangeNoSortTest, InitTest){
  List<Field> fieldList;
  Fake_TABLE_SHARE  dummyShare(1);
  Mock_dd_field_longlong id;
  Mock_dd_field_varstring name(255, &dummyShare);

  fieldList.push_back(&id);
  fieldList.push_back(&name);

  Fake_TABLE table(fieldList);
  bitmap_set_all(table.write_set);


  int workers = 2;
  int refLength = 0;
  bool stabOutPut = false;
  Exchange_nosort exchangeNoSort(thd, &table, workers, refLength, stabOutPut);
  
  bool ret;
  ret = exchangeNoSort.init();
  EXPECT_EQ(false, ret);
  
}

class MockExchange_nosort : public Exchange_nosort{
  public : 
    MOCK_METHOD2(read_next,bool(void **, uint32 *));
    MOCK_METHOD3(covert_mq_data_to_record, bool(uchar*,int,uchar*));

    MockExchange_nosort(THD *thd, TABLE *table, int workers, int ref_length, bool stab_output)
    : Exchange_nosort(thd,table,workers,ref_length,stab_output){}

    virtual ~MockExchange_nosort() {}
};

TEST_F(ExchangeNoSortTest, readRecord){
  using ::testing::_;
  using ::testing::Return;
  List<Field> fieldList;
  Fake_TABLE_SHARE  dummyShare(1);
  Mock_dd_field_longlong id;
  Mock_dd_field_varstring name(255, &dummyShare);

  fieldList.push_back(&id);
  fieldList.push_back(&name);

  Fake_TABLE table(fieldList);
  bitmap_set_all(table.write_set);

  int workers = 2;
  int refLength = 0;
  bool stabOutPut = false;
//  MockExchange_nosort MockExchangeNoSort(thd, &table, workers, refLength, stabOutPut);
//  MockExchangeNoSort.init();

  MockExchange_nosort *mockExchangeNoSort = new MockExchange_nosort(thd, &table, workers, refLength, stabOutPut);
  
  
  ON_CALL(*mockExchangeNoSort,read_next(_, _)).WillByDefault(Return(true));
  EXPECT_CALL(*mockExchangeNoSort,read_next(_, _)).Times(1);

  ON_CALL(*mockExchangeNoSort,covert_mq_data_to_record(_, _, _)).WillByDefault(Return(true));
  EXPECT_CALL(*mockExchangeNoSort,covert_mq_data_to_record(_, _, _)).Times(1);

  bool ret;
  ret = mockExchangeNoSort->read_mq_record();
  EXPECT_EQ(true, ret);

}

}  // namespace
