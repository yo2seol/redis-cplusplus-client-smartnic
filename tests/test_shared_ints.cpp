#include "../redisclient.h"

#include "functions.h"

void test_shared_ints(redis::client & c)
{
  test("shared_int");
  {
    redis::shared_int sh_int1(c, "sh_int1", 123);
    
    int int1 = sh_int1;
    ASSERT_EQUAL(int1, 123);
    ASSERT_EQUAL(sh_int1.to_int(), (redis::client::int_type) 123);
    
    ASSERT_EQUAL(sh_int1++, (redis::client::int_type) 123);
    ASSERT_EQUAL(++sh_int1, (redis::client::int_type) 125);
    
    ASSERT_EQUAL(sh_int1 == 125, true);
    ASSERT_EQUAL(sh_int1 != 125, false);
    
    sh_int1 -= sh_int1;
    ASSERT_EQUAL(sh_int1.to_int(), (redis::client::int_type) 0);
    sh_int1 += 314;
    ASSERT_EQUAL(sh_int1 == 314, true);
    
    redis::shared_int si(c, "si", 0);
    redis::client::int_type i, i1 = 0;
    while( (i = si++) < 10 )
      ASSERT_EQUAL(i, i1++);
  }
}