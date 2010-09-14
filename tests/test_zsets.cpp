#include "functions.h"

#include "../redisclient.h"

void test_zsets(redis::client & c)
{
  test("zadd");
  {
    c.zadd("zset1", 1.234, "zval1");
    c.zadd("zset1", 2.719, "zval2");
    c.zadd("zset1", 3.141, "zval3");
    
    ASSERT_EQUAL(c.exists("zset1"), true);
    ASSERT_EQUAL(c.type("zset1"), redis::client::datatype_zset);
    ASSERT_EQUAL(c.zcard("zset1"), 3L);
  }
  
  test("zrem");
  {
    c.zrem("zset1", "zval1");
    
    ASSERT_EQUAL(c.exists("zset1"), true);
    ASSERT_EQUAL(c.type("zset1"), redis::client::datatype_zset);
    ASSERT_EQUAL(c.zcard("zset1"), 2L);
  }
}