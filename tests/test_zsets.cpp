#include "functions.h"

#include "../redisclient.h"

void test_zsets(redis::client & c)
{
  test("zadd");
  {
    c.zadd("{zset}1", 2.719, "zval1");
    c.zadd("{zset}1", 1.234, "zval2");
    c.zadd("{zset}1", 3.141, "zval3");
    
    ASSERT_EQUAL(c.exists("{zset}1"), true);
    ASSERT_EQUAL(c.type("{zset}1"), redis::client::datatype_zset);
    ASSERT_EQUAL(c.zcard("{zset}1"), 3L);
  }
  
  test("zrem");
  {
    c.zrem("{zset}1", "zval1");
    
    ASSERT_EQUAL(c.exists("{zset}1"), true);
    ASSERT_EQUAL(c.type("{zset}1"), redis::client::datatype_zset);
    ASSERT_EQUAL(c.zcard("{zset}1"), 2L);
    
    c.zadd("{zset}1", 2.719, "zval1");
  }

  test("zincrby");
  {
    c.zincrby("{zset}1", "zval3", 5);
    ASSERT_EQUAL(c.zscore("{zset}1",  "zval3"), 3.141+5);
    c.zincrby("{zset}1", "zval3", -5);
    ASSERT_EQUAL(c.zscore("{zset}1",  "zval3"), 3.141);
  }

  test("zrank");
  {
    ASSERT_EQUAL(c.zrank("{zset}1", "zval1"), 1L);
    ASSERT_EQUAL(c.zrank("{zset}1", "zval2"), 0L);
    ASSERT_EQUAL(c.zrank("{zset}1", "zval3"), 2L);
  }
 
  test("zrevrank");
  {
    ASSERT_EQUAL(c.zrevrank("{zset}1", "zval1"), 1L);
    ASSERT_EQUAL(c.zrevrank("{zset}1", "zval2"), 2L);
    ASSERT_EQUAL(c.zrevrank("{zset}1", "zval3"), 0L);
  }

  test("zrange");
  {
    redis::client::string_vector first_two;
    c.zrange("{zset}1", 0, 1, first_two);
    ASSERT_EQUAL(first_two.size(), (size_t) 2);
    ASSERT_EQUAL(first_two[0], string("zval2") );
    ASSERT_EQUAL(first_two[1], string("zval1") );
    
    redis::client::string_vector last_two;
    c.zrange("{zset}1", -2, -1, last_two);
    ASSERT_EQUAL(last_two.size(), (size_t) 2);
    ASSERT_EQUAL(last_two[1], string("zval3") );
    ASSERT_EQUAL(last_two[0], string("zval1") );
  }
  
  test("zrevrange");
  {
    redis::client::string_vector first_two;
    c.zrevrange("{zset}1", 0, 1, first_two);
    ASSERT_EQUAL(first_two.size(), (size_t) 2);
    ASSERT_EQUAL(first_two[0], string("zval3") );
    ASSERT_EQUAL(first_two[1], string("zval1") );
    
    redis::client::string_vector last_two;
    c.zrevrange("{zset}1", -2, -1, last_two);
    ASSERT_EQUAL(last_two.size(), (size_t) 2);
    ASSERT_EQUAL(last_two[1], string("zval2") );
    ASSERT_EQUAL(last_two[0], string("zval1") );
  }
  
  test("zrangebyscore");
  {
    redis::client::string_vector between_1_and_2;
    c.zrangebyscore("{zset}1", 1.0, 2.0, between_1_and_2);
    ASSERT_EQUAL(between_1_and_2.size(), (size_t) 1);
    ASSERT_EQUAL(between_1_and_2[0], string("zval2") );

    redis::client::string_vector between_pi_and_4;
    c.zrangebyscore("{zset}1", 3.141, 4.0, between_pi_and_4);
    ASSERT_EQUAL(between_pi_and_4.size(), (size_t) 1);
    ASSERT_EQUAL(between_pi_and_4[0], string("zval3") );
    
    redis::client::string_vector between_pi_and_4_excluding;
    c.zrangebyscore("{zset}1", 3.141, 4.0, between_pi_and_4_excluding, 0, 0, redis::client::exclude_min);
    ASSERT_EQUAL(between_pi_and_4_excluding.size(), (size_t) 0);
    
    redis::client::string_vector between_3_and_pi;
    c.zrangebyscore("{zset}1", 3.0, 3.141, between_3_and_pi);
    ASSERT_EQUAL(between_3_and_pi.size(), (size_t) 1);
    ASSERT_EQUAL(between_3_and_pi[0], string("zval3") );
    
    redis::client::string_vector between_3_and_pi_excluding;
    c.zrangebyscore("{zset}1", 3.0, 3.141, between_pi_and_4_excluding, 0, 0, redis::client::exclude_max);
    ASSERT_EQUAL(between_pi_and_4_excluding.size(), (size_t) 0);

    redis::client::string_vector between_0_and_inf;
    c.zrangebyscore("{zset}1", 0.0, INFINITY, between_0_and_inf, 1);
    ASSERT_EQUAL(between_0_and_inf.size(), (size_t) 2);
    ASSERT_EQUAL(between_0_and_inf[0], string("zval1"));
    ASSERT_EQUAL(between_0_and_inf[1], string("zval3"));
    
    between_0_and_inf.clear();
    c.zrangebyscore("{zset}1", 0.0, INFINITY, between_0_and_inf, 0, 1);
    ASSERT_EQUAL(between_0_and_inf.size(), (size_t) 1);
    ASSERT_EQUAL(between_0_and_inf[0], string("zval2"));
  }

  test("zcount");
  {
    ASSERT_EQUAL(c.zcount("{zset}1", 0.0, INFINITY), 3L);
    ASSERT_EQUAL(c.zcount("{zset}1", 1.0, 2.0), 1L);
    
    ASSERT_EQUAL(c.zcount("{zset}1", 3.0, 3.141), 1L);
    ASSERT_EQUAL(c.zcount("{zset}1", 3.0, 3.141, redis::client::exclude_max), 0L);

    ASSERT_EQUAL(c.zcount("{zset}1", 3.141, 4.0), 1L);
    ASSERT_EQUAL(c.zcount("{zset}1", 3.141, 4.0, redis::client::exclude_min), 0L);
    
    ASSERT_EQUAL(c.zcount("{zset}1", 2.719, 3.141), 2L);
    ASSERT_EQUAL(c.zcount("{zset}1", 2.719, 3.141, redis::client::exclude_min), 1L);
    ASSERT_EQUAL(c.zcount("{zset}1", 2.719, 3.141, redis::client::exclude_max), 1L);
    ASSERT_EQUAL(c.zcount("{zset}1", 2.719, 3.141, redis::client::exclude_max | redis::client::exclude_min), 0L);
  }
  
  test("zremrangebyrank");
  {
    redis::client::int_type del_count = c.zremrangebyrank("{zset}1", 0, 1);
    ASSERT_EQUAL(del_count, 2L);
    ASSERT_EQUAL(c.zcard("{zset}1"), 1L);
    redis::client::string_vector content;
    c.zrange("{zset}1", 0, -1, content);
    ASSERT_EQUAL(content.size(), (size_t)1);
    ASSERT_EQUAL(content[0], string("zval3"));

    c.zadd("{zset}1", 2.719, "zval1");
    c.zadd("{zset}1", 1.234, "zval2");
  }

  test("zremrangebyscore");
  {
    redis::client::int_type del_count = c.zremrangebyscore("{zset}1", 0.0, 3.0);
    ASSERT_EQUAL(del_count, 2L);
    ASSERT_EQUAL(c.zcard("{zset}1"), 1L);
    redis::client::string_vector content;
    c.zrange("{zset}1", 0, -1, content);
    ASSERT_EQUAL(content.size(), (size_t)1);
    ASSERT_EQUAL(content[0], string("zval3"));
    
    c.zadd("{zset}1", 2.719, "zval1");
    c.zadd("{zset}1", 1.234, "zval2");
  }

  test("zcard");
  {
    ASSERT_EQUAL(c.zcard("{zset}1"), 3L);
  }
  
  test("zscore");
  {
    ASSERT_EQUAL(c.zscore("{zset}1", "zval1"), 2.719);
    ASSERT_EQUAL(c.zscore("{zset}1", "zval2"), 1.234);
    ASSERT_EQUAL(c.zscore("{zset}1", "zval3"), 3.141);
  }

  c.zadd("{zset}2", 1, "zval2");
  c.zadd("{zset}2", 2, "zval3");
  c.zadd("{zset}2", 3, "zval4");

  c.zadd("{zset}3", 4, "zval3");
  c.zadd("{zset}3", 5, "zval4");
  c.zadd("{zset}3", 6, "zval5");

  redis::client::string_vector sources;
  std::vector<double> weiths;
  sources.push_back("{zset}1");
  weiths.push_back(5);
  sources.push_back("{zset}2");
  weiths.push_back(3);
  sources.push_back("{zset}3");
  weiths.push_back(1);
  
  test("zunionstore");
  {
    c.zunionstore("{zset}4", sources);

    ASSERT_EQUAL(c.zcard("{zset}4"), 5L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval1"), 2.719);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval2"), 2.234);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 9.141);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval4"), 8.0);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval5"), 6.0);

    c.del("{zset}4");
    c.zunionstore("{zset}4", sources, std::vector<double>(), redis::client::aggregate_max);
    
    ASSERT_EQUAL(c.zcard("{zset}4"), 5L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval1"), 2.719);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval2"), 1.234);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 4.0);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval4"), 5.0);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval5"), 6.0);
    
    c.del("{zset}4");
    c.zunionstore("{zset}4", sources, weiths, redis::client::aggregate_max);

    ASSERT_EQUAL(c.zcard("{zset}4"), 5L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval1"), 2.719*5);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval2"), 1.234*5);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 3.141*5);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval4"), 3.0*3);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval5"), 6.0*1);
  }

  test("zinterstore");
  {
    c.del("{zset}4");
    c.zinterstore("{zset}4", sources);

    ASSERT_EQUAL(c.zcard("{zset}4"), 1L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 9.141);
    
    c.del("{zset}4");
    c.zinterstore("{zset}4", sources, std::vector<double>(), redis::client::aggregate_min);
    
    ASSERT_EQUAL(c.zcard("{zset}4"), 1L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 2.0);
    
    c.del("{zset}4");
    c.zinterstore("{zset}4", sources, weiths, redis::client::aggregate_min);
    
    ASSERT_EQUAL(c.zcard("{zset}4"), 1L);
    ASSERT_EQUAL(c.zscore("{zset}4", "zval3"), 4.0);
  }
}