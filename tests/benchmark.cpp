#include "functions.h"

#include "../redisclient.h"

void benchmark_mset(redis::client & c, int TEST_SIZE)
{
  block_duration b("Writing keys with mset", TEST_SIZE);
  redis::client::string_pair_vector keyValuePairs;
  for(int i=0; i < TEST_SIZE; i++)
  {
    stringstream ss;
    ss << "key_" << i;
    keyValuePairs.push_back( make_pair( ss.str(), boost::lexical_cast<string>(i) ) );
    
    if( keyValuePairs.size() == 250 )
    {
      c.mset( keyValuePairs );
      keyValuePairs.clear();
    }
  }
  c.mset( keyValuePairs );
  keyValuePairs.clear();
}

void benchmark_get(redis::client & c, int TEST_SIZE)
{
  block_duration b("Reading keys with GET", TEST_SIZE);
  redis::client::string_vector keys;
  for(int i=0; i < TEST_SIZE; i++)
  {
    stringstream ss;
    ss << "key_" << i;
    string val = c.get( ss.str() );
  }
}

void benchmark_set(redis::client & c, int TEST_SIZE)
{
  block_duration b("Reading keys with GET", TEST_SIZE);
  redis::client::string_vector keys;
  for(int i=0; i < TEST_SIZE; i++)
  {
    stringstream ss;
    ss << "key_" << i;
    c.set( ss.str(), boost::lexical_cast<string>(i) );
  }
}

void benchmark_mget(redis::client & c, int TEST_SIZE)
{
  block_duration b("Reading keys with MGET", TEST_SIZE);
  redis::client::string_vector keys;
  for(int i=0; i < TEST_SIZE; i++)
  {
    stringstream ss;
    ss << "key_" << i;
    keys.push_back( ss.str() );
    
    if( keys.size() == 250 )
    {
      redis::client::string_vector out;
      c.mget( keys, out );
      for(int i1=0; i1 < (int) keys.size(); i1++)
      {
        assert( boost::lexical_cast<int>( keys[i1].substr(4) ) == boost::lexical_cast<int>( out[i1] ) );
        //ASSERT_EQUAL( boost::lexical_cast<int>( keys[i1].substr(4) ), boost::lexical_cast<int>( out[i1] ) );
      }
      keys.clear();
    }
  }
  redis::client::string_vector out;
  c.mget( keys, out );
  for(int i1=0; i1 < (int) keys.size(); i1++)
  {
    assert( boost::lexical_cast<int>( keys[i1].substr(4) ) == boost::lexical_cast<int>( out[i1] ) );
    //ASSERT_EQUAL( boost::lexical_cast<int>( keys[i1].substr(4) ), boost::lexical_cast<int>( out[i1] ) );
  }
  keys.clear();
}

void benchmark_incr(redis::client & c, int TEST_SIZE)
{
  block_duration dur("Incrementing with shared_int", TEST_SIZE);
  
  // Increment
  for(int i=0; i < TEST_SIZE; i++)
  {
    stringstream ss;
    ss << "key_" << i;
    redis::shared_int sh_int(c, ss.str());
    
    ASSERT_EQUAL( (long) i+1, ++sh_int );
  }
}

void benchmark(redis::client & c, int TEST_SIZE)
{
  benchmark_set (c, TEST_SIZE);
  benchmark_mset(c, TEST_SIZE);
  benchmark_get (c, TEST_SIZE);
  benchmark_mget(c, TEST_SIZE);
  benchmark_incr(c, TEST_SIZE);
}
