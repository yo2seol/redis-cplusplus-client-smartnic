# A C++ client for Redis 

- Depends on c++ boost library
- It uses anet from Redis itself
- It has been tested with g++ on Linux (and older versions also on Mac OS X 10.5)
- Consistent hashing (with support for user-specified algorithm),

## Status

This client is based on the initial release of a redis c++ client from http://github.com/fictorial/redis-cplusplus-client.
It was changed to work with Redis 1.2. 
Please note, that this software has an experimental state.

API:
It is not guaranteed that the api from the initial 'fictorial' release will be continued. Generally the current api is not
stable and not well documented. Please have a look at the provided test cases (test_client.cpp) to see what is currently possible.

Any kind of feedback, suggestions and bug reports is very welcome.

## License

This client is licensed under the same license as redis. 

## Authors

- Brian Hammond <brian at fictorial dot com>   (intial 'fictorial' release)
- Ludger Sprenker <ludger at sprenker dot net> ('mrpi' release: extensions and changes for redis versions greater 1.1)
