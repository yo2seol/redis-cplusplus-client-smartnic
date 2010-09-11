
command handlers:
- Cluster mode: Emulate multi-key commands like SDIFF/SDIFFSTORE locally if they are on different servers.
                Currently all multi-key commands fail if the consistent hasher maps them to different servers.
- support DEL as vararg
- support MLLEN and MSCARD

unit tests:
- sort with limit
- sort lexicographically
- sort with pattern and weights

extras:
- streaming versions for multi-bulk commands (and maybe also for GET and SET for better support of very large values)
- benchmarking "test" app
- switch networking code to boost::asio with non serial access of different cluster nodes
- merge duplicate code

maybe/someday:
- make all string literals constants so they can be easily changed
- add conveniences that store a std::set in its entirety (same for std::list, std::vector)
