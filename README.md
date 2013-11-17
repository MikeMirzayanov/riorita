# [Curator](https://github.com/MikeMirzayanov/curator)

Simple C++-server to store byte-arrays by key, uses leveldb as a backend

It is written using C++ on the top of boost::asio::io_service. Also contains Java client implementation.

## Protocol

Curator uses very simple request-response protocol. It supports keep-alive out-of-the box, a client should connect to the
server via TCP and send requests. The client receives response after each request.

### Requests

Each response returns at least two boolean fields: `success` and `verdict`, where `success` equals to 1
if and only if the request has been processed without unexpected errors and `verdict` equals to 0 or 1
and stands for the result of operation.

There three types of requests:

Type      | Description                           | Parameters                       |    Return value
----------|---------------------------------------|----------------------------------|-------------------------
`PING`    | Just ping a server to be sure that it is alive | No parameters           |    `verdict` is always 1
`HAS`     | Checks that server has value by given key | String `key`            |    `verdict` is 1 if has and 0 if has no
`GET`     | Returns value by given key | String `key`            |    `verdict` is 1 if the server contains value by key or 0 in opposite case
`PUT`     | Puts value by given key, overwrites existing data | String `key`, byte[] `value`            |    `verdict` is always 1
`DELETE`     | Deletes data by given key | String `key`            |    `verdict` is always 1
