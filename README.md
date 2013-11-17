# [Curator](https://github.com/MikeMirzayanov/curator)

Simple C++-server to store byte-arrays by key, uses leveldb as a backend

It is written using C++ on the top of boost::asio::io_service. Also contains Java client implementation.

## Protocol

Curator uses very simple binary request-response protocol. It supports keep-alive out-of-the box, a client should connect to the
server via TCP and send requests. The client receives response after each request.

Server closes connection on any error, client can reopen connection.

Protocol uses LITTLE_ENDIAN byte order.

### Requests

There three types of requests:

Type       | Byte value | Description                           | Parameters                       |    Return value
-----------|------|---------------------------------------|----------------------------------|-------------------------
`PING`     |  1 | Just ping a server to be sure that it is alive | No parameters           |    verdict is always 1
`HAS`      |  2 | Checks that server has value by given key | String key            |    verdict is 1 if has and 0 if has no
`GET`      |  3 | Returns value by given key | String key            |    verdict is 1 if the server contains value by key or 0 in opposite case
`PUT`      |  4 | Puts value by given key, overwrites existing data | String key, byte[] value            |    verdict is always 1
`DELETE`   |  5 | Deletes data by given key | String key            |    verdict is always 1

Each request has a form:

`<total-request-size:4><magic-byte:1><protocol-version:1><request-type-byte-value:1><request-id:8>` + `<key-length:4><key-data:key-length>`

where `:x` stands for the length of a field in bytes. Operator `+` stands for simple concatenation and used just to break long line.

Also if type is `PUT` the request is appended with:

`<value-length:4><value-data:value-length>`

For `PING` request the key should be empty (key-length=0).

### Responses

Each response returns at least one boolean field: success, where success equals to 1
if and only if the request has been processed without unexpected errors. If success=1 then
a response contains verdict (equals to 0 or 1), it stands for the result of operation.

Each response has a form:

`<total-response-size:4><magic-byte:1><protocol-version:1><request-id:8><success:1>`

If success=1 the response is appended with:

`<verdict:1>`

If request type was `GET` and verdict=1 then the response is appended with:

`<value-length:4><value-data:value-length>`
