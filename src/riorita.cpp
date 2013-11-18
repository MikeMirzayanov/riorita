#include "protocol.h"

#include <algorithm>
#include <cstdlib>
#include <vector>
#include <deque>
#include <iostream>
#include <list>
#include <map>
#include <set>

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;
using namespace std;

class Session;
typedef boost::shared_ptr<Session> SessionPtr;
set<SessionPtr> sessions;

map<string, string> storage;

bool storageHas(const string& key)
{
//    cout << "has '" << key << "'" << endl;
    return storage.count(key) != 0;
}

string storageGet(const string& key)
{
//    cout << "get '" << key << "'" << endl;
    return storage[key];
}

void storageDelete(const string& key)
{
//    cout << "delete '" << key << "'" << endl;
    storage.erase(key);
}

void storagePut(const string& key, const string& value)
{
//    cout << "put '" << key << "' '" << value << "'" << endl;
    storage[key] = value;
}

riorita::byte* processRequest(const riorita::Request& request, bool& success, bool& verdict, riorita::int32& dataLength)
{
    success = true;
    verdict = false;
    dataLength = 0;
    riorita::byte* result = null;

    if (request.type == riorita::PING)
    {
        verdict = true;
        return result;
    }

    string key(request.key.data, request.key.data + request.key.size);

    if (request.type == riorita::HAS || request.type == riorita::GET)
        verdict = storageHas(key);

    if (request.type == riorita::GET && verdict)
    {
        string data = storageGet(key);
        result = new riorita::byte[data.length()];
        memcpy(result, data.c_str(), data.length());
        dataLength = data.length();
    }

#undef DELETE
    if (request.type == riorita::DELETE)
    {
        storageDelete(key);
        verdict = true;
    }

    if (request.type == riorita::PUT)
    {
        string value(request.value.data, request.value.data + request.value.size);
        storagePut(key, value);
        verdict = true;
    }

    return result;
}

class Session: public boost::enable_shared_from_this<Session>
{
public:
    virtual ~Session()
    {
        cout << "Connection closed." << endl;

        response.reset();
        requestBytes.reset();

        if (null != request)
            delete[] request;
    }

    Session(boost::asio::io_service& io_service)
        : _socket(io_service)
    {
    }

    void onError()
    {
        cout << "Ready to close" << endl;
        sessions.erase(shared_from_this());
    }

    tcp::socket& socket()
    {
        return _socket;
    }

    void start()
    {
        cout << "New connection" << endl;
        sessions.insert(shared_from_this());
        boost::system::error_code error;
        handleStart(error);    
    }

    void handleStart(const boost::system::error_code& error)
    {
        if (!error)
        {
            // cout << "Started to read request size" << endl;

            boost::asio::async_read(
                _socket,
                boost::asio::buffer(&requestBytes.size, sizeof(requestBytes.size)),
                boost::bind(&Session::handleRead, shared_from_this(), boost::asio::placeholders::error)
            );
        }
        else
        {
            cout << "error handleStart" << endl;
            onError();
        }
    }

    void handleRead(const boost::system::error_code& error)
    {
        if (!error)
        {
            requestBytes.size -= sizeof(riorita::int32);
            // cout << "Request size is " << requestBytes.size << endl;
            requestBytes.data = new riorita::byte[requestBytes.size];

            // cout << "Started to read request body" << endl;
            boost::asio::async_read(
                _socket,
                boost::asio::buffer(requestBytes.data, requestBytes.size),
                boost::bind(&Session::handleRequest, shared_from_this(), boost::asio::placeholders::error)
            );
        }
        else
        {
            cout << "error handleRead" << endl;
            onError();
        }
    }

    void handleRequest(const boost::system::error_code& error)
    {
        if (!error)
        {
            // cout << "Ready to parse request" << endl;

            riorita::int32 parsedByteCount;
            request = parseRequest(requestBytes, 0, parsedByteCount);

            if (request != null && parsedByteCount == requestBytes.size)
            {
                // cout << "Parsed: " << request->id << endl;

                bool success, verdict;
                
                riorita::Bytes responseData;
                responseData.data = processRequest(*request, success, verdict, responseData.size);

                response = riorita::newResponseHeader(*request, success, verdict, responseData);
                responseData.reset();

                // cout << "Ready to send: " << verdict << endl;

                boost::asio::async_write(
                    _socket,
                    boost::asio::buffer(response.data, response.size),
                    boost::bind(&Session::handleEnd, shared_from_this(), boost::asio::placeholders::error)
                );
            }
            else
            {
                cout << "Can't parse request" << endl;
                onError();
            }

            if (null != request)
            {
                delete request;
                request = null;
            }
        }
        else
        {
            cout << "error handleRequest" << endl;
            onError();
        }
    }

    void handleEnd(const boost::system::error_code& error)
    {
        requestBytes.reset();
        response.reset();

        if (!error)
        {
            handleStart(error);
        }
        else
        {
            cout << "error handleEnd" << endl;
            onError();
        }
    }

private:
    tcp::socket _socket;

    riorita::Bytes requestBytes;
    riorita::Request* request;
    riorita::Bytes response;
};

//----------------------------------------------------------------------

class RioritaServer
{
public:
    RioritaServer(boost::asio::io_service& io_service,
        const tcp::endpoint& endpoint)
        : io_service_(io_service),
        acceptor_(io_service, endpoint)
    {
        startAccept();
    }

    void startAccept()
    {
        SessionPtr newSession(new Session(io_service_));
        acceptor_.async_accept(newSession->socket(),
            boost::bind(&RioritaServer::handleAccept, this, newSession,
            boost::asio::placeholders::error));
    }

    void handleAccept(SessionPtr Session,
        const boost::system::error_code& error)
    {
        if (!error)
        {
            Session->start();
        }

        startAccept();
    }

private:
    boost::asio::io_service& io_service_;
    tcp::acceptor acceptor_;
};

typedef boost::shared_ptr<RioritaServer> RioritaServerPtr;
typedef std::list<RioritaServerPtr> RioritaServerList;

//----------------------------------------------------------------------

void init()
{
}

int main(int argc, char* argv[])
{
    init();

    try
    {
        if (argc < 2)
        {
            std::cerr << "Usage: riorita_server <port> [<port> ...]\n";
            return 1;
        }

        boost::asio::io_service io_service;

        RioritaServerList servers;
        for (int i = 1; i < argc; ++i)
        {
            tcp::endpoint endpoint(tcp::v4(), atoi(argv[i]));
            RioritaServerPtr server(new RioritaServer(io_service, endpoint));
            servers.push_back(server);
        }

        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
