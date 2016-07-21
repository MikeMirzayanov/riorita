#include "protocol.h"
#include "storage.h"
#include "logger.h"

#include <algorithm>
#include <cstdlib>
#include <vector>
#include <deque>
#include <iostream>
#include <list>
#include <ctime>
#include <map>
#include <set>

#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

using boost::asio::ip::tcp;
using namespace std;

const riorita::int32 MIN_VALID_REQUEST_SIZE = 15;
const riorita::int32 MAX_VALID_REQUEST_SIZE = 1073741824;

class Session;
typedef boost::shared_ptr<Session> SessionPtr;
set<SessionPtr> sessions;

boost::shared_ptr<riorita::Logger> lout;
boost::shared_ptr<riorita::Storage> storage;

static long long currentTimeMillis()
{
    return (long long)(clock() / double(CLOCKS_PER_SEC) * 1000.0 + 0.5);
}

static uint32_t string_address_to_uint32_t(const std::string& ip, bool& error)
{
    error = true;
    
    int a, b, c, d;
    uint32_t result = 0;
 
    if (sscanf(ip.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) != 4)
        return 0;

    if (a < 0 || a >= 256 || b < 0 || b >= 256
            || c < 0 || c >= 256 || d < 0 || d >= 256)
        return 0;
 
    result = a << 24;
    result |= b << 16;
    result |= c << 8;
    result |= d;
    
    error = false;
    return result;
}

static bool string_address_matches(const std::string& ip, std::string network)
{
    if (network.find("/") == string::npos)
        network += "/32";

    int pos = network.find("/");
    int bits;
    if (sscanf(network.substr(pos + 1).c_str(), "%d", &bits) != 1)
        return false;
    if (bits < 0 || bits > 32)
        return false;
    uint32_t network_mask = uint32_t(((1ULL << bits) - 1) << (32 - bits));

    bool error;
    uint32_t ip_addr = string_address_to_uint32_t(ip, error);
    if (error)
        return false;
    uint32_t network_addr = string_address_to_uint32_t(network.substr(0, pos), error);
    if (error)
        return false;

    return (ip_addr & network_mask) == network_addr;
}

riorita::Bytes processRequest(const string& remoteAddr, const riorita::Request& request)
{
    long long startTimeMillis = currentTimeMillis();

    bool success = true;
    bool verdict = false;
    string data;

    if (request.type == riorita::PING)
        verdict = true;

    string key(request.key.data, request.key.data + request.key.size);

    if (request.type == riorita::HAS)
        verdict = storage->has(key);

    if (request.type == riorita::GET)
        verdict = storage->get(key, data);

#undef DELETE
    if (request.type == riorita::DELETE)
    {
        storage->erase(key);
        verdict = true;
    }

    if (request.type == riorita::PUT)
    {
        string value(request.value.data, request.value.data + request.value.size);
        storage->put(key, value);
        verdict = true;
    }

    int size = max(int(data.length()), int(request.value.size));

    *lout
         << "Processed " << riorita::toChars(request.type)
         << " in " << (currentTimeMillis() - startTimeMillis) << " ms,"
         << " returns success=" << success << ", verdict=" << verdict << ", size=" << size
         << " [" << remoteAddr << ", id=" << request.id << "]"
         << endl;

    return newResponse(request, success, verdict,
            static_cast<riorita::int32>(data.length()),
            reinterpret_cast<const riorita::byte*>(data.c_str()));
}

class Session: public boost::enable_shared_from_this<Session>
{
public:
    virtual ~Session()
    {
        *lout << "Connection closed " << remoteAddr << endl;

        response.reset();
        requestBytes.reset();

        if (null != request)
            delete[] request;
    }

    Session(boost::asio::io_service& io_service)
        : _strand(io_service), _socket(io_service), request(null)
    {
    }

    void onError()
    {
        *lout << "Ready to close " << remoteAddr << endl;
        sessions.erase(shared_from_this());
    }

    tcp::socket& socket()
    {
        return _socket;
    }

    void start(const vector<string>& allowed_remote_addrs)
    {
        remoteAddr = boost::lexical_cast<std::string>(_socket.remote_endpoint());
        *lout << "Testing connection " << remoteAddr << endl;

        bool allowed = false;
        for (size_t i = 0; i < allowed_remote_addrs.size(); i++)
            if (string_address_matches(remoteAddr, allowed_remote_addrs[i]))
            {
                *lout << "Connection " << remoteAddr << " matches " << allowed_remote_addrs[i] << endl;
                allowed = true;
            }

        if (allowed)
        {
            *lout << "New connection " << remoteAddr << endl;
            sessions.insert(shared_from_this());
            boost::system::error_code error;
            handleStart(error);    
        }
        else
            *lout << "Denied " << remoteAddr << endl;
    }

    void handleStart(const boost::system::error_code& error)
    {
        if (!error)
        {
            boost::asio::async_read(
                _socket,
                boost::asio::buffer(&requestBytes.size, sizeof(requestBytes.size)),
                _strand.wrap(boost::bind(&Session::handleRead, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred))
            );
        }
        else
        {
            *lout << "error handleStart: " << remoteAddr << endl;
            onError();
        }
    }

    void handleRead(const boost::system::error_code& error, std::size_t bytes_transferred)
    {
        if (!error && bytes_transferred == sizeof(requestBytes.size)
                && requestBytes.size >= MIN_VALID_REQUEST_SIZE
                && requestBytes.size <= MAX_VALID_REQUEST_SIZE)
        {
            requestBytes.size -= sizeof(riorita::int32);

            long long startTimeMillis = currentTimeMillis();
            requestBytes.data = new riorita::byte[requestBytes.size];
            *lout
                 << "New bytes in " << (currentTimeMillis() - startTimeMillis) << " ms"
                 << ", size=" << requestBytes.size
                 << endl;

            boost::asio::async_read(
                _socket,
                boost::asio::buffer(requestBytes.data, requestBytes.size),
                _strand.wrap(boost::bind(&Session::handleRequest, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred))
            );
        }
        else
        {
            *lout
                << "error handleRead: " << remoteAddr << ":"
                << " error=" << error
                << " bytes_transferred=" << bytes_transferred
                << endl;
            ;
            
            onError();
        }
    }

    void handleRequest(const boost::system::error_code& error, std::size_t bytes_transferred)
    {
        if (!error && riorita::int32(bytes_transferred) == requestBytes.size)
        {
            riorita::int32 parsedByteCount;

            long long startTimeMillis = currentTimeMillis();
            request = parseRequest(requestBytes, 0, parsedByteCount);
            *lout
                 << "Parsed " << riorita::toChars(request->type)
                 << " in " << (currentTimeMillis() - startTimeMillis) << " ms"
                 << ", size=" << requestBytes.size
                 << " [" << remoteAddr << ", id=" << request->id << "]"
                 << endl;

            if (request != null && parsedByteCount == requestBytes.size)
            {
                response = processRequest(remoteAddr, *request);

                *lout
                     << "Ready to async_write " << riorita::toChars(request->type)
                     << " in " << (currentTimeMillis() - startTimeMillis) << " ms"
                     << ", size=" << requestBytes.size
                     << " [" << remoteAddr << ", id=" << request->id << "]"
                     << endl;

                boost::asio::async_write(
                    _socket,
                    boost::asio::buffer(response.data, response.size),
                    _strand.wrap(boost::bind(&Session::handleEnd, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred))
                );
            }
            else
            {
                *lout << "Can't parse request: " << remoteAddr << endl;
                
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
            *lout << "error handleRequest: " << remoteAddr << endl;
            onError();
        }
    }

    void handleEnd(const boost::system::error_code& error, std::size_t bytes_transferred)
    {
        std::size_t responseSize = response.size;

        requestBytes.reset();
        response.reset();

        if (!error && bytes_transferred == responseSize)
        {
            handleStart(error);
        }
        else
        {
            *lout << "error handleEnd: " << remoteAddr << endl;
            onError();
        }
    }

private:
    boost::asio::io_service::strand _strand;
    tcp::socket _socket;

    riorita::Bytes requestBytes;
    riorita::Request* request;
    riorita::Bytes response;

    string remoteAddr;
};

//----------------------------------------------------------------------

SessionPtr session;

class RioritaServer
{
public:
    RioritaServer(boost::asio::io_service& io_service,
        const tcp::endpoint& endpoint, const string& allowedRemoteAddrs)
        : io_service_(io_service),
        acceptor_(io_service)
    {
        allowed_remote_addrs_.clear();
        string remote_addr;
        for (size_t i = 0; i < allowedRemoteAddrs.length(); i++)
            if (allowedRemoteAddrs[i] != ';')
                remote_addr += allowedRemoteAddrs[i];
            else if (!remote_addr.empty())
            {
                allowed_remote_addrs_.push_back(remote_addr);
                remote_addr = "";
            }
        if (!remote_addr.empty())
            allowed_remote_addrs_.push_back(remote_addr);

        *lout << "Allowed size: " << allowed_remote_addrs_.size() << endl;
        for (size_t i = 0; i < allowed_remote_addrs_.size(); i++)
            *lout << "Allowed from: " << allowed_remote_addrs_[i] << endl;

        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();

        startAccept();
    }

    void startAccept()
    {
        *lout << "startAccept" << endl;
        session.reset(new Session(io_service_));
        acceptor_.async_accept(session->socket(),
            boost::bind(&RioritaServer::handleAccept, this,
            boost::asio::placeholders::error));
    }

    void handleAccept(const boost::system::error_code& error)
    {
        if (!error)
            session->start(allowed_remote_addrs_);

        startAccept();
    }

private:
    boost::asio::io_service& io_service_;
    tcp::acceptor acceptor_;
    vector<string> allowed_remote_addrs_;
};

typedef boost::shared_ptr<RioritaServer> RioritaServerPtr;
typedef std::list<RioritaServerPtr> RioritaServerList;

//----------------------------------------------------------------------

void init(const string& logFile, const string& dataDir, riorita::StorageType storageType)
{
    lout = boost::shared_ptr<riorita::Logger>(new riorita::Logger(logFile));

    riorita::StorageOptions opts;
    opts.directory = dataDir;

    storage = boost::shared_ptr<riorita::Storage>(riorita::newStorage(storageType, opts));
    if (null == storage)
    {
        std::cerr << "Can't initialize storage" << std::endl;
        exit(1);
    }
}

#ifdef HAS_ROCKSDB
    const string DEFAULT_BACKEND = "rocksdb";
#elif HAS_LEVELDB
    const string DEFAULT_BACKEND = "leveldb";
#else
    const string DEFAULT_BACKEND = "compact";
#endif

boost::asio::io_service io_service(4);

int main(int argc, char* argv[])
{
    int port;
    string allowedRemoteAddrs;
    
    {
        po::options_description description("=== riorita ===\nOptions");

        string logFile;
        string dataDir;
        string backend;

        description.add_options()
            ("help", "Help message")
            ("log", po::value<string>(&logFile)->default_value("riorita.log"), "Log file")
            ("data", po::value<string>(&dataDir)->default_value("data"), "Data directory")
            ("backend", po::value<string>(&backend)->default_value(DEFAULT_BACKEND), "Backend: rocksdb, leveldb, files, compact or memory")
            ("port", po::value<int>(&port)->default_value(8024), "Port")
            ("allowed", po::value<string>(&allowedRemoteAddrs)->default_value("0.0.0.0;127.0.0.1"), "Allows remote addresses: example '212.193.32.0/19;0.0.0.0;127.0.0.1'")
        ;

        po::variables_map varmap;
        po::store(po::parse_command_line(argc, argv, description), varmap);
        po::notify(varmap);
        
        if (varmap.count("help"))
        {
            std::cout << description << std::endl;
            return 1;
        }

        riorita::StorageType type = riorita::getType(backend);
        if (type == riorita::ILLEGAL_STORAGE_TYPE)
        {
            std::cout << description << std::endl;
            return 1;
        }

        init(logFile, dataDir, type);
    }

    *lout << "Starting riorita server" << endl;

    try
    {
        RioritaServerList servers;
        {
            *lout << "Listen port " << port << endl;
            tcp::endpoint endpoint(tcp::v4(), short(port));
            RioritaServerPtr server(new RioritaServer(io_service, endpoint, allowedRemoteAddrs));
            servers.push_back(server);
        }

        boost::asio::signal_set signals_(io_service);
        signals_.add(SIGINT);
        signals_.add(SIGTERM);
#if defined(SIGQUIT)
        signals_.add(SIGQUIT);
#endif // defined(SIGQUIT)
        signals_.async_wait(boost::bind(&boost::asio::io_service::stop, &io_service));


        *lout << "Started riorita server" << endl;
    
        std::vector<boost::shared_ptr<boost::thread> > threads;
        for (std::size_t i = 0; i < 4; ++i)
        {
          boost::shared_ptr<boost::thread> thread(new boost::thread(
                boost::bind(&boost::asio::io_service::run, &io_service)));
          threads.push_back(thread);
        }

        // io_service.run();
        
        for (std::size_t i = 0; i < threads.size(); ++i)
          threads[i]->join();
    }
    catch (std::exception& e)
    {
        *lout << "Exception: " << e.what() << endl;
        std::cerr << "Exception: " << e.what() << endl;
        return 1;
    }
    catch(...)
    {
        *lout << "Unexpected exception" << endl;
        std::cerr << "Unexpected exception" << endl;
        return 1;
    }

    *lout << "Exited riorita server [exitCode=0]" << endl;
    return 0;
}
