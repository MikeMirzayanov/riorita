#include "protocol.h"
#include "storage.h"

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
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/program_options.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace expr = boost::log::expressions;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;
namespace po = boost::program_options;

using boost::asio::ip::tcp;
using namespace std;

const riorita::int32 MIN_VALID_REQUEST_SIZE = 15;
const riorita::int32 MAX_VALID_REQUEST_SIZE = 1073741824;

class Session;
typedef boost::shared_ptr<Session> SessionPtr;
set<SessionPtr> sessions;

riorita::Storage* storage;

long long currentTimeMillis()
{
    return (long long)(clock() / double(CLOCKS_PER_SEC) * 1000.0 + 0.5);
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

    BOOST_LOG_TRIVIAL(info)
         << "Processed " << riorita::toChars(request.type)
         << " in " << (currentTimeMillis() - startTimeMillis) << " ms,"
         << " returns " << success << ", " << verdict << ", " << data.length()
         << " [" << remoteAddr << ", id=" << request.id << "]";

    return newResponse(request, success, verdict,
            static_cast<riorita::int32>(data.length()),
            reinterpret_cast<const riorita::byte*>(data.c_str()));
}

class Session: public boost::enable_shared_from_this<Session>
{
public:
    virtual ~Session()
    {
        BOOST_LOG_TRIVIAL(info) << "Connection closed " << remoteAddr;

        response.reset();
        requestBytes.reset();

        if (null != request)
            delete[] request;
    }

    Session(boost::asio::io_service& io_service)
        : _socket(io_service), request(null)
    {
    }

    void onError()
    {
        BOOST_LOG_TRIVIAL(info) << "Ready to close " << remoteAddr;
        sessions.erase(shared_from_this());
    }

    tcp::socket& socket()
    {
        return _socket;
    }

    void start()
    {
        remoteAddr = boost::lexical_cast<std::string>(_socket.remote_endpoint());
        BOOST_LOG_TRIVIAL(info) << "New connection " << remoteAddr;
        sessions.insert(shared_from_this());
        boost::system::error_code error;
        handleStart(error);    
    }

    void handleStart(const boost::system::error_code& error)
    {
        if (!error)
        {
            boost::asio::async_read(
                _socket,
                boost::asio::buffer(&requestBytes.size, sizeof(requestBytes.size)),
                boost::bind(&Session::handleRead, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred)
            );
        }
        else
        {
            BOOST_LOG_TRIVIAL(info) << "error handleStart: " << remoteAddr;
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
            requestBytes.data = new riorita::byte[requestBytes.size];

            boost::asio::async_read(
                _socket,
                boost::asio::buffer(requestBytes.data, requestBytes.size),
                boost::bind(&Session::handleRequest, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred)
            );
        }
        else
        {
            BOOST_LOG_TRIVIAL(info)
                << "error handleRead: " << remoteAddr << ":"
                << " error=" << error
                << " bytes_transferred=" << bytes_transferred;
            ;
            
            onError();
        }
    }

    void handleRequest(const boost::system::error_code& error, std::size_t bytes_transferred)
    {
        if (!error && riorita::int32(bytes_transferred) == requestBytes.size)
        {
            riorita::int32 parsedByteCount;
            request = parseRequest(requestBytes, 0, parsedByteCount);

            if (request != null && parsedByteCount == requestBytes.size)
            {
                response = processRequest(remoteAddr, *request);

                boost::asio::async_write(
                    _socket,
                    boost::asio::buffer(response.data, response.size),
                    boost::bind(&Session::handleEnd, shared_from_this(), boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred)
                );
            }
            else
            {
                BOOST_LOG_TRIVIAL(info) << "Can't parse request: " << remoteAddr;
                
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
            BOOST_LOG_TRIVIAL(info) << "error handleRequest: " << remoteAddr;
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
            BOOST_LOG_TRIVIAL(info) << "error handleEnd: " << remoteAddr;
            onError();
        }
    }

private:
    tcp::socket _socket;

    riorita::Bytes requestBytes;
    riorita::Request* request;
    riorita::Bytes response;

    string remoteAddr;
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

void init(const string& logsDir, riorita::StorageType storageType)
{
    riorita::StorageOptions opts;
    opts.directory = "data";

    boost::filesystem::path logsPath(logsDir);
    logsPath /= "riorita_%N.log";

    storage = riorita::newStorage(storageType, opts);
    if (null == storage)
    {
        std::cerr << "Can't initialize storage\n";
        exit(1);
    }

    logging::add_common_attributes();

    logging::add_file_log
    (
        keywords::open_mode = std::ios::app,
        keywords::file_name = logsPath.string(),
        keywords::rotation_size = 32 * 1024 * 1024,
        keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0),
        keywords::format = "%Severity%: [%TimeStamp%]: %Message%"
    );

    logging::core::get()->set_filter
    (
        logging::trivial::severity >= logging::trivial::info
    );
}

int main(int argc, char* argv[])
{
    int port;
    
    {
        po::options_description description("=== riorita ===\nOptions");

        string logsDir;
        string backend;

        description.add_options()
            ("help", "Help message")
            ("logsDir", po::value<string>(&logsDir)->default_value("."), "Directory for logs")
            ("backend", po::value<string>(&backend)->default_value("leveldb"), "Backend: leveldb, files or memory")
            ("port", po::value<int>(&port)->default_value(8024), "Port")
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

        init(logsDir, type);
    }

    BOOST_LOG_TRIVIAL(info) << "Starting riorita server";

    try
    {
        boost::asio::io_service io_service;

        RioritaServerList servers;
        {
            BOOST_LOG_TRIVIAL(error) << "Listen port " << port;
            tcp::endpoint endpoint(tcp::v4(), short(port));
            RioritaServerPtr server(new RioritaServer(io_service, endpoint));
            servers.push_back(server);
        }

        BOOST_LOG_TRIVIAL(info) << "Started riorita server";
    
        io_service.run();
    }
    catch (std::exception& e)
    {
        BOOST_LOG_TRIVIAL(error) << "Exception: " << e.what();
        std::cerr << "Exception: " << e.what() << "\n";
        return 1;
    }
    catch(...)
    {
        BOOST_LOG_TRIVIAL(error) << "Unexpected exception";
        std::cerr << "Unexpected exception\n";
        return 1;
    }

    BOOST_LOG_TRIVIAL(info) << "Exited riorita server [exitCode=0]";
    return 0;
}
