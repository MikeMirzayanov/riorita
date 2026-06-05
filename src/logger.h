#ifndef RIORITA_LOGGER_H_
#define RIORITA_LOGGER_H_

#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <boost/thread/mutex.hpp>
#include <cstdlib>

namespace riorita {

class Logger
{
public:
    Logger(const std::string& fileName)
    {
        ofs.open(fileName.c_str(), std::ios_base::app);
        ofs.imbue(std::locale(ofs.getloc(), new boost::posix_time::time_facet("%Y-%b-%d %H:%M:%S.%f")));
    }

    typedef std::ostream& (*ostream_manipulator)(std::ostream&);

    class Line
    {
    public:
        explicit Line(Logger& logger):
            logger(logger),
            timestamp(boost::posix_time::microsec_clock::local_time()),
            finished(false)
        {
        }

        Line(const Line&) = delete;
        Line& operator = (const Line&) = delete;

        Line(Line&& other):
            logger(other.logger),
            timestamp(other.timestamp),
            stream(std::move(other.stream)),
            finished(other.finished)
        {
            other.finished = true;
        }

        ~Line()
        {
            if (!finished)
                flush(false);
        }

        template<typename T>
        Line& operator << (const T& o)
        {
            stream << o;
            return *this;
        }

        Logger& operator << (ostream_manipulator pf)
        {
            stream << pf;
            flush(isFlushManipulator(pf));
            return logger;
        }

    private:
        Logger& logger;
        boost::posix_time::ptime timestamp;
        std::ostringstream stream;
        bool finished;

        static bool isFlushManipulator(ostream_manipulator pf)
        {
            return pf == static_cast<ostream_manipulator>(std::flush);
        }

        void flush(bool flushOutput)
        {
            if (finished)
                return;
            logger.write(timestamp, stream.str(), flushOutput);
            finished = true;
        }
    };

    template<typename T>
    Line operator << (const T& o)
    {
        Line line(*this);
        line << o;
        return line;
    }

    Logger& operator << (ostream_manipulator pf)
    {
        Line line(*this);
        return line << pf;
    }

    // Fatal logging function
    template<typename T>
    void fatal(const T& message)
    {
        // Log the fatal message
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        ofs << boost::posix_time::microsec_clock::local_time() << ": FATAL: " << message << '\n';
        ofs.flush();
        std::exit(1);  // Exit the program with code 1
    }
    
    ~Logger()
    {
        ofs.close();
    }

private:
    void write(const boost::posix_time::ptime& timestamp, const std::string& text, bool flushOutput)
    {
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        if (!text.empty())
            ofs << timestamp << ": " << text;
        if (flushOutput)
            ofs.flush();
    }

    std::ofstream ofs; 
    boost::mutex mutex;
};

}

#endif
