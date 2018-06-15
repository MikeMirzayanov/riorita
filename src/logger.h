#ifndef RIORITA_LOGGER_H_
#define RIORITA_LOGGER_H_

#include <fstream>
#include <string>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <boost/thread/mutex.hpp>

namespace riorita {

class Logger
{
public:
    Logger(const std::string& fileName): newLine(true)
    {
        ofs.open(fileName.c_str(), std::ios_base::app);
        ofs.imbue(std::locale(ofs.getloc(), new boost::posix_time::time_facet("%Y-%b-%d %H:%M:%S.%f")));
    }

    template<typename T>
    Logger& operator << (const T& o)
    {
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        if (newLine)
            ofs << boost::posix_time::microsec_clock::local_time() << ": ",
            newLine = false;
        ofs << o;
        return *this;
    }

    typedef std::ostream& (*ostream_manipulator)(std::ostream&);
    Logger& operator << (ostream_manipulator pf)
    {
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        ofs << pf << std::flush;
        newLine = true;
        return *this;
    }

    ~Logger()
    {
        ofs.close();
    }

private:
    std::ofstream ofs; 
    bool newLine;
    boost::mutex mutex;
};

}

#endif