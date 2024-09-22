#ifndef RIORITA_LOGGER_H_
#define RIORITA_LOGGER_H_

#include <fstream>
#include <string>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <boost/thread/mutex.hpp>
#include <cstdlib>

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

    // Fatal logging function
    template<typename T>
    void fatal(const T& message)
    {
        // Log the fatal message
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        if (newLine)
            ofs << boost::posix_time::microsec_clock::local_time() << ": ",
            newLine = false;
        ofs << "FATAL: " << message << std::endl;
        ofs.flush();
        std::exit(1);  // Exit the program with code 1
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
