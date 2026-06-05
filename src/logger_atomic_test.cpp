#include "logger.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

int countOccurrences(const std::string& text, const std::string& needle)
{
    int count = 0;
    std::string::size_type position = 0;
    while ((position = text.find(needle, position)) != std::string::npos)
    {
        ++count;
        position += needle.size();
    }
    return count;
}

}

int main()
{
    const std::string logPath = "/tmp/riorita-logger-atomic-test.log";
    std::remove(logPath.c_str());

    const int threadCount = 8;
    const int linesPerThread = 200;
    std::atomic<bool> start(false);

    {
        riorita::Logger logger(logPath);
        std::vector<std::thread> threads;
        for (int threadId = 0; threadId < threadCount; ++threadId)
        {
            threads.emplace_back([&logger, &start, threadId]() {
                while (!start.load(std::memory_order_acquire))
                    std::this_thread::yield();

                const std::string payload(64, static_cast<char>('A' + threadId));
                for (int line = 0; line < linesPerThread; ++line)
                    logger << "thread=" << threadId << " line=" << line << " payload=" << payload << std::endl;
            });
        }

        start.store(true, std::memory_order_release);
        for (std::thread& thread: threads)
            thread.join();
    }

    std::ifstream input(logPath.c_str());
    if (!input)
    {
        std::cerr << "failed to open log file" << std::endl;
        return 1;
    }

    int lineCount = 0;
    std::string line;
    while (std::getline(input, line))
    {
        ++lineCount;
        if (countOccurrences(line, "thread=") != 1
            || countOccurrences(line, " line=") != 1
            || countOccurrences(line, " payload=") != 1)
        {
            std::cerr << "interleaved or malformed log line: " << line << std::endl;
            return 1;
        }
    }

    const int expectedLineCount = threadCount * linesPerThread;
    if (lineCount != expectedLineCount)
    {
        std::cerr << "expected " << expectedLineCount << " lines, got " << lineCount << std::endl;
        return 1;
    }

    return 0;
}
