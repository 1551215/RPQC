#pragma once

#include <algorithm>
#include <iostream>
#include <mutex>
#include <streambuf>
#include <string>

// Keep complete lines in memory during query work. Existing terminal status
// and result RPCs drain them synchronously, inside coordinator query timing.
class DistributedLineLogBuffer : public std::streambuf {
    std::streambuf* sink_;
    std::mutex sink_mu_;
    std::mutex pending_mu_;
    std::string pending_;
    bool query_buffering_ = false;

    static std::string& Pending() {
        static thread_local std::string line;
        return line;
    }

    int Flush() {
        auto& line = Pending();
        if (line.empty()) return 0;
        bool drain = false;
        {
            std::lock_guard<std::mutex> lock(pending_mu_);
            pending_.append(line);
            line.clear();
            drain = !query_buffering_;
        }
        return drain ? Drain() : 0;
    }

protected:
    std::streamsize xsputn(const char* data, std::streamsize size) override {
        std::streamsize consumed = 0;
        while (consumed < size) {
            const char* begin = data + consumed;
            const char* end = data + size;
            const char* newline = std::find(begin, end, '\n');
            const auto count = static_cast<std::streamsize>(
                (newline == end ? end : newline + 1) - begin);
            Pending().append(begin, static_cast<size_t>(count));
            consumed += count;
            if (newline != end && Flush() != 0) return consumed - count;
        }
        return consumed;
    }

    int_type overflow(int_type value) override {
        if (traits_type::eq_int_type(value, traits_type::eof()))
            return traits_type::not_eof(value);
        const char character = traits_type::to_char_type(value);
        Pending().push_back(character);
        if (character == '\n' && Flush() != 0) return traits_type::eof();
        return value;
    }

    int sync() override { return Flush(); }

public:
    explicit DistributedLineLogBuffer(std::streambuf* sink) : sink_(sink) {}

    void BeginQuery() {
        std::lock_guard<std::mutex> lock(pending_mu_);
        query_buffering_ = true;
    }

    int Drain() {
        // Writers only take pending_mu_; shared-disk waits hold no log append
        // lock and no query-state lock. Serialize drains to preserve order.
        std::lock_guard<std::mutex> sink_lock(sink_mu_);
        std::string batch;
        {
            std::lock_guard<std::mutex> lock(pending_mu_);
            batch.swap(pending_);
        }
        if (batch.empty()) return 0;
        const auto size = static_cast<std::streamsize>(batch.size());
        const auto written = sink_->sputn(batch.data(), size);
        if (written != size) {
            std::lock_guard<std::mutex> lock(pending_mu_);
            pending_.insert(0, batch, static_cast<size_t>(std::max<std::streamsize>(0, written)),
                std::string::npos);
            return -1;
        }
        return sink_->pubsync();
    }
};

class ScopedDistributedLineLogging {
    std::streambuf* original_ = std::cerr.rdbuf();
    std::ostream* original_tie_ = std::cerr.tie();
    const bool original_unitbuf_ = (std::cerr.flags() & std::ios_base::unitbuf) != 0;
    DistributedLineLogBuffer buffer_{original_};

public:
    ScopedDistributedLineLogging() {
        std::cerr.unsetf(std::ios_base::unitbuf);
        std::cerr.tie(nullptr);
        std::cerr.rdbuf(&buffer_);
    }
    ~ScopedDistributedLineLogging() {
        // Restore the stream before main returns and thread-local strings are
        // destroyed, including startup failures before the RPC server runs.
        std::cerr.flush();
        buffer_.Drain();
        std::cerr.rdbuf(original_);
        std::cerr.tie(original_tie_);
        if (original_unitbuf_) std::cerr.setf(std::ios_base::unitbuf);
    }
    ScopedDistributedLineLogging(const ScopedDistributedLineLogging&) = delete;
    ScopedDistributedLineLogging& operator=(const ScopedDistributedLineLogging&) = delete;

    void BeginQuery() { buffer_.BeginQuery(); }
    void Checkpoint() {
        std::cerr.flush();
        if (buffer_.Drain() != 0) throw std::ios_base::failure("distributed log write failed");
    }
};
