#pragma once

// Final rows already contain portable Roaring bitmaps and exact bitmap groups.
// Use a fixed run-length deflate strategy for their existing zlib envelope.
// Peer matrix/Biclique encoding and every decoder remain unchanged.
static string WrapIncrementalFinalPayload(const string& raw) {
    z_stream stream{};
    if (deflateInit2(&stream, Z_BEST_SPEED, Z_DEFLATED, MAX_WBITS,
            8, Z_RLE) != Z_OK)
        throw runtime_error("final deflate initialization failed");
    struct DeflateOwner {
        z_stream* stream;
        ~DeflateOwner() { deflateEnd(stream); }
    } cleanup{&stream};
    string compressed(static_cast<size_t>(deflateBound(&stream,
        static_cast<uLong>(raw.size()))), char(0));
    size_t supplied = 0, produced = 0;
    int result = Z_OK;
    while (result == Z_OK) {
        if (stream.avail_in == 0 && supplied < raw.size()) {
            const auto count = static_cast<uInt>(min<size_t>(raw.size() - supplied,
                numeric_limits<uInt>::max()));
            stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(raw.data() + supplied));
            stream.avail_in = count;
            supplied += count;
        }
        if (stream.avail_out == 0) {
            if (produced == compressed.size())
                throw runtime_error("final deflate exceeded its bound");
            stream.next_out = reinterpret_cast<Bytef*>(compressed.data() + produced);
            stream.avail_out = static_cast<uInt>(min<size_t>(compressed.size() - produced,
                numeric_limits<uInt>::max()));
        }
        const uInt available = stream.avail_out;
        result = deflate(&stream, supplied == raw.size() ? Z_FINISH : Z_NO_FLUSH);
        produced += available - stream.avail_out;
    }
    if (result != Z_STREAM_END) throw runtime_error("final deflate failed");
    const bool use_compressed = produced < raw.size();
    compressed.resize(produced);
    string out;
    out.reserve(20 + (use_compressed ? produced : raw.size()));
    AppendRawU32(out, kCompressedPayloadMagic);
    AppendRawU32(out, kCompressedPayloadVersion);
    AppendRawU32(out, use_compressed ? 1u : 0u);
    AppendRawU64(out, raw.size());
    out.append(use_compressed ? compressed : raw);
    return out;
}
