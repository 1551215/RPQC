#include "shared_common.inc"

enum class IncrementalMacroKind : uint8_t {
    LEAF = 0,
    SEQUENCE = 1,
    CHOICE = 2,
    REPETITION = 3,
};

struct IncrementalMacroNode {
    IncrementalMacroKind kind = IncrementalMacroKind::LEAF;
    int left = -1;
    int right = -1;
    int body_id = -1;
    int min_rep = 0;
    int max_rep = 0;  // -1 means open range
};

struct IncrementalProgram {
    vector<QuerySpec> bodies;
    // -1 denotes an ordinary graph-regex body. A nonnegative entry denotes a
    // virtual body whose length-one rows are produced by that macro AST node.
    vector<int> virtual_body_nodes;
    vector<IncrementalMacroNode> nodes;
    int root = -1;
};

static bool IncrementalProgramHasOpenRange(const IncrementalProgram& program) {
    for (const auto& node : program.nodes) {
        if ((node.kind == IncrementalMacroKind::LEAF ||
             node.kind == IncrementalMacroKind::REPETITION) &&
            node.max_rep < 0) {
            return true;
        }
    }
    return false;
}

static bool ParseIncrementalRangeSuffix(
    const string& expression, string& body, int& min_rep, int& max_rep,
    string& err) {
    if (expression.empty() || expression.back() != '}') return false;
    int depth = 0;
    size_t lbrace = string::npos;
    for (size_t cursor = expression.size(); cursor-- > 0;) {
        const char c = expression[cursor];
        if (c == '}') {
            ++depth;
        } else if (c == '{') {
            if (--depth == 0) {
                lbrace = cursor;
                break;
            }
            if (depth < 0) {
                err = "Unbalanced '{'";
                return false;
            }
        }
    }
    if (lbrace == string::npos || depth != 0 || lbrace == 0) return false;
    body = expression.substr(0, lbrace);
    const string range = expression.substr(
        lbrace + 1, expression.size() - lbrace - 2);
    const size_t comma = range.find(',');
    const string lower = comma == string::npos ? range : range.substr(0, comma);
    const string upper = comma == string::npos ? range : range.substr(comma + 1);
    if (lower.empty() ||
        !all_of(lower.begin(), lower.end(), [](unsigned char c) {
            return isdigit(c);
        }) ||
        (comma != string::npos && !upper.empty() &&
         !all_of(upper.begin(), upper.end(), [](unsigned char c) {
             return isdigit(c);
         }))) {
        err = "Invalid repetition range";
        return false;
    }
    min_rep = stoi(lower);
    max_rep = comma == string::npos
        ? min_rep : (upper.empty() ? -1 : stoi(upper));
    if (max_rep >= 0 && min_rep > max_rep) {
        err = "Invalid repetition range";
        return false;
    }
    return true;
}

static bool IsWholeParenthesized(const string& text) {
    if (text.size() < 2 || text.front() != '(' || text.back() != ')') return false;
    int depth = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '(') ++depth;
        else if (text[i] == ')' && --depth == 0 && i + 1 != text.size()) return false;
    }
    return depth == 0;
}

static bool SplitLastTopLevelMacroOp(const string& expr, char& op, string& lhs, string& rhs, string& err) {
    int paren = 0;
    int brace = 0;
    size_t pos = string::npos;
    for (size_t i = 0; i < expr.size(); ++i) {
        const char c = expr[i];
        if (c == '(') { ++paren; continue; }
        if (c == ')') { if (--paren < 0) { err = "Unbalanced ')'"; return false; } continue; }
        if (c == '{') { ++brace; continue; }
        if (c == '}') { if (--brace < 0) { err = "Unbalanced '}'"; return false; } continue; }
        if (paren == 0 && brace == 0 && (c == '/' || c == '|')) { pos = i; op = c; }
    }
    if (paren != 0 || brace != 0) { err = "Unbalanced query expression"; return false; }
    if (pos == string::npos) return false;
    lhs = expr.substr(0, pos);
    rhs = expr.substr(pos + 1);
    if (lhs.empty() || rhs.empty()) { err = "Top-level binary operator is missing an operand"; return false; }
    return true;
}

static bool CompileIncrementalMacroRec(const string& raw, IncrementalProgram& program, int& node_id, string& err) {
    string expr = StripQuerySpaces(raw);
    while (IsWholeParenthesized(expr)) expr = expr.substr(1, expr.size() - 2);
    char op = '\0';
    string lhs, rhs;
    string split_err;
    const bool split = SplitLastTopLevelMacroOp(expr, op, lhs, rhs, split_err);
    if (split && expr.find('{') != string::npos) {
        int left = -1, right = -1;
        if (!CompileIncrementalMacroRec(lhs, program, left, err) ||
            !CompileIncrementalMacroRec(rhs, program, right, err)) return false;
        node_id = static_cast<int>(program.nodes.size());
        program.nodes.push_back(IncrementalMacroNode{
            op == '/' ? IncrementalMacroKind::SEQUENCE : IncrementalMacroKind::CHOICE,
            left, right, -1, 0, 0});
        return true;
    }
    if (!split_err.empty()) { err = split_err; return false; }
    QuerySpec spec;
    string leaf_error;
    if (!ParseQuerySpec(expr, spec, leaf_error)) {
        string repeated_expression;
        int min_rep = 0;
        int max_rep = 0;
        string range_error;
        if (ParseIncrementalRangeSuffix(
                expr, repeated_expression, min_rep, max_rep, range_error) &&
            repeated_expression.find('{') != string::npos) {
            while (IsWholeParenthesized(repeated_expression)) {
                repeated_expression = repeated_expression.substr(
                    1, repeated_expression.size() - 2);
            }
            int child = -1;
            if (!CompileIncrementalMacroRec(
                    repeated_expression, program, child, err)) {
                return false;
            }
            QuerySpec virtual_body;
            virtual_body.body = "__macro_" + to_string(child);
            virtual_body.min_rep = 1;
            virtual_body.max_rep = 1;
            const int body_id = static_cast<int>(program.bodies.size());
            program.bodies.push_back(std::move(virtual_body));
            program.virtual_body_nodes.push_back(child);
            node_id = static_cast<int>(program.nodes.size());
            program.nodes.push_back(IncrementalMacroNode{
                IncrementalMacroKind::REPETITION, child, -1, body_id,
                min_rep, max_rep});
            return true;
        }
        // A plain state-machine segment is a one-step macro edge.  Composite
        // expressions such as a/B{32} must not require the user to spell the
        // ordinary prefix as a{1}.
        if (expr.find('{') != string::npos ||
            !ParseQuerySpec("(" + expr + "){1}", spec, err)) {
            if (!range_error.empty()) err = range_error;
            else if (err.empty()) err = leaf_error;
            return false;
        }
    }
    int body_id = -1;
    for (size_t i = 0; i < program.bodies.size(); ++i) {
        if (program.virtual_body_nodes[i] < 0 &&
            program.bodies[i].body == spec.body) {
            body_id = static_cast<int>(i);
            break;
        }
    }
    if (body_id < 0) {
        body_id = static_cast<int>(program.bodies.size());
        program.bodies.push_back(std::move(spec));
        program.virtual_body_nodes.push_back(-1);
    }
    else spec = program.bodies[static_cast<size_t>(body_id)];
    node_id = static_cast<int>(program.nodes.size());
    program.nodes.push_back(IncrementalMacroNode{
        IncrementalMacroKind::LEAF, -1, -1, body_id, spec.min_rep, spec.max_rep});
    return true;
}

static bool CompileIncrementalProgram(const string& expr, IncrementalProgram& program, string& err) {
    program = IncrementalProgram{};
    return CompileIncrementalMacroRec(expr, program, program.root, err);
}

static vector<string> IncrementalMacroStartLabels(const IncrementalProgram& program, int node_id) {
    const auto& node = program.nodes.at(static_cast<size_t>(node_id));
    if (node.kind == IncrementalMacroKind::LEAF) {
        return CollectRegexStartLabels(program.bodies.at(static_cast<size_t>(node.body_id)).pattern.nfa);
    }
    if (node.kind == IncrementalMacroKind::REPETITION) {
        return IncrementalMacroStartLabels(program, node.left);
    }
    vector<string> out = IncrementalMacroStartLabels(program, node.left);
    if (node.kind == IncrementalMacroKind::CHOICE) {
        vector<string> rhs = IncrementalMacroStartLabels(program, node.right);
        out.insert(out.end(), rhs.begin(), rhs.end());
    }
    sort(out.begin(), out.end());
    out.erase(unique(out.begin(), out.end()), out.end());
    return out;
}

static void IncAppendU32(string& out, uint32_t value) { AppendRawU32(out, value); }
static void IncAppendU64(string& out, uint64_t value) { AppendRawU64(out, value); }
static bool IncReadU32(const string& in, size_t& off, uint32_t& value) { return ReadRawU32(in, off, value); }
static bool IncReadU64(const string& in, size_t& off, uint64_t& value) { return ReadRawU64(in, off, value); }
static void IncAppendString(string& out, const string& value) { IncAppendU32(out, static_cast<uint32_t>(value.size())); out.append(value); }
static bool IncReadString(const string& in, size_t& off, string& value) {
    uint32_t size = 0;
    if (!IncReadU32(in, off, size) || off + size > in.size()) return false;
    value.assign(in.data() + off, size); off += size; return true;
}

static void IncAppendSet(string& out, const RelationDstSet& values) {
    // Keep Roaring compressed across the incremental data plane.  Expanding a
    // dense delta to individual uint64_t values before wrapping/compressing it
    // dominated the B16/B32 stream path even though the in-memory relation is
    // already a Roaring64Map.
    const size_t bytes = values.bitmap.getSizeInBytes(true);
    IncAppendU64(out, static_cast<uint64_t>(bytes));
    const size_t offset = out.size();
    out.resize(offset + bytes);
    values.bitmap.write(out.data() + offset, true);
}
static bool IncReadSet(const string& in, size_t& off, RelationDstSet& values) {
    values = RelationDstSet{};
    uint64_t bytes = 0;
    if (!IncReadU64(in, off, bytes) || bytes > in.size() - off || bytes == 0) return false;
    values.bitmap = roaring::Roaring64Map::readSafe(in.data() + off, static_cast<size_t>(bytes));
    off += static_cast<size_t>(bytes);
    return true;
}

static constexpr uint32_t kIncrementalWireMagic = 0x31524e49;  // INR1
static constexpr uint32_t kIncrementalWireVersion = 1;
#include "incremental_final_deflate.h"
enum class IncrementalWireKind : uint32_t { ROW_REQUEST = 1, ROW_READY = 2, BASE_EXPAND = 3, BASE_RESULT = 4, RESULTS = 5, ROW_DELTA = 6, ROW_COMPLETE = 7, ROW_DELTA_BATCH = 8, ROW_REQUEST_BATCH = 9, ROW_COMPLETE_BATCH = 10, ROW_STREAM_BATCH = 11, DEMAND_BATCH_REQUEST = 12, DEMAND_BATCH_STREAM = 13, MATRIX_JOIN_UPDATE = 14, MATRIX_JOIN_RESULT = 15, MATRIX_RESIDENT_CONTINUE = 16, BASE_EXPAND_BATCH = 17, BASE_RESULT_BATCH = 18, MATRIX_JOIN_FACTORIZED_RESULT = 19 };

static string IncWrap(IncrementalWireKind kind, const string& raw) {
    string out; IncAppendU32(out, kIncrementalWireMagic); IncAppendU32(out, kIncrementalWireVersion); IncAppendU32(out, static_cast<uint32_t>(kind)); out.append(raw); return WrapCompressedPayload(out);
}
static bool IncUnwrap(const string& data, IncrementalWireKind expected, string& raw, string& err) {
    string wrapped; if (!UnwrapCompressedPayload(data, wrapped, err)) return false; size_t off = 0; uint32_t magic = 0, version = 0, kind = 0;
    if (!IncReadU32(wrapped, off, magic) || !IncReadU32(wrapped, off, version) || !IncReadU32(wrapped, off, kind) || magic != kIncrementalWireMagic || version != kIncrementalWireVersion || kind != static_cast<uint32_t>(expected)) { err = "Bad incremental payload"; return false; }
    raw.assign(wrapped.data() + off, wrapped.size() - off); return true;
}

static string IncWrapRaw(IncrementalWireKind kind, const string& raw) {
    string out;
    IncAppendU32(out, kIncrementalWireMagic);
    IncAppendU32(out, kIncrementalWireVersion);
    IncAppendU32(out, static_cast<uint32_t>(kind));
    out.append(raw);
    return out;
}

static bool IncUnwrapRaw(
    const string& data, IncrementalWireKind expected, string& raw, string& err) {
    size_t off = 0;
    uint32_t magic = 0, version = 0, kind = 0;
    if (!IncReadU32(data, off, magic) || !IncReadU32(data, off, version) ||
        !IncReadU32(data, off, kind) || magic != kIncrementalWireMagic ||
        version != kIncrementalWireVersion ||
        kind != static_cast<uint32_t>(expected)) {
        err = "Bad uncompressed incremental payload";
        return false;
    }
    raw.assign(data.data() + off, data.size() - off);
    return true;
}

struct IncrementalRowKey {
    int query_id = 0;
    int body_id = 0;
    int length = 0;
    uint64_t source = 0;
    bool operator==(const IncrementalRowKey& other) const { return query_id == other.query_id && body_id == other.body_id && length == other.length && source == other.source; }
};

// A DemandBatch is the execution/scheduling unit.  Keep this wire format
// separate from row-cache traffic: one request carries an entire active
// source frontier and one stream event carries many relation rows.
struct IncrementalDemandBatchRequest {
    int query_id = 0;
    int body_id = 0;
    int length = 0;
    int reply_pid = -1;
    uint64_t token = 0;
    bool direct_right_stream = false;
    // In middle-partition mode the RHS owner is the middle worker itself.
    // It installs the RHS there instead of returning the payload to the
    // source owner for an immediate second hop back to the same worker.
    bool middle_rhs_direct = false;
    uint64_t parent_batch_id = 0;
    RelationDstSet sources;
};

struct IncrementalDemandBatchStream {
    int query_id = 0;
    uint64_t token = 0;
    bool complete = false;
    // Direct-right streams use this compact mid -> destination-partition set
    // for RSFM; ordinary streams still carry relation rows.
    bool direct_signature = false;
    // Middle-owner exact evaluation sends only the nonempty RHS middle keys
    // back to the source owner. The RHS payload itself stays at owner(mid).
    bool middle_presence = false;
    // Presence is metadata, not a relation payload. Keeping the keys in a
    // bitmap avoids sending the RHS endpoint sets back to every requester.
    RelationDstSet middle_presence_mids;
    RelationRows rows;
};

static void IncAppendRowsLegacy(string& out, const RelationRows& rows) {
    IncAppendU64(out, static_cast<uint64_t>(rows.size()));
    for (const auto& [source, values] : rows) {
        IncAppendU64(out, source);
        IncAppendSet(out, values);
    }
}

static bool IncReadRowsLegacy(const string& in, size_t& off, RelationRows& rows) {
    uint64_t count = 0;
    if (!IncReadU64(in, off, count) || count > (1ULL << 32)) return false;
    rows.clear();
    rows.reserve(static_cast<size_t>(min<uint64_t>(count, 1ULL << 20)));
    for (uint64_t index = 0; index < count; ++index) {
        uint64_t source = 0;
        RelationDstSet values;
        if (!IncReadU64(in, off, source) || !IncReadSet(in, off, values)) return false;
        if (!values.empty()) rows.emplace(source, std::move(values));
    }
    return true;
}

#include "biclique_wire.inc"

static string SerializeIncrementalDemandBatchRequest(const IncrementalDemandBatchRequest& request) {
    string raw;
    IncAppendU32(raw, static_cast<uint32_t>(request.query_id));
    IncAppendU32(raw, static_cast<uint32_t>(request.body_id));
    IncAppendU32(raw, static_cast<uint32_t>(request.length));
    IncAppendU32(raw, static_cast<uint32_t>(request.reply_pid));
    IncAppendU64(raw, request.token);
    IncAppendU32(raw, request.direct_right_stream ? 1U : 0U);
    IncAppendU32(raw, request.middle_rhs_direct ? 1U : 0U);
    IncAppendU64(raw, request.parent_batch_id);
    IncAppendSet(raw, request.sources);
    return IncWrap(IncrementalWireKind::DEMAND_BATCH_REQUEST, raw);
}

static bool ParseIncrementalDemandBatchRequest(
    const string& data, IncrementalDemandBatchRequest& request, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::DEMAND_BATCH_REQUEST, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, body_id = 0, length = 0, reply_pid = 0, direct_right = 0;
    uint32_t middle_rhs_direct = 0;
    if (!IncReadU32(raw, off, query_id) || !IncReadU32(raw, off, body_id) ||
        !IncReadU32(raw, off, length) || !IncReadU32(raw, off, reply_pid) ||
        !IncReadU64(raw, off, request.token) || !IncReadU32(raw, off, direct_right) ||
        direct_right > 1 || !IncReadU32(raw, off, middle_rhs_direct) ||
        middle_rhs_direct > 1 || !IncReadU64(raw, off, request.parent_batch_id) ||
        !IncReadSet(raw, off, request.sources) ||
        off != raw.size()) {
        err = "Bad demand batch request";
        return false;
    }
    request.query_id = static_cast<int>(query_id);
    request.body_id = static_cast<int>(body_id);
    request.length = static_cast<int>(length);
    request.reply_pid = static_cast<int>(reply_pid);
    request.direct_right_stream = direct_right != 0;
    request.middle_rhs_direct = middle_rhs_direct != 0;
    return true;
}

static string SerializeIncrementalDemandBatchStream(const IncrementalDemandBatchStream& event) {
    string raw;
    IncAppendU32(raw, static_cast<uint32_t>(event.query_id));
    IncAppendU64(raw, event.token);
    IncAppendU32(raw, event.complete ? 1U : 0U);
    IncAppendU32(raw, event.direct_signature ? 1U : 0U);
    IncAppendU32(raw, event.middle_presence ? 1U : 0U);
    if (event.middle_presence &&
        (!event.complete || !event.middle_presence_mids.empty())) {
        IncAppendSet(raw, event.middle_presence_mids);
    }
    IncAppendRows(raw, event.rows);
    return IncWrap(IncrementalWireKind::DEMAND_BATCH_STREAM, raw);
}

static bool ParseIncrementalDemandBatchStream(
    const string& data, IncrementalDemandBatchStream& event, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::DEMAND_BATCH_STREAM, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, complete = 0, direct_signature = 0, middle_presence = 0;
    if (!IncReadU32(raw, off, query_id) || !IncReadU64(raw, off, event.token) ||
        !IncReadU32(raw, off, complete) || !IncReadU32(raw, off, direct_signature) ||
        direct_signature > 1 || !IncReadU32(raw, off, middle_presence) ||
        middle_presence > 1 ||
        (middle_presence != 0 &&
         !(complete != 0 && raw.size() - off == sizeof(uint64_t)) &&
         !IncReadSet(raw, off, event.middle_presence_mids)) ||
        !IncReadRows(raw, off, event.rows) ||
        off != raw.size()) {
        err = "Bad demand batch stream";
        return false;
    }
    event.query_id = static_cast<int>(query_id);
    event.complete = complete != 0;
    event.direct_signature = direct_signature != 0;
    event.middle_presence = middle_presence != 0;
    return true;
}

// A join update belongs to one parent DemandBatch. In middle-partition mode,
// `left_rows` contains A[:,k] and `right_rows` contains B[k,:]; W_k emits a
// partial C delta for reduction at the parent. Legacy builds use destination
// blocks with the same envelope.
struct IncrementalMatrixJoinUpdate {
    int query_id = 0;
    int parent_pid = 0;
    uint64_t parent_batch_id = 0;
    uint64_t token = 0;
    // Direct exact-count roots use one globally identified compose stage.
    // Every source owner contributes once; owner(mid) runs GCNP only after
    // all source-owner seals for this stage have arrived.
    bool global_exact_stage = false;
    bool global_exact_seal = false;
    // RHS shards are physically source-owned. Owner(mid) publishes its
    // signature only after it has received one sealed RHS shard from every
    // source owner.
    bool global_rhs_seal = false;
    bool global_base_stage = false;
    // A virtual macro edge propagates the original-source frontier through
    // rows already materialized by a counted-expression evaluator.  It uses
    // the global stage transport, but deliberately skips GCNP/matrix join.
    bool global_virtual_stage = false;
    // Complete-relation stages defer expansion until every middle contribution
    // is present. This permits exact source-set rectangles from different
    // middles to be coalesced before ordinary source rows are materialized.
    bool defer_factorized_materialization = false;
    uint64_t global_stage_id = 0;
    int global_sender_pid = -1;
    bool retain_output_on_destination = false;
    // Legacy visibility bit: true marks a retained intermediate that must be
    // excluded from final fetching. It no longer returns endpoint rows to its
    // parent; later compositions use its destination-resident block handle.
    bool return_output_to_parent = false;
    bool cleanup_only = false;
    bool direct_right_stream = false;
    bool direct_stream_complete = false;
    // This branch partitions a binary join by the middle vertex. The sender
    // routes A(*,k) and B(k,*) to W_k; the result is reduced at parent_pid.
    bool middle_partition_join = false;
    // This update contains a RHS delta installed directly by its middle
    // owner. It is not a source-owned matrix token and therefore needs no
    // matrix acknowledgement back to the source.
    bool middle_rhs_direct = false;
    // Independent panel seals for synchronous middle composition. Data may
    // arrive early, but GCNP/join starts only after both seals are observed.
    bool middle_left_complete = false;
    bool middle_stream_complete = false;
    // A physical middle owner may delegate one stateless incremental join to
    // another worker.  The helper never retains matrix state; it only
    // evaluates left_rows o right_rows and returns the candidates to owner.
    bool middle_compute_task = false;
    // Set only when the parent observes that this physical middle owner is
    // materially heavier than its peers in the same routed input batch.
    bool middle_allow_remote_compute = false;
    // The parent has the only same-batch view of every physical middle owner.
    // It nominates the lightest peer for an overloaded middle task.
    int middle_preferred_compute_pid = -1;
    int middle_partition = -1;
    uint64_t middle_virtual_shard = 0;
    uint64_t scheduled_join_work = 0;
    // Legacy destination-shard finalization marker. Middle mode uses the two
    // explicit panel seals above instead.
    bool finalize_shard = false;
    // A resident-frontier continuation has one input stream per nonempty
    // upstream destination shard.  Each producer sends this marker after its
    // final left-panel update.  The output shard seals only after every stream
    // arrives, so GCNP still sees one complete local panel.
    bool resident_left_stream_complete = false;
    uint64_t resident_left_stream_id = 0;
    uint64_t expected_resident_left_streams = 0;
    uint64_t expected_right_streams = 0;
    // A retained generic RHS already lives in another destination-owned
    // matrix block. Refer to that immutable complete block instead of sending
    // its payload back through the parent and immediately returning it.
    int right_parent_pid = -1;
    uint64_t right_parent_batch_id = 0;
    uint64_t expected_right_rows = 0;
    uint64_t expected_right_pairs = 0;
    RelationDstSet right_sources;
    // Physical B^k payloads are resident destination shards.  A left-only
    // RSFM update carries this identity and joins against the resident RHS.
    int right_body_id = -1;
    int right_length = 0;
    bool right_include_identity = false;
    int coverage_parent_pid = -1;
    uint64_t coverage_parent_batch_id = 0;
    int coverage_body_id = -1;
    int coverage_length = 0;
    bool coverage_include_identity = false;
    // Source rows belonging to this bounded suffix instance. Resident
    // relation shards are query-wide caches and must be projected through
    // this set before they become S[a] or S[b] for the current batch.
    RelationDstSet coverage_sources;
    // RSFM replay can retain its natural mid -> sources orientation until it
    // reaches the destination worker. The destination expands it locally,
    // avoiding a source-owner replay bottleneck.
    bool left_rows_transposed = false;
    RelationRows left_rows;
    // A route flush may contain both ordinary source -> mid deltas and a
    // signature-triggered mid -> sources replay for the same C shard. Keep
    // the second orientation encoded on wire but execute both in one task.
    RelationRows transposed_left_rows;
    RelationRows right_rows;
    // Sender-only borrowed rows. The stage owner keeps the immutable RHS alive
    // until serialization completes; these pointers never cross the wire.
    vector<pair<uint64_t, const RelationDstSet*>> right_row_views;
    // Sender-only, stage-scoped byte reuse; never part of the wire schema.
    shared_ptr<BicliqueImmutableWireCache> right_wire_cache;
    vector<pair<uint64_t, const RelationDstSet*>> left_row_views;
    vector<pair<uint64_t, const RelationDstSet*>> coverage_row_views;
    shared_ptr<const RelationRows> left_view_owner;
    shared_ptr<const RelationRows> coverage_view_owner;
    // For the bounded dual-state recurrence S[a+b] = S[a] union
    // (P[a] o S[b]), this is the already-known S[a] output shard. The
    // destination owner removes it from join candidates before replying.
    RelationRows coverage_rows;
    bool restore_coverage_output = false;
};

// Continue `right_node_id` from a complete destination-resident left block.
// The message deliberately carries only two block handles and stream metadata;
// its receiver reads A(s,m) from its local destination shard.
struct IncrementalMatrixResidentContinue {
    int query_id = 0;
    int input_parent_pid = -1;
    uint64_t input_parent_batch_id = 0;
    int output_parent_pid = -1;
    uint64_t output_parent_batch_id = 0;
    int right_node_id = -1;
    uint64_t stream_id = 0;
    uint64_t expected_streams = 0;
    bool retain_output_on_destination = true;
};

struct IncrementalMatrixJoinResult {
    int query_id = 0;
    uint64_t token = 0;
    uint64_t parent_batch_id = 0;
    bool global_exact_stage = false;
    bool defer_factorized_materialization = false;
    uint64_t global_stage_id = 0;
    bool direct_right_stream = false;
    bool middle_partial = false;
    bool middle_complete = false;
    bool middle_compute_result = false;
    int middle_partition = -1;
    uint64_t middle_virtual_shard = 0;
    uint64_t scheduled_join_work = 0;
    int destination_pid = -1;
    RelationDstSet output_sources;
    uint64_t output_rows = 0;
    uint64_t output_pairs = 0;
    // Complete middle-local rows with identical destination bitmaps are
    // grouped on the wire. The source owner accumulates exact source-set
    // equality classes across middle messages before restoring ordinary rows.
    struct SharedBlock {
        enum class DestinationCodec : uint32_t {
            DEFINE = 0,
            REFERENCE = 1,
            XOR_DELTA = 2,
        };
        uint64_t block_id = 0;
        DestinationCodec destination_codec = DestinationCodec::DEFINE;
        uint64_t dictionary_id = 0;
        uint64_t base_dictionary_id = 0;
        RelationDstSet sources;
        // DEFINE carries the complete bitmap; XOR_DELTA carries only the xor
        // residual; REFERENCE carries no destination payload.
        RelationDstSet destinations;
    };
    bool factorized_rows = false;
    bool dictionary_coded_blocks = false;
    vector<SharedBlock> shared_blocks;
    // In grouped mode rows contains complete rows whose destination bitmap is
    // unique within this source-owner message.
    RelationRows rows;
};

struct IncrementalMatrixEncodingTiming {
    uint64_t encode_us = 0, wrap_us = 0, raw_bytes = 0, wire_bytes = 0;
};

static string SerializeIncrementalMatrixJoinUpdate(
    const IncrementalMatrixJoinUpdate& update,
    IncrementalMatrixEncodingTiming* timing = nullptr) {
    const auto begin = chrono::steady_clock::now();
    string raw;
    if (!update.right_row_views.empty()) {
        size_t reserve_bytes = 512;
        for (const auto& row : update.right_row_views) {
            reserve_bytes += 16 + row.second->bitmap.getSizeInBytes(true);
        }
        raw.reserve(reserve_bytes);
    }
    IncAppendU32(raw, static_cast<uint32_t>(update.query_id));
    IncAppendU32(raw, static_cast<uint32_t>(update.parent_pid));
    IncAppendU64(raw, update.parent_batch_id);
    IncAppendU64(raw, update.token);
    IncAppendU32(raw, update.global_exact_stage ? 1U : 0U);
    IncAppendU32(raw, update.global_exact_seal ? 1U : 0U);
    IncAppendU32(raw, update.global_rhs_seal ? 1U : 0U);
    IncAppendU32(raw, update.global_base_stage ? 1U : 0U);
    IncAppendU32(raw, update.global_virtual_stage ? 1U : 0U);
    IncAppendU32(raw, update.defer_factorized_materialization ? 1U : 0U);
    IncAppendU64(raw, update.global_stage_id);
    IncAppendU32(raw, static_cast<uint32_t>(update.global_sender_pid));
    IncAppendU32(raw, update.retain_output_on_destination ? 1U : 0U);
    IncAppendU32(raw, update.return_output_to_parent ? 1U : 0U);
    IncAppendU32(raw, update.cleanup_only ? 1U : 0U);
    IncAppendU32(raw, update.direct_right_stream ? 1U : 0U);
    IncAppendU32(raw, update.direct_stream_complete ? 1U : 0U);
    IncAppendU32(raw, update.middle_partition_join ? 1U : 0U);
    IncAppendU32(raw, update.middle_rhs_direct ? 1U : 0U);
    IncAppendU32(raw, update.middle_left_complete ? 1U : 0U);
    IncAppendU32(raw, update.middle_stream_complete ? 1U : 0U);
    IncAppendU32(raw, update.middle_compute_task ? 1U : 0U);
    IncAppendU32(raw, update.middle_allow_remote_compute ? 1U : 0U);
    IncAppendU32(raw, static_cast<uint32_t>(update.middle_preferred_compute_pid));
    IncAppendU32(raw, static_cast<uint32_t>(update.middle_partition));
    IncAppendU64(raw, update.middle_virtual_shard);
    IncAppendU64(raw, update.scheduled_join_work);
    IncAppendU32(raw, update.finalize_shard ? 1U : 0U);
    IncAppendU32(raw, update.resident_left_stream_complete ? 1U : 0U);
    IncAppendU64(raw, update.resident_left_stream_id);
    IncAppendU64(raw, update.expected_resident_left_streams);
    IncAppendU64(raw, update.expected_right_streams);
    IncAppendU32(raw, static_cast<uint32_t>(update.right_parent_pid));
    IncAppendU64(raw, update.right_parent_batch_id);
    IncAppendU64(raw, update.expected_right_rows);
    IncAppendU64(raw, update.expected_right_pairs);
    IncAppendSet(raw, update.right_sources);
    IncAppendU32(raw, static_cast<uint32_t>(update.right_body_id));
    IncAppendU32(raw, static_cast<uint32_t>(update.right_length));
    IncAppendU32(raw, update.right_include_identity ? 1U : 0U);
    IncAppendU32(raw, static_cast<uint32_t>(update.coverage_parent_pid));
    IncAppendU64(raw, update.coverage_parent_batch_id);
    IncAppendU32(raw, static_cast<uint32_t>(update.coverage_body_id));
    IncAppendU32(raw, static_cast<uint32_t>(update.coverage_length));
    IncAppendU32(raw, update.coverage_include_identity ? 1U : 0U);
    IncAppendSet(raw, update.coverage_sources);
    IncAppendU32(raw, update.left_rows_transposed ? 1U : 0U);
    auto append_input = [&raw](const RelationRows& rows,
        const vector<pair<uint64_t, const RelationDstSet*>>& views) {
        if (views.empty()) { IncAppendRows(raw, rows); return; }
        if (!rows.empty()) throw runtime_error("mixed owned and borrowed input rows");
        if (IncAppendBicliqueRows(raw, views)) return;
        IncAppendU64(raw, views.size());
        for (const auto& row : views) {
            IncAppendU64(raw, row.first);
            IncAppendSet(raw, *row.second);
        }
    };
    append_input(update.left_rows, update.left_row_views);
    IncAppendRows(raw, update.transposed_left_rows);
    if (update.right_row_views.empty()) {
        IncAppendRows(raw, update.right_rows);
    } else {
        if (!update.right_rows.empty()) throw runtime_error(
            "mixed owned and borrowed RHS rows");
        if (!IncAppendBicliqueRows(raw, update.right_row_views, update.right_wire_cache.get())) {
            IncAppendU64(raw, update.right_row_views.size());
            for (const auto& [mid, targets] : update.right_row_views) {
                IncAppendU64(raw, mid);
                if (update.right_wire_cache) {
                    const auto& encoded = update.right_wire_cache->Get(*targets);
                    IncAppendU64(raw, encoded.bytes.size());
                    raw.append(encoded.bytes);
                } else IncAppendSet(raw, *targets);
            }
        }
    }
    // Empty coverage is the common path for exact-count composition.  Do not
    // spend a second row-count field on it; the parser treats end-of-payload
    // after right_rows as an empty coverage relation.
    if (!update.coverage_rows.empty() || !update.coverage_row_views.empty() || update.restore_coverage_output)
        append_input(update.coverage_rows, update.coverage_row_views);
    if (update.restore_coverage_output) IncAppendU32(raw, 1U);
    const auto encoded = chrono::steady_clock::now();
    string wire;
    if (update.middle_compute_task && update.middle_virtual_shard == 3) {
        // Preserve the existing envelope and bitmap representation. RHS-only
        // replies need not recompress the already-compressed Roaring payload.
        string wrapped = IncWrapRaw(IncrementalWireKind::MATRIX_JOIN_UPDATE, raw);
        AppendRawU32(wire, kCompressedPayloadMagic);
        AppendRawU32(wire, kCompressedPayloadVersion);
        AppendRawU32(wire, 0);
        AppendRawU64(wire, wrapped.size());
        wire.append(wrapped);
    } else if (update.middle_compute_task &&
               (update.middle_virtual_shard == 1 ||
                update.middle_virtual_shard == 4 ||
                update.middle_virtual_shard == 5)) {
        // LHS panels already contain Roaring/Biclique-compressed matrices.
        // The outer deflate sat on the single serial producer path ahead of
        // every later helper's panel; send the existing stored (flag 0)
        // envelope instead. Biclique encoding and content are unchanged.
        string wrapped = IncWrapRaw(IncrementalWireKind::MATRIX_JOIN_UPDATE, raw);
        wire.reserve(20 + wrapped.size());
        AppendRawU32(wire, kCompressedPayloadMagic);
        AppendRawU32(wire, kCompressedPayloadVersion);
        AppendRawU32(wire, 0);
        AppendRawU64(wire, wrapped.size());
        wire.append(wrapped);
    } else {
        wire = IncWrap(IncrementalWireKind::MATRIX_JOIN_UPDATE, raw);
    }
    if (timing != nullptr) {
        const auto end = chrono::steady_clock::now();
        timing->encode_us = chrono::duration_cast<chrono::microseconds>(
            encoded - begin).count();
        timing->wrap_us = chrono::duration_cast<chrono::microseconds>(
            end - encoded).count();
        timing->raw_bytes = raw.size();
        timing->wire_bytes = wire.size();
    }
    return wire;
}

static bool ParseIncrementalMatrixJoinUpdate(
    const string& data, IncrementalMatrixJoinUpdate& update, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::MATRIX_JOIN_UPDATE, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, parent_pid = 0, retain_output = 0, return_output = 0;
    uint32_t global_exact_stage = 0, global_exact_seal = 0;
    uint32_t global_rhs_seal = 0;
    uint32_t global_base_stage = 0, global_virtual_stage = 0;
    uint32_t defer_factorized_materialization = 0;
    uint32_t global_sender_pid = 0;
    uint32_t cleanup_only = 0, direct_right = 0, stream_complete = 0;
    uint32_t middle_join = 0, middle_rhs_direct = 0, middle_left_complete = 0, middle_complete = 0, middle_compute_task = 0;
    uint32_t middle_allow_remote_compute = 0;
    uint32_t middle_partition = 0, middle_preferred_compute_pid = 0;
    uint32_t finalize_shard = 0, resident_left_stream_complete = 0, transposed = 0;
    uint32_t restore_coverage = 0;
    uint32_t right_parent_pid = 0, right_body = 0, right_length = 0, right_identity = 0, coverage_parent_pid = 0;
    uint32_t coverage_body = 0, coverage_length = 0, coverage_identity = 0;
    if (!IncReadU32(raw, off, query_id) || !IncReadU32(raw, off, parent_pid) ||
        !IncReadU64(raw, off, update.parent_batch_id) || !IncReadU64(raw, off, update.token) ||
        !IncReadU32(raw, off, global_exact_stage) || global_exact_stage > 1 ||
        !IncReadU32(raw, off, global_exact_seal) || global_exact_seal > 1 ||
        !IncReadU32(raw, off, global_rhs_seal) || global_rhs_seal > 1 ||
        !IncReadU32(raw, off, global_base_stage) || global_base_stage > 1 ||
        !IncReadU32(raw, off, global_virtual_stage) || global_virtual_stage > 1 ||
        !IncReadU32(raw, off, defer_factorized_materialization) ||
        defer_factorized_materialization > 1 ||
        !IncReadU64(raw, off, update.global_stage_id) ||
        !IncReadU32(raw, off, global_sender_pid) ||
        !IncReadU32(raw, off, retain_output) || !IncReadU32(raw, off, return_output) ||
        return_output > 1 || !IncReadU32(raw, off, cleanup_only) || cleanup_only > 1 ||
        !IncReadU32(raw, off, direct_right) ||
        !IncReadU32(raw, off, stream_complete) || direct_right > 1 || stream_complete > 1 ||
        !IncReadU32(raw, off, middle_join) || middle_join > 1 ||
        !IncReadU32(raw, off, middle_rhs_direct) || middle_rhs_direct > 1 ||
        !IncReadU32(raw, off, middle_left_complete) || middle_left_complete > 1 ||
        !IncReadU32(raw, off, middle_complete) || middle_complete > 1 ||
        !IncReadU32(raw, off, middle_compute_task) || middle_compute_task > 1 ||
        !IncReadU32(raw, off, middle_allow_remote_compute) ||
        middle_allow_remote_compute > 1 ||
        !IncReadU32(raw, off, middle_preferred_compute_pid) ||
        !IncReadU32(raw, off, middle_partition) ||
        !IncReadU64(raw, off, update.middle_virtual_shard) ||
        !IncReadU64(raw, off, update.scheduled_join_work) ||
        !IncReadU32(raw, off, finalize_shard) || finalize_shard > 1 ||
        !IncReadU32(raw, off, resident_left_stream_complete) || resident_left_stream_complete > 1 ||
        !IncReadU64(raw, off, update.resident_left_stream_id) ||
        !IncReadU64(raw, off, update.expected_resident_left_streams) ||
        !IncReadU64(raw, off, update.expected_right_streams) ||
        !IncReadU32(raw, off, right_parent_pid) ||
        !IncReadU64(raw, off, update.right_parent_batch_id) ||
        !IncReadU64(raw, off, update.expected_right_rows) ||
        !IncReadU64(raw, off, update.expected_right_pairs) ||
        !IncReadSet(raw, off, update.right_sources) ||
        !IncReadU32(raw, off, right_body) || !IncReadU32(raw, off, right_length) ||
        !IncReadU32(raw, off, right_identity) || right_identity > 1 ||
        !IncReadU32(raw, off, coverage_parent_pid) ||
        !IncReadU64(raw, off, update.coverage_parent_batch_id) ||
        !IncReadU32(raw, off, coverage_body) ||
        !IncReadU32(raw, off, coverage_length) ||
        !IncReadU32(raw, off, coverage_identity) || coverage_identity > 1 ||
        !IncReadSet(raw, off, update.coverage_sources) ||
        !IncReadU32(raw, off, transposed) || transposed > 1 ||
        !IncReadRows(raw, off, update.left_rows) || !IncReadRows(raw, off, update.transposed_left_rows) ||
        !IncReadRows(raw, off, update.right_rows) ||
        (off != raw.size() && !IncReadRows(raw, off, update.coverage_rows)) ||
        (off != raw.size() && (!IncReadU32(raw, off, restore_coverage) || restore_coverage > 1)) ||
        off != raw.size()) {
        err = "Bad matrix join update";
        return false;
    }
    update.query_id = static_cast<int>(query_id);
    update.parent_pid = static_cast<int>(parent_pid);
    update.global_exact_stage = global_exact_stage != 0;
    update.global_exact_seal = global_exact_seal != 0;
    update.global_rhs_seal = global_rhs_seal != 0;
    update.global_base_stage = global_base_stage != 0;
    update.global_virtual_stage = global_virtual_stage != 0;
    update.defer_factorized_materialization =
        defer_factorized_materialization != 0;
    update.global_sender_pid = static_cast<int>(global_sender_pid);
    update.retain_output_on_destination = retain_output != 0;
    update.return_output_to_parent = return_output != 0;
    update.cleanup_only = cleanup_only != 0;
    update.direct_right_stream = direct_right != 0;
    update.direct_stream_complete = stream_complete != 0;
    update.middle_partition_join = middle_join != 0;
    update.middle_rhs_direct = middle_rhs_direct != 0;
    update.middle_left_complete = middle_left_complete != 0;
    update.middle_stream_complete = middle_complete != 0;
    update.middle_compute_task = middle_compute_task != 0;
    update.middle_allow_remote_compute = middle_allow_remote_compute != 0;
    update.middle_preferred_compute_pid =
        static_cast<int>(middle_preferred_compute_pid);
    update.middle_partition = static_cast<int>(middle_partition);
    update.finalize_shard = finalize_shard != 0;
    update.resident_left_stream_complete = resident_left_stream_complete != 0;
    update.right_parent_pid = static_cast<int>(right_parent_pid);
    update.right_body_id = static_cast<int>(right_body);
    update.right_length = static_cast<int>(right_length);
    update.right_include_identity = right_identity != 0;
    update.coverage_parent_pid = static_cast<int>(coverage_parent_pid);
    update.coverage_body_id = static_cast<int>(coverage_body);
    update.coverage_length = static_cast<int>(coverage_length);
    update.coverage_include_identity = coverage_identity != 0;
    update.left_rows_transposed = transposed != 0;
    update.restore_coverage_output = restore_coverage != 0;
    return true;
}

static string SerializeIncrementalMatrixResidentContinue(
    const IncrementalMatrixResidentContinue& request) {
    string raw;
    IncAppendU32(raw, static_cast<uint32_t>(request.query_id));
    IncAppendU32(raw, static_cast<uint32_t>(request.input_parent_pid));
    IncAppendU64(raw, request.input_parent_batch_id);
    IncAppendU32(raw, static_cast<uint32_t>(request.output_parent_pid));
    IncAppendU64(raw, request.output_parent_batch_id);
    IncAppendU32(raw, static_cast<uint32_t>(request.right_node_id));
    IncAppendU64(raw, request.stream_id);
    IncAppendU64(raw, request.expected_streams);
    IncAppendU32(raw, request.retain_output_on_destination ? 1U : 0U);
    return IncWrap(IncrementalWireKind::MATRIX_RESIDENT_CONTINUE, raw);
}

static bool ParseIncrementalMatrixResidentContinue(
    const string& data, IncrementalMatrixResidentContinue& request, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::MATRIX_RESIDENT_CONTINUE, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, input_parent_pid = 0, output_parent_pid = 0;
    uint32_t right_node_id = 0, retain_output = 0;
    if (!IncReadU32(raw, off, query_id) || !IncReadU32(raw, off, input_parent_pid) ||
        !IncReadU64(raw, off, request.input_parent_batch_id) ||
        !IncReadU32(raw, off, output_parent_pid) ||
        !IncReadU64(raw, off, request.output_parent_batch_id) ||
        !IncReadU32(raw, off, right_node_id) || !IncReadU64(raw, off, request.stream_id) ||
        !IncReadU64(raw, off, request.expected_streams) ||
        !IncReadU32(raw, off, retain_output) || retain_output > 1 || off != raw.size()) {
        err = "Bad resident matrix continuation";
        return false;
    }
    request.query_id = static_cast<int>(query_id);
    request.input_parent_pid = static_cast<int>(input_parent_pid);
    request.output_parent_pid = static_cast<int>(output_parent_pid);
    request.right_node_id = static_cast<int>(right_node_id);
    request.retain_output_on_destination = retain_output != 0;
    return true;
}

static string SerializeIncrementalMatrixJoinResult(const IncrementalMatrixJoinResult& result) {
    string raw;
    IncAppendU32(raw, static_cast<uint32_t>(result.query_id));
    IncAppendU64(raw, result.token);
    IncAppendU64(raw, result.parent_batch_id);
    IncAppendU32(raw, result.global_exact_stage ? 1U : 0U);
    if (!result.factorized_rows && !result.defer_factorized_materialization) {
        IncAppendU64(raw, result.global_stage_id);
        IncAppendU32(raw, result.direct_right_stream ? 1U : 0U);
        IncAppendU32(raw, result.middle_partial ? 1U : 0U);
        IncAppendU32(raw, result.middle_complete ? 1U : 0U);
        IncAppendU32(raw, result.middle_compute_result ? 1U : 0U);
        IncAppendU32(raw, static_cast<uint32_t>(result.middle_partition));
        IncAppendU64(raw, result.middle_virtual_shard);
        IncAppendU64(raw, result.scheduled_join_work);
        IncAppendU32(raw, static_cast<uint32_t>(result.destination_pid));
        IncAppendSet(raw, result.output_sources);
        IncAppendU64(raw, result.output_rows);
        IncAppendU64(raw, result.output_pairs);
        IncAppendRows(raw, result.rows);
        return IncWrapRaw(IncrementalWireKind::MATRIX_JOIN_RESULT, raw);
    }
    size_t reserve_bytes = 128 + result.output_sources.bitmap.getSizeInBytes(true);
    for (const auto& [_, destinations] : result.rows) {
        reserve_bytes += 24 + destinations.bitmap.getSizeInBytes(true);
    }
    for (const auto& block : result.shared_blocks) {
        reserve_bytes += 40 + block.sources.bitmap.getSizeInBytes(true);
        if (!result.dictionary_coded_blocks ||
            block.destination_codec !=
                IncrementalMatrixJoinResult::SharedBlock::DestinationCodec::REFERENCE) {
            reserve_bytes += block.destinations.bitmap.getSizeInBytes(true);
        }
    }
    raw.clear();
    raw.reserve(reserve_bytes);
    IncAppendU32(raw, kIncrementalWireMagic);
    IncAppendU32(raw, kIncrementalWireVersion);
    IncAppendU32(
        raw,
        static_cast<uint32_t>(
            IncrementalWireKind::MATRIX_JOIN_FACTORIZED_RESULT));
    IncAppendU32(raw, static_cast<uint32_t>(result.query_id));
    IncAppendU64(raw, result.token);
    IncAppendU64(raw, result.parent_batch_id);
    IncAppendU32(raw, result.global_exact_stage ? 1U : 0U);
    IncAppendU32(raw, result.defer_factorized_materialization ? 1U : 0U);
    IncAppendU64(raw, result.global_stage_id);
    IncAppendU32(raw, result.direct_right_stream ? 1U : 0U);
    IncAppendU32(raw, result.middle_partial ? 1U : 0U);
    IncAppendU32(raw, result.middle_complete ? 1U : 0U);
    IncAppendU32(raw, result.middle_compute_result ? 1U : 0U);
    IncAppendU32(raw, static_cast<uint32_t>(result.middle_partition));
    IncAppendU64(raw, result.middle_virtual_shard);
    IncAppendU64(raw, result.scheduled_join_work);
    IncAppendU32(raw, static_cast<uint32_t>(result.destination_pid));
    IncAppendSet(raw, result.output_sources);
    IncAppendU64(raw, result.output_rows);
    IncAppendU64(raw, result.output_pairs);
    IncAppendU32(
        raw, result.factorized_rows
            ? (result.dictionary_coded_blocks ? 2U : 1U)
            : 0U);
    IncAppendRows(raw, result.rows);
    IncAppendU64(raw, static_cast<uint64_t>(result.shared_blocks.size()));
    for (const auto& block : result.shared_blocks) {
        IncAppendU64(raw, block.block_id);
        if (result.dictionary_coded_blocks) {
            IncAppendU32(raw, static_cast<uint32_t>(block.destination_codec));
            IncAppendU64(raw, block.dictionary_id);
            IncAppendU64(raw, block.base_dictionary_id);
        }
        IncAppendSet(raw, block.sources);
        if (!result.dictionary_coded_blocks ||
            block.destination_codec !=
            IncrementalMatrixJoinResult::SharedBlock::DestinationCodec::REFERENCE) {
            IncAppendSet(raw, block.destinations);
        }
    }
    // Result rows already use Roaring's portable bitmap encoding. Avoid an
    // additional whole-message zlib pass so ordinary and factorized layouts
    // retain their actual structural wire-size difference.
    return raw;
}

static bool ParseIncrementalMatrixJoinResult(
    const string& data, IncrementalMatrixJoinResult& result, string& err) {
    string raw;
    size_t off = 0;
    uint32_t wire_magic = 0, wire_version = 0, wire_kind = 0;
    const bool extended =
        IncReadU32(data, off, wire_magic) &&
        IncReadU32(data, off, wire_version) &&
        IncReadU32(data, off, wire_kind) &&
        wire_magic == kIncrementalWireMagic &&
        wire_version == kIncrementalWireVersion &&
        wire_kind == static_cast<uint32_t>(
            IncrementalWireKind::MATRIX_JOIN_FACTORIZED_RESULT);
    if (!extended) {
        if (!IncUnwrapRaw(
                data, IncrementalWireKind::MATRIX_JOIN_RESULT, raw, err)) {
            return false;
        }
        off = 0;
        uint32_t query_id = 0, global_exact_stage = 0;
        uint32_t direct_right = 0, middle_partial = 0;
        uint32_t middle_complete = 0, middle_compute_result = 0;
        uint32_t middle_partition = 0, destination_pid = 0;
        if (!IncReadU32(raw, off, query_id) ||
            !IncReadU64(raw, off, result.token) ||
            !IncReadU64(raw, off, result.parent_batch_id) ||
            !IncReadU32(raw, off, global_exact_stage) ||
            global_exact_stage > 1 ||
            !IncReadU64(raw, off, result.global_stage_id) ||
            !IncReadU32(raw, off, direct_right) || direct_right > 1 ||
            !IncReadU32(raw, off, middle_partial) || middle_partial > 1 ||
            !IncReadU32(raw, off, middle_complete) || middle_complete > 1 ||
            !IncReadU32(raw, off, middle_compute_result) ||
            middle_compute_result > 1 ||
            !IncReadU32(raw, off, middle_partition) ||
            !IncReadU64(raw, off, result.middle_virtual_shard) ||
            !IncReadU64(raw, off, result.scheduled_join_work) ||
            !IncReadU32(raw, off, destination_pid) ||
            !IncReadSet(raw, off, result.output_sources) ||
            !IncReadU64(raw, off, result.output_rows) ||
            !IncReadU64(raw, off, result.output_pairs) ||
            !IncReadRows(raw, off, result.rows) || off != raw.size()) {
            err = "Bad matrix join result";
            return false;
        }
        result.query_id = static_cast<int>(query_id);
        result.global_exact_stage = global_exact_stage != 0;
        result.defer_factorized_materialization = false;
        result.direct_right_stream = direct_right != 0;
        result.middle_partial = middle_partial != 0;
        result.middle_complete = middle_complete != 0;
        result.middle_compute_result = middle_compute_result != 0;
        result.factorized_rows = false;
        result.dictionary_coded_blocks = false;
        result.middle_partition = static_cast<int>(middle_partition);
        result.destination_pid = static_cast<int>(destination_pid);
        result.shared_blocks.clear();
        return true;
    }
    uint32_t query_id = 0, direct_right = 0, middle_partial = 0;
    uint32_t global_exact_stage = 0, defer_factorized_materialization = 0;
    uint32_t middle_complete = 0, middle_compute_result = 0;
    uint32_t factorized_rows = 0;
    uint32_t middle_partition = 0, destination_pid = 0;
    if (!IncReadU32(data, off, query_id) || !IncReadU64(data, off, result.token) ||
        !IncReadU64(data, off, result.parent_batch_id) ||
        !IncReadU32(data, off, global_exact_stage) || global_exact_stage > 1 ||
        !IncReadU32(data, off, defer_factorized_materialization) ||
        defer_factorized_materialization > 1 ||
        !IncReadU64(data, off, result.global_stage_id) ||
        !IncReadU32(data, off, direct_right) || direct_right > 1 ||
        !IncReadU32(data, off, middle_partial) || middle_partial > 1 ||
        !IncReadU32(data, off, middle_complete) || middle_complete > 1 ||
        !IncReadU32(data, off, middle_compute_result) || middle_compute_result > 1 ||
        !IncReadU32(data, off, middle_partition) ||
        !IncReadU64(data, off, result.middle_virtual_shard) ||
        !IncReadU64(data, off, result.scheduled_join_work) ||
        !IncReadU32(data, off, destination_pid) ||
        !IncReadSet(data, off, result.output_sources) ||
        !IncReadU64(data, off, result.output_rows) ||
        !IncReadU64(data, off, result.output_pairs) ||
        !IncReadU32(data, off, factorized_rows) || factorized_rows > 2 ||
        !IncReadRows(data, off, result.rows)) {
        err = "Bad matrix join result";
        return false;
    }
    uint64_t shared_count = 0;
    if (!IncReadU64(data, off, shared_count) ||
        shared_count > (1ULL << 32)) {
        err = "Bad matrix join shared block count";
        return false;
    }
    result.shared_blocks.clear();
    result.shared_blocks.reserve(static_cast<size_t>(
        min<uint64_t>(shared_count, 1ULL << 20)));
    for (uint64_t index = 0; index < shared_count; ++index) {
        IncrementalMatrixJoinResult::SharedBlock block;
        uint32_t destination_codec = 0;
        if (!IncReadU64(data, off, block.block_id)) {
            err = "Bad matrix join shared block";
            return false;
        }
        if (factorized_rows == 2 &&
            (!IncReadU32(data, off, destination_codec) ||
             destination_codec > static_cast<uint32_t>(
                 IncrementalMatrixJoinResult::SharedBlock::
                     DestinationCodec::XOR_DELTA) ||
             !IncReadU64(data, off, block.dictionary_id) ||
             !IncReadU64(data, off, block.base_dictionary_id))) {
            err = "Bad matrix join dictionary block";
            return false;
        }
        block.destination_codec = static_cast<
            IncrementalMatrixJoinResult::SharedBlock::DestinationCodec>(
                destination_codec);
        if (!IncReadSet(data, off, block.sources)) {
            err = "Bad matrix join shared block sources";
            return false;
        }
        if ((factorized_rows != 2 ||
             block.destination_codec !=
                 IncrementalMatrixJoinResult::SharedBlock::
                     DestinationCodec::REFERENCE) &&
            !IncReadSet(data, off, block.destinations)) {
            err = "Bad matrix join shared block destinations";
            return false;
        }
        result.shared_blocks.push_back(std::move(block));
    }
    if (off != data.size()) {
        err = "Trailing matrix join result bytes";
        return false;
    }
    result.query_id = static_cast<int>(query_id);
    result.global_exact_stage = global_exact_stage != 0;
    result.defer_factorized_materialization =
        defer_factorized_materialization != 0;
    result.direct_right_stream = direct_right != 0;
    result.middle_partial = middle_partial != 0;
    result.middle_complete = middle_complete != 0;
    result.middle_compute_result = middle_compute_result != 0;
    result.factorized_rows = factorized_rows != 0;
    result.dictionary_coded_blocks = factorized_rows == 2;
    result.middle_partition = static_cast<int>(middle_partition);
    result.destination_pid = static_cast<int>(destination_pid);
    return true;
}
struct IncrementalRowKeyHash {
    size_t operator()(const IncrementalRowKey& key) const {
        size_t h = hash<uint64_t>{}(key.source); h ^= hash<int>{}(key.query_id + 0x9e3779b9 + static_cast<int>(h)); h ^= hash<int>{}(key.body_id * 1315423911); h ^= hash<int>{}(key.length * 2654435761U); return h;
    }
};

static string SerializeIncrementalRowRequest(const IncrementalRowKey& key, int reply_pid, uint64_t token) {
    string raw; IncAppendU32(raw, key.query_id); IncAppendU32(raw, key.body_id); IncAppendU32(raw, key.length); IncAppendU64(raw, key.source); IncAppendU32(raw, reply_pid); IncAppendU64(raw, token); return IncWrap(IncrementalWireKind::ROW_REQUEST, raw);
}
static bool ParseIncrementalRowRequest(const string& data, IncrementalRowKey& key, int& reply_pid, uint64_t& token, string& err) {
    string raw; if (!IncUnwrap(data, IncrementalWireKind::ROW_REQUEST, raw, err)) return false; size_t off = 0; uint32_t q = 0, b = 0, l = 0, p = 0;
    if (!IncReadU32(raw, off, q) || !IncReadU32(raw, off, b) || !IncReadU32(raw, off, l) || !IncReadU64(raw, off, key.source) || !IncReadU32(raw, off, p) || !IncReadU64(raw, off, token)) { err = "Bad row request"; return false; }
    key.query_id = static_cast<int>(q); key.body_id = static_cast<int>(b); key.length = static_cast<int>(l); reply_pid = static_cast<int>(p); return true;
}
struct IncrementalRowRequestEnvelope {
    IncrementalRowKey key;
    int reply_pid = -1;
    uint64_t token = 0;
};
static string SerializeIncrementalRowRequestBatch(const vector<IncrementalRowRequestEnvelope>& requests) {
    string raw;
    IncAppendU64(raw, requests.size());
    for (const auto& request : requests) {
        IncAppendU32(raw, request.key.query_id);
        IncAppendU32(raw, request.key.body_id);
        IncAppendU32(raw, request.key.length);
        IncAppendU64(raw, request.key.source);
        IncAppendU32(raw, request.reply_pid);
        IncAppendU64(raw, request.token);
    }
    return IncWrap(IncrementalWireKind::ROW_REQUEST_BATCH, raw);
}
static bool ParseIncrementalRowRequestBatch(
    const string& data, vector<IncrementalRowRequestEnvelope>& requests, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::ROW_REQUEST_BATCH, raw, err)) return false;
    size_t off = 0;
    uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > 65536) {
        err = "Bad row request batch count";
        return false;
    }
    requests.clear();
    requests.reserve(static_cast<size_t>(count));
    for (uint64_t index = 0; index < count; ++index) {
        uint32_t query_id = 0, body_id = 0, length = 0, reply_pid = 0;
        IncrementalRowRequestEnvelope request;
        if (!IncReadU32(raw, off, query_id) ||
            !IncReadU32(raw, off, body_id) ||
            !IncReadU32(raw, off, length) ||
            !IncReadU64(raw, off, request.key.source) ||
            !IncReadU32(raw, off, reply_pid) ||
            !IncReadU64(raw, off, request.token)) {
            err = "Bad row request batch";
            return false;
        }
        request.key.query_id = static_cast<int>(query_id);
        request.key.body_id = static_cast<int>(body_id);
        request.key.length = static_cast<int>(length);
        request.reply_pid = static_cast<int>(reply_pid);
        requests.push_back(std::move(request));
    }
    return off == raw.size();
}
static string SerializeIncrementalRowReady(const IncrementalRowKey& key, uint64_t token, const RelationDstSet& values) {
    string raw; IncAppendU32(raw, key.query_id); IncAppendU32(raw, key.body_id); IncAppendU32(raw, key.length); IncAppendU64(raw, key.source); IncAppendU64(raw, token); IncAppendSet(raw, values); return IncWrap(IncrementalWireKind::ROW_READY, raw);
}
static bool ParseIncrementalRowReady(const string& data, IncrementalRowKey& key, uint64_t& token, RelationDstSet& values, string& err) {
    string raw; if (!IncUnwrap(data, IncrementalWireKind::ROW_READY, raw, err)) return false; size_t off = 0; uint32_t q = 0, b = 0, l = 0;
    if (!IncReadU32(raw, off, q) || !IncReadU32(raw, off, b) || !IncReadU32(raw, off, l) || !IncReadU64(raw, off, key.source) || !IncReadU64(raw, off, token) || !IncReadSet(raw, off, values)) { err = "Bad row ready"; return false; }
    key.query_id = static_cast<int>(q); key.body_id = static_cast<int>(b); key.length = static_cast<int>(l); return true;
}

static string SerializeIncrementalRowDelta(const IncrementalRowKey& key, uint64_t token, const RelationDstSet& values) {
    string raw; IncAppendU32(raw, key.query_id); IncAppendU32(raw, key.body_id); IncAppendU32(raw, key.length); IncAppendU64(raw, key.source); IncAppendU64(raw, token); IncAppendSet(raw, values); return IncWrap(IncrementalWireKind::ROW_DELTA, raw);
}
static bool ParseIncrementalRowDelta(const string& data, IncrementalRowKey& key, uint64_t& token, RelationDstSet& values, string& err) {
    string raw; if (!IncUnwrap(data, IncrementalWireKind::ROW_DELTA, raw, err)) return false; size_t off = 0; uint32_t q = 0, b = 0, l = 0;
    if (!IncReadU32(raw, off, q) || !IncReadU32(raw, off, b) || !IncReadU32(raw, off, l) || !IncReadU64(raw, off, key.source) || !IncReadU64(raw, off, token) || !IncReadSet(raw, off, values)) { err = "Bad row delta"; return false; }
    key.query_id = static_cast<int>(q); key.body_id = static_cast<int>(b); key.length = static_cast<int>(l); return true;
}
struct IncrementalRowDeltaEnvelope { IncrementalRowKey key; uint64_t token = 0; RelationDstSet values; };
static string SerializeIncrementalRowDeltaBatch(const vector<IncrementalRowDeltaEnvelope>& events) {
    string raw; IncAppendU64(raw, events.size());
    for (const auto& event : events) {
        IncAppendU32(raw, event.key.query_id); IncAppendU32(raw, event.key.body_id); IncAppendU32(raw, event.key.length);
        IncAppendU64(raw, event.key.source); IncAppendU64(raw, event.token); IncAppendSet(raw, event.values);
    }
    return IncWrap(IncrementalWireKind::ROW_DELTA_BATCH, raw);
}
static bool ParseIncrementalRowDeltaBatch(const string& data, vector<IncrementalRowDeltaEnvelope>& events, string& err) {
    string raw; if (!IncUnwrap(data, IncrementalWireKind::ROW_DELTA_BATCH, raw, err)) return false;
    size_t off = 0; uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > 65536) { err = "Bad row delta batch count"; return false; }
    events.clear(); events.reserve(static_cast<size_t>(count));
    for (uint64_t index = 0; index < count; ++index) {
        uint32_t q = 0, b = 0, l = 0; IncrementalRowDeltaEnvelope event;
        if (!IncReadU32(raw, off, q) || !IncReadU32(raw, off, b) || !IncReadU32(raw, off, l) ||
            !IncReadU64(raw, off, event.key.source) || !IncReadU64(raw, off, event.token) ||
            !IncReadSet(raw, off, event.values)) { err = "Bad row delta batch"; return false; }
        event.key.query_id = static_cast<int>(q); event.key.body_id = static_cast<int>(b); event.key.length = static_cast<int>(l);
        events.push_back(std::move(event));
    }
    return off == raw.size();
}
// Keep DELTA and COMPLETE in one ordered peer stream.  A row completion must
// follow its own deltas, but it does not require flushing unrelated deltas.
struct IncrementalRowStreamEnvelope {
    bool complete = false;
    IncrementalRowKey key;
    uint64_t token = 0;
    RelationDstSet values;
};
static string SerializeIncrementalRowStreamBatch(const vector<IncrementalRowStreamEnvelope>& events) {
    string raw;
    IncAppendU64(raw, events.size());
    for (const auto& event : events) {
        IncAppendU32(raw, event.complete ? 1U : 0U);
        IncAppendU32(raw, event.key.query_id); IncAppendU32(raw, event.key.body_id);
        IncAppendU32(raw, event.key.length); IncAppendU64(raw, event.key.source);
        IncAppendU64(raw, event.token);
        if (!event.complete) IncAppendSet(raw, event.values);
    }
    return IncWrap(IncrementalWireKind::ROW_STREAM_BATCH, raw);
}
static bool ParseIncrementalRowStreamBatch(
    const string& data, vector<IncrementalRowStreamEnvelope>& events, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::ROW_STREAM_BATCH, raw, err)) return false;
    size_t off = 0;
    uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > 65536) {
        err = "Bad row stream batch count";
        return false;
    }
    events.clear();
    events.reserve(static_cast<size_t>(count));
    for (uint64_t index = 0; index < count; ++index) {
        uint32_t complete = 0, query_id = 0, body_id = 0, length = 0;
        IncrementalRowStreamEnvelope event;
        if (!IncReadU32(raw, off, complete) || complete > 1 ||
            !IncReadU32(raw, off, query_id) || !IncReadU32(raw, off, body_id) ||
            !IncReadU32(raw, off, length) || !IncReadU64(raw, off, event.key.source) ||
            !IncReadU64(raw, off, event.token)) {
            err = "Bad row stream batch";
            return false;
        }
        event.complete = complete != 0;
        event.key.query_id = static_cast<int>(query_id);
        event.key.body_id = static_cast<int>(body_id);
        event.key.length = static_cast<int>(length);
        if (!event.complete && !IncReadSet(raw, off, event.values)) {
            err = "Bad row stream delta";
            return false;
        }
        events.push_back(std::move(event));
    }
    return off == raw.size();
}
static string SerializeIncrementalRowComplete(const IncrementalRowKey& key, uint64_t token) {
    string raw; IncAppendU32(raw, key.query_id); IncAppendU32(raw, key.body_id); IncAppendU32(raw, key.length); IncAppendU64(raw, key.source); IncAppendU64(raw, token); return IncWrap(IncrementalWireKind::ROW_COMPLETE, raw);
}
static bool ParseIncrementalRowComplete(const string& data, IncrementalRowKey& key, uint64_t& token, string& err) {
    string raw; if (!IncUnwrap(data, IncrementalWireKind::ROW_COMPLETE, raw, err)) return false; size_t off = 0; uint32_t q = 0, b = 0, l = 0;
    if (!IncReadU32(raw, off, q) || !IncReadU32(raw, off, b) || !IncReadU32(raw, off, l) || !IncReadU64(raw, off, key.source) || !IncReadU64(raw, off, token)) { err = "Bad row complete"; return false; }
    key.query_id = static_cast<int>(q); key.body_id = static_cast<int>(b); key.length = static_cast<int>(l); return true;
}
struct IncrementalRowCompleteEnvelope { IncrementalRowKey key; uint64_t token = 0; };
static string SerializeIncrementalRowCompleteBatch(const vector<IncrementalRowCompleteEnvelope>& events) {
    string raw;
    IncAppendU64(raw, events.size());
    for (const auto& event : events) {
        IncAppendU32(raw, event.key.query_id); IncAppendU32(raw, event.key.body_id);
        IncAppendU32(raw, event.key.length); IncAppendU64(raw, event.key.source);
        IncAppendU64(raw, event.token);
    }
    return IncWrap(IncrementalWireKind::ROW_COMPLETE_BATCH, raw);
}
static bool ParseIncrementalRowCompleteBatch(
    const string& data, vector<IncrementalRowCompleteEnvelope>& events, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::ROW_COMPLETE_BATCH, raw, err)) return false;
    size_t off = 0;
    uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > 65536) {
        err = "Bad row complete batch count";
        return false;
    }
    events.clear();
    events.reserve(static_cast<size_t>(count));
    for (uint64_t index = 0; index < count; ++index) {
        uint32_t query_id = 0, body_id = 0, length = 0;
        IncrementalRowCompleteEnvelope event;
        if (!IncReadU32(raw, off, query_id) || !IncReadU32(raw, off, body_id) ||
            !IncReadU32(raw, off, length) || !IncReadU64(raw, off, event.key.source) ||
            !IncReadU64(raw, off, event.token)) {
            err = "Bad row complete batch";
            return false;
        }
        event.key.query_id = static_cast<int>(query_id);
        event.key.body_id = static_cast<int>(body_id);
        event.key.length = static_cast<int>(length);
        events.push_back(std::move(event));
    }
    return off == raw.size();
}

struct IncrementalBaseEvent { IncrementalRowKey key; int origin_pid = 0; uint64_t vertex = 0; int state_id = 0; };
struct IncrementalBaseResult { IncrementalRowKey key; RelationDstSet accepted; vector<pair<uint64_t, int>> children; };

static string SerializeIncrementalBaseExpand(const IncrementalBaseEvent& event) {
    string raw;
    IncAppendU32(raw, event.key.query_id);
    IncAppendU32(raw, event.key.body_id);
    IncAppendU64(raw, event.key.source);
    IncAppendU32(raw, event.origin_pid);
    IncAppendU64(raw, event.vertex);
    IncAppendU32(raw, event.state_id);
    return IncWrap(IncrementalWireKind::BASE_EXPAND, raw);
}

static bool ParseIncrementalBaseExpand(
    const string& data, IncrementalBaseEvent& event, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::BASE_EXPAND, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, body_id = 0, origin_pid = 0, state_id = 0;
    if (!IncReadU32(raw, off, query_id) ||
        !IncReadU32(raw, off, body_id) ||
        !IncReadU64(raw, off, event.key.source) ||
        !IncReadU32(raw, off, origin_pid) ||
        !IncReadU64(raw, off, event.vertex) ||
        !IncReadU32(raw, off, state_id) || off != raw.size()) {
        err = "Bad base expand";
        return false;
    }
    event.key.query_id = static_cast<int>(query_id);
    event.key.body_id = static_cast<int>(body_id);
    event.key.length = 1;
    event.origin_pid = static_cast<int>(origin_pid);
    event.state_id = static_cast<int>(state_id);
    return true;
}

static string SerializeIncrementalBaseResult(const IncrementalBaseResult& result) {
    string raw;
    IncAppendU32(raw, result.key.query_id);
    IncAppendU32(raw, result.key.body_id);
    IncAppendU64(raw, result.key.source);
    IncAppendSet(raw, result.accepted);
    IncAppendU64(raw, result.children.size());
    for (const auto& [vertex, state_id] : result.children) {
        IncAppendU64(raw, vertex);
        IncAppendU32(raw, state_id);
    }
    return IncWrap(IncrementalWireKind::BASE_RESULT, raw);
}

static bool ParseIncrementalBaseResult(
    const string& data, IncrementalBaseResult& result, string& err) {
    string raw;
    if (!IncUnwrap(data, IncrementalWireKind::BASE_RESULT, raw, err)) return false;
    size_t off = 0;
    uint32_t query_id = 0, body_id = 0;
    uint64_t child_count = 0;
    if (!IncReadU32(raw, off, query_id) ||
        !IncReadU32(raw, off, body_id) ||
        !IncReadU64(raw, off, result.key.source) ||
        !IncReadSet(raw, off, result.accepted) ||
        !IncReadU64(raw, off, child_count) ||
        child_count > (raw.size() - off) / 12) {
        err = "Bad base result";
        return false;
    }
    result.key.query_id = static_cast<int>(query_id);
    result.key.body_id = static_cast<int>(body_id);
    result.key.length = 1;
    result.children.clear();
    result.children.reserve(static_cast<size_t>(child_count));
    for (uint64_t index = 0; index < child_count; ++index) {
        uint64_t vertex = 0;
        uint32_t state_id = 0;
        if (!IncReadU64(raw, off, vertex) || !IncReadU32(raw, off, state_id)) {
            err = "Bad base result child";
            return false;
        }
        result.children.emplace_back(vertex, static_cast<int>(state_id));
    }
    if (off != raw.size()) {
        err = "Trailing base result";
        return false;
    }
    return true;
}

static string SerializeIncrementalBaseExpandBatch(
    const vector<IncrementalBaseEvent>& events) {
    string raw;
    IncAppendU64(raw, static_cast<uint64_t>(events.size()));
    for (const auto& event : events) {
        IncAppendU32(raw, event.key.query_id);
        IncAppendU32(raw, event.key.body_id);
        IncAppendU64(raw, event.key.source);
        IncAppendU32(raw, event.origin_pid);
        IncAppendU64(raw, event.vertex);
        IncAppendU32(raw, event.state_id);
    }
    return IncWrap(IncrementalWireKind::BASE_EXPAND_BATCH, raw);
}

static bool ParseIncrementalBaseExpandBatch(
    const string& data, vector<IncrementalBaseEvent>& events, string& err) {
    string raw;
    if (!IncUnwrap(
            data, IncrementalWireKind::BASE_EXPAND_BATCH, raw, err)) {
        return false;
    }
    size_t off = 0;
    uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > (1ULL << 32)) {
        err = "Bad base expand batch count";
        return false;
    }
    events.clear();
    events.reserve(static_cast<size_t>(min<uint64_t>(count, 1ULL << 20)));
    for (uint64_t index = 0; index < count; ++index) {
        IncrementalBaseEvent event;
        uint32_t query_id = 0, body_id = 0, origin_pid = 0, state_id = 0;
        if (!IncReadU32(raw, off, query_id) ||
            !IncReadU32(raw, off, body_id) ||
            !IncReadU64(raw, off, event.key.source) ||
            !IncReadU32(raw, off, origin_pid) ||
            !IncReadU64(raw, off, event.vertex) ||
            !IncReadU32(raw, off, state_id)) {
            err = "Bad base expand batch event";
            return false;
        }
        event.key.query_id = static_cast<int>(query_id);
        event.key.body_id = static_cast<int>(body_id);
        event.key.length = 1;
        event.origin_pid = static_cast<int>(origin_pid);
        event.state_id = static_cast<int>(state_id);
        events.push_back(std::move(event));
    }
    if (off != raw.size()) {
        err = "Trailing base expand batch";
        return false;
    }
    return true;
}

static string SerializeIncrementalBaseResultBatch(
    const vector<IncrementalBaseResult>& results) {
    string raw;
    IncAppendU64(raw, static_cast<uint64_t>(results.size()));
    for (const auto& result : results) {
        IncAppendU32(raw, result.key.query_id);
        IncAppendU32(raw, result.key.body_id);
        IncAppendU64(raw, result.key.source);
        IncAppendSet(raw, result.accepted);
        IncAppendU64(raw, static_cast<uint64_t>(result.children.size()));
        for (const auto& [vertex, state_id] : result.children) {
            IncAppendU64(raw, vertex);
            IncAppendU32(raw, state_id);
        }
    }
    return IncWrap(IncrementalWireKind::BASE_RESULT_BATCH, raw);
}

static bool ParseIncrementalBaseResultBatch(
    const string& data, vector<IncrementalBaseResult>& results, string& err) {
    string raw;
    if (!IncUnwrap(
            data, IncrementalWireKind::BASE_RESULT_BATCH, raw, err)) {
        return false;
    }
    size_t off = 0;
    uint64_t count = 0;
    if (!IncReadU64(raw, off, count) || count > (1ULL << 32)) {
        err = "Bad base result batch count";
        return false;
    }
    results.clear();
    results.reserve(static_cast<size_t>(min<uint64_t>(count, 1ULL << 20)));
    for (uint64_t index = 0; index < count; ++index) {
        IncrementalBaseResult result;
        uint32_t query_id = 0, body_id = 0;
        if (!IncReadU32(raw, off, query_id) ||
            !IncReadU32(raw, off, body_id) ||
            !IncReadU64(raw, off, result.key.source) ||
            !IncReadSet(raw, off, result.accepted)) {
            err = "Bad base result batch row";
            return false;
        }
        uint64_t children = 0;
        if (!IncReadU64(raw, off, children) ||
            children > (raw.size() - off) / 12) {
            err = "Bad base result batch children";
            return false;
        }
        result.children.reserve(static_cast<size_t>(children));
        for (uint64_t child = 0; child < children; ++child) {
            uint64_t vertex = 0;
            uint32_t state_id = 0;
            if (!IncReadU64(raw, off, vertex) ||
                !IncReadU32(raw, off, state_id)) {
                err = "Bad base result batch child";
                return false;
            }
            result.children.emplace_back(vertex, static_cast<int>(state_id));
        }
        result.key.query_id = static_cast<int>(query_id);
        result.key.body_id = static_cast<int>(body_id);
        result.key.length = 1;
        results.push_back(std::move(result));
    }
    if (off != raw.size()) {
        err = "Trailing base result batch";
        return false;
    }
    return true;
}

enum class IncrementalFinalRowsCodec : uint32_t {
    ROWS = 1,
    PATTERN_GROUPS = 2,
    CHUNKS = 3,
};

struct IncrementalFinalPatternGroup {
    RelationDstSet sources;
    RelationDstSet destinations;
};


static string SerializeIncrementalFinalRowsSerial(
    const RelationRows& rows, const vector<uint64_t>* selected = nullptr) {
    const auto trace_begin = chrono::steady_clock::now();
    vector<const RelationRows::value_type*> sources;
    if (selected != nullptr) {
        sources.reserve(selected->size());
        for (uint64_t source : *selected) {
            const auto found = rows.find(source);
            if (found == rows.end()) throw out_of_range("missing selected final row");
            sources.push_back(&*found);
        }
    } else {
        sources.reserve(rows.size());
        for (const auto& row : rows) sources.push_back(&row);
    }
    sort(sources.begin(), sources.end(), [](const auto* a, const auto* b) {
        return a->first < b->first;
    });

    const size_t header_bytes = 12 + 4 + 8;
    size_t row_bytes = header_bytes;
    // Closure and repeated-count queries often contain many source rows with
    // exactly the same destination bitmap. Encode one destination bitmap plus
    // a Roaring source group in that case. The source bitmap is especially
    // compact for partition-owned source ranges.
    vector<IncrementalFinalPatternGroup> groups;
    unordered_map<string, size_t> group_by_bitmap;
    struct StoredGroup { size_t index; size_t bytes; };
    unordered_map<const void*, StoredGroup> group_by_storage;
    for (const auto* row : sources) {
        const uint64_t source = row->first;
        const auto& values = row->second;
        // Immutable aliases have identical portable bytes for this call.
        const void* identity = values.bitmap.identity();
        const auto cached = group_by_storage.find(identity);
        if (cached != group_by_storage.end()) {
            row_bytes += 16 + cached->second.bytes;
            groups[cached->second.index].sources.push_back(source);
            continue;
        }
        const size_t bytes = values.bitmap.getSizeInBytes(true);
        row_bytes += 16 + bytes;
        string key(bytes, '\0');
        values.bitmap.write(key.data(), true);
        auto [it, inserted] = group_by_bitmap.emplace(std::move(key), groups.size());
        if (inserted) {
            IncrementalFinalPatternGroup group;
            group.destinations = values;
            groups.push_back(std::move(group));
        }
        group_by_storage.emplace(identity, StoredGroup{it->second, bytes});
        groups[it->second].sources.push_back(source);
    }

    size_t grouped_bytes = header_bytes;
    for (const auto& group : groups) {
        grouped_bytes += 8 + group.sources.bitmap.getSizeInBytes(true);
        grouped_bytes += 8 + group.destinations.bitmap.getSizeInBytes(true);
    }
    const bool use_groups = grouped_bytes < row_bytes;

    const auto trace_grouped = chrono::steady_clock::now();
    string raw;
    IncAppendU64(raw, use_groups
        ? static_cast<uint64_t>(groups.size())
        : static_cast<uint64_t>(sources.size()));
    if (use_groups) {
        for (const auto& group : groups) {
            IncAppendSet(raw, group.sources);
            IncAppendSet(raw, group.destinations);
        }
    } else {
        for (const auto* row : sources) {
            IncAppendU64(raw, row->first);
            IncAppendSet(raw, row->second);
        }
    }

    string header;
    IncAppendU32(header, kIncrementalWireMagic);
    IncAppendU32(header, kIncrementalWireVersion);
    IncAppendU32(header, static_cast<uint32_t>(IncrementalWireKind::RESULTS));
    IncAppendU32(header, static_cast<uint32_t>(use_groups
        ? IncrementalFinalRowsCodec::PATTERN_GROUPS
        : IncrementalFinalRowsCodec::ROWS));
    header.append(raw);
    const auto trace_encoded = chrono::steady_clock::now();
    // Final RESULTS already hold portable Roaring bitmaps and exact groups;
    // deflate saved ~3% on measured DB/YAGO replies while sitting on the
    // slowest worker's fetch path. Use the existing stored (flag 0) envelope,
    // which every decoder already accepts. Content and grouping are unchanged.
    string wire;
    wire.reserve(20 + header.size());
    AppendRawU32(wire, kCompressedPayloadMagic);
    AppendRawU32(wire, kCompressedPayloadVersion);
    AppendRawU32(wire, 0u);
    AppendRawU64(wire, static_cast<uint64_t>(header.size()));
    wire.append(header);
    if (IncrementalScalingTraceEnabled()) {
        const auto end = chrono::steady_clock::now();
        ostringstream line;
        line << "[scaling-final-encode] rows=" << rows.size()
             << " groups=" << groups.size()
             << " storage_identities=" << group_by_storage.size()
             << " row_bytes=" << row_bytes
             << " raw_bytes=" << header.size() << " wire_bytes=" << wire.size()
             << " grouping_us=" << chrono::duration_cast<chrono::microseconds>(trace_grouped-trace_begin).count()
             << " encode_us=" << chrono::duration_cast<chrono::microseconds>(trace_encoded-trace_grouped).count()
             << " compress_us=" << chrono::duration_cast<chrono::microseconds>(end-trace_encoded).count();
        cerr << line.str() << '\n';
    }
    return wire;
}

static size_t IncrementalFinalThreads() {
    return rpq_final::kFinalThreads;
}

template<class Function>
static void IncrementalFinalParallel(size_t count, Function work) {
    // One shared budget also bounds coordinator decode across concurrent RPCs.
    static atomic<size_t> active{0};
    atomic<size_t> next{0};
    auto run = [&] {
        for (size_t index; (index = next.fetch_add(1)) < count;) work(index);
    };
    vector<future<void>> tasks;
    const size_t limit = IncrementalFinalThreads();
    for (size_t lane = 1; lane < min(count, limit); ++lane) {
        size_t used = active.load();
        while (used < limit - 1 && !active.compare_exchange_weak(used, used + 1)) {}
        if (used >= limit - 1) break;
        try {
            tasks.push_back(async(launch::async, [&] {
                struct Release { atomic<size_t>& counter; ~Release() { --counter; } } release{active};
                run();
            }));
        } catch (...) { --active; throw; }
    }
    run();
    for (auto& task : tasks) task.get();
}

static string SerializeIncrementalFinalRows(const RelationRows& rows) {
    if (IncrementalFinalThreads() == 1 || rows.size() < 2) {
        return SerializeIncrementalFinalRowsSerial(rows);
    }
    size_t bytes = 0;
    vector<uint64_t> sources;
    for (const auto& [source, targets] : rows) {
        sources.push_back(source);
        bytes += 16 + targets.bitmap.getSizeInBytes(true);
    }
    const char* threshold = getenv("INCREMENTAL_FINAL_CHUNK_BYTES");
    const size_t chunk_bytes = threshold == nullptr ? (1ULL << 20)
        : max<size_t>(1024, strtoull(threshold, nullptr, 10));
    if (bytes <= chunk_bytes) return SerializeIncrementalFinalRowsSerial(rows);
    sort(sources.begin(), sources.end());
    const size_t count = min<size_t>(32, min(rows.size(), 1 + (bytes - 1) / chunk_bytes));
    const size_t target_bytes = 1 + (bytes - 1) / count;
    vector<vector<uint64_t>> selected(1);
    size_t current_bytes = 0;
    for (uint64_t source : sources) {
        if (current_bytes >= target_bytes && selected.size() < count) {
            selected.emplace_back();
            current_bytes = 0;
        }
        selected.back().push_back(source);
        current_bytes += 16 + rows.at(source).bitmap.getSizeInBytes(true);
    }
    vector<string> chunks(selected.size());
    IncrementalFinalParallel(chunks.size(), [&](size_t index) {
        chunks[index] = SerializeIncrementalFinalRowsSerial(rows, &selected[index]);
    });
    string raw;
    IncAppendU32(raw, kIncrementalWireMagic);
    IncAppendU32(raw, kIncrementalWireVersion);
    IncAppendU32(raw, static_cast<uint32_t>(IncrementalWireKind::RESULTS));
    IncAppendU32(raw, static_cast<uint32_t>(IncrementalFinalRowsCodec::CHUNKS));
    IncAppendU64(raw, chunks.size());
    for (const auto& chunk : chunks) {
        IncAppendU64(raw, chunk.size());
        raw.append(chunk);
    }
    // Each chunk is already independently compressed. Do not recompress it.
    string wire;
    AppendRawU32(wire, kCompressedPayloadMagic);
    AppendRawU32(wire, kCompressedPayloadVersion);
    AppendRawU32(wire, 0);
    AppendRawU64(wire, raw.size());
    wire.append(raw);
    return wire;
}

static bool ParseIncrementalFinalRowsImpl(
    const string& data, RelationRows& rows, string& err, bool allow_chunks) {
    rows.clear();
    string raw;
    if (!UnwrapCompressedPayload(data, raw, err)) return false;
    size_t off = 0;
    uint32_t magic = 0, version = 0, kind = 0, codec_value = 0;
    uint64_t count = 0;
    if (!IncReadU32(raw, off, magic) || !IncReadU32(raw, off, version) ||
        !IncReadU32(raw, off, kind) || !IncReadU32(raw, off, codec_value) ||
        !IncReadU64(raw, off, count) || magic != kIncrementalWireMagic ||
        version != kIncrementalWireVersion ||
        kind != static_cast<uint32_t>(IncrementalWireKind::RESULTS)) {
        err = "Bad incremental final rows";
        return false;
    }
    const auto codec = static_cast<IncrementalFinalRowsCodec>(codec_value);
    if (codec == IncrementalFinalRowsCodec::CHUNKS) {
        if (!allow_chunks || count == 0 || count > 32) {
            err = "Bad incremental final chunk count";
            return false;
        }
        vector<string> chunks;
        for (uint64_t index = 0; index < count; ++index) {
            uint64_t bytes = 0;
            if (!IncReadU64(raw, off, bytes) || bytes > raw.size() - off) {
                err = "Bad incremental final chunk length";
                return false;
            }
            chunks.emplace_back(raw.data() + off, static_cast<size_t>(bytes));
            off += static_cast<size_t>(bytes);
        }
        vector<RelationRows> decoded(count);
        vector<string> errors(count);
        vector<uint8_t> valid(count);
        IncrementalFinalParallel(count, [&](size_t index) {
            valid[index] = ParseIncrementalFinalRowsImpl(
                chunks[index], decoded[index], errors[index], false);
        });
        size_t total_rows = 0;
        for (const auto& chunk : decoded) total_rows += chunk.size();
        rows.reserve(total_rows);
        for (size_t index = 0; index < count; ++index) {
            if (!valid[index]) { err = errors[index]; return false; }
            rows.merge(decoded[index]);
            if (!decoded[index].empty()) {
                err = "Overlapping incremental final chunks";
                return false;
            }
        }
    } else if (codec == IncrementalFinalRowsCodec::ROWS) {
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t source = 0;
            RelationDstSet values;
            if (!IncReadU64(raw, off, source) || !IncReadSet(raw, off, values)) {
                err = "Bad incremental final row";
                return false;
            }
            if (!values.empty()) rows.emplace(source, std::move(values));
        }
    } else if (codec == IncrementalFinalRowsCodec::PATTERN_GROUPS) {
        vector<IncrementalFinalPatternGroup> groups;
        size_t total_sources = 0;
        for (uint64_t i = 0; i < count; ++i) {
            IncrementalFinalPatternGroup group;
            if (!IncReadSet(raw, off, group.sources) ||
                !IncReadSet(raw, off, group.destinations)) {
                err = "Bad incremental final pattern group";
                return false;
            }
            const uint64_t sources = group.sources.cardinality();
            if (sources > rows.max_size() - total_sources) {
                err = "Incremental final row count overflow";
                return false;
            }
            total_sources += static_cast<size_t>(sources);
            groups.push_back(std::move(group));
        }
        // Group cardinalities describe the full capacity before insertion.
        rows.reserve(total_sources);
        for (const auto& group : groups) {
            for (uint64_t source : group.sources)
                rows.emplace(source, group.destinations);
        }
    } else {
        err = "Unknown incremental final result codec";
        return false;
    }
    if (off != raw.size()) {
        err = "Trailing incremental final result bytes";
        return false;
    }
    return true;
}

static bool ParseIncrementalFinalRows(
    const string& data, RelationRows& rows, string& err) {
    return ParseIncrementalFinalRowsImpl(data, rows, err, true);
}
