struct RegexBodyNfaTransition {
    int to = -1;
    string label;
};

struct RegexBodyNfaState {
    vector<int> eps;
    vector<RegexBodyNfaTransition> labeled;
};

struct RegexBodyNfa {
    vector<RegexBodyNfaState> states;
    int start = -1;
    int accept = -1;
    vector<int> start_closure;
    vector<vector<int>> closure_by_state;
};

static vector<int> EpsilonClosure(const RegexBodyNfa& nfa, const vector<int>& seeds);

struct RegexQueryPattern {
    string normalized_body;
    int min_rep = 0;
    int max_rep = 0;  // -1 means open range
    RegexBodyNfa nfa;
};

class RegexBodyParser {
public:
    explicit RegexBodyParser(const string& input) : input_(input) {}

    bool Parse(RegexBodyNfa& out, string& err) {
        if (input_.empty()) {
            err = "Empty body";
            return false;
        }
        Frag frag;
        if (!ParseExpr(frag, err)) return false;
        SkipSlashes();
        if (pos_ != input_.size()) {
            err = string("Unexpected trailing token at position ") + to_string(pos_);
            return false;
        }
        int accept = NewState();
        states_[frag.out].eps.push_back(accept);
        out.states = std::move(states_);
        out.start = frag.start;
        out.accept = accept;
        out.start_closure = EpsilonClosure(out, vector<int>{out.start});
        out.closure_by_state.resize(out.states.size());
        for (size_t i = 0; i < out.states.size(); ++i) {
            out.closure_by_state[i] = EpsilonClosure(out, vector<int>{static_cast<int>(i)});
        }
        return true;
    }

private:
    struct Frag {
        int start = -1;
        int out = -1;
    };

    string input_;
    size_t pos_ = 0;
    vector<RegexBodyNfaState> states_;

    int NewState() {
        states_.push_back(RegexBodyNfaState{});
        return static_cast<int>(states_.size()) - 1;
    }

    void SkipSlashes() {
        while (pos_ < input_.size() && input_[pos_] == '/') {
            ++pos_;
        }
    }

    char Peek() {
        SkipSlashes();
        if (pos_ >= input_.size()) return '\0';
        return input_[pos_];
    }

    bool Consume(char c) {
        SkipSlashes();
        if (pos_ < input_.size() && input_[pos_] == c) {
            ++pos_;
            return true;
        }
        return false;
    }

    string ParseLabel() {
        SkipSlashes();
        string out;
        while (pos_ < input_.size() && isdigit(static_cast<unsigned char>(input_[pos_]))) {
            out.push_back(input_[pos_++]);
        }
        return out;
    }

    bool ParseExpr(Frag& out, string& err) {
        if (!ParseSeq(out, err)) return false;
        while (Consume('|')) {
            Frag rhs;
            if (!ParseSeq(rhs, err)) return false;
            out = MakeUnion(out, rhs);
        }
        return true;
    }

    bool ParseSeq(Frag& out, string& err) {
        bool first = true;
        while (true) {
            char c = Peek();
            if (c == '\0' || c == ')' || c == '|') break;
            Frag item;
            if (!ParseAtom(item, err)) return false;
            if (first) {
                out = item;
                first = false;
            } else {
                out = MakeSeq(out, item);
            }
        }
        if (first) {
            err = string("Empty sequence at position ") + to_string(pos_);
            return false;
        }
        return true;
    }

    bool ParseAtom(Frag& out, string& err) {
        char c = Peek();
        if (c == '(') {
            Consume('(');
            if (!ParseExpr(out, err)) return false;
            if (!Consume(')')) {
                err = string("Missing ')' at position ") + to_string(pos_);
                return false;
            }
            return true;
        }

        string label = ParseLabel();
        if (label.empty()) {
            err = string("Expected label at position ") + to_string(pos_);
            return false;
        }
        out = MakeLabel(label);
        return true;
    }

    Frag MakeLabel(const string& label) {
        int s = NewState();
        int t = NewState();
        states_[s].labeled.push_back(RegexBodyNfaTransition{t, label});
        return Frag{s, t};
    }

    Frag MakeSeq(const Frag& lhs, const Frag& rhs) {
        states_[lhs.out].eps.push_back(rhs.start);
        return Frag{lhs.start, rhs.out};
    }

    Frag MakeUnion(const Frag& lhs, const Frag& rhs) {
        int s = NewState();
        int t = NewState();
        states_[s].eps.push_back(lhs.start);
        states_[s].eps.push_back(rhs.start);
        states_[lhs.out].eps.push_back(t);
        states_[rhs.out].eps.push_back(t);
        return Frag{s, t};
    }
};

static vector<int> EpsilonClosure(const RegexBodyNfa& nfa, const vector<int>& seeds) {
    vector<int> closure;
    vector<char> seen(nfa.states.size(), 0);
    vector<int> q;
    for (int s : seeds) {
        if (s < 0 || s >= static_cast<int>(nfa.states.size()) || seen[static_cast<size_t>(s)]) continue;
        seen[static_cast<size_t>(s)] = 1;
        q.push_back(s);
    }
    size_t head = 0;
    while (head < q.size()) {
        int cur = q[head++];
        closure.push_back(cur);
        for (int nxt : nfa.states[static_cast<size_t>(cur)].eps) {
            if (nxt < 0 || nxt >= static_cast<int>(nfa.states.size())) continue;
            if (seen[static_cast<size_t>(nxt)]) continue;
            seen[static_cast<size_t>(nxt)] = 1;
            q.push_back(nxt);
        }
    }
    sort(closure.begin(), closure.end());
    return closure;
}

static bool ParseRegexQueryPattern(
    const string& expr,
    bool allow_open_range,
    bool allow_bounded_range,
    int max_exact_n,
    RegexQueryPattern& out,
    string& err)
{
    string s;
    s.reserve(expr.size());
    for (char c : expr) {
        if (!isspace(static_cast<unsigned char>(c))) s.push_back(c);
    }
    size_t lbrace = s.find('{');
    size_t rbrace = s.rfind('}');
    if (lbrace == string::npos || rbrace == string::npos || rbrace != s.size() - 1 || lbrace >= rbrace) {
        err = "Expression must end with {range}";
        return false;
    }

    string body = s.substr(0, lbrace);
    string range = s.substr(lbrace + 1, rbrace - lbrace - 1);
    if (body.empty()) {
        err = "Empty body";
        return false;
    }

    RegexBodyParser parser(body);
    RegexBodyNfa nfa;
    if (!parser.Parse(nfa, err)) return false;

    int min_rep = 0;
    int max_rep = 0;
    size_t comma = range.find(',');
    if (comma == string::npos) {
        if (range.empty()) {
            err = "Empty repetition";
            return false;
        }
        for (char c : range) {
            if (!isdigit(static_cast<unsigned char>(c))) {
                err = "Invalid repetition number";
                return false;
            }
        }
        min_rep = stoi(range);
        max_rep = min_rep;
        if (max_exact_n >= 0 && max_rep > max_exact_n) {
            err = "n too large";
            return false;
        }
    } else {
        if (!allow_bounded_range) {
            err = "Range expressions are not supported here";
            return false;
        }
        string lhs = range.substr(0, comma);
        string rhs = range.substr(comma + 1);
        if (lhs.empty()) {
            err = "Missing lower bound";
            return false;
        }
        for (char c : lhs) {
            if (!isdigit(static_cast<unsigned char>(c))) {
                err = "Invalid lower bound";
                return false;
            }
        }
        min_rep = stoi(lhs);
        if (rhs.empty()) {
            if (!allow_open_range) {
                err = "Open range is not supported here";
                return false;
            }
            max_rep = -1;
        } else {
            for (char c : rhs) {
                if (!isdigit(static_cast<unsigned char>(c))) {
                    err = "Invalid upper bound";
                    return false;
                }
            }
            max_rep = stoi(rhs);
        }
    }

    if (min_rep < 0 || (max_rep >= 0 && min_rep > max_rep)) {
        err = "Invalid repetition range";
        return false;
    }

    out.normalized_body = body;
    out.min_rep = min_rep;
    out.max_rep = max_rep;
    out.nfa = std::move(nfa);
    return true;
}

static vector<string> CollectRegexStartLabels(const RegexBodyNfa& nfa) {
    vector<string> labels;
    for (int sid : nfa.start_closure) {
        if (sid < 0 || sid >= static_cast<int>(nfa.states.size())) continue;
        for (const auto& tr : nfa.states[static_cast<size_t>(sid)].labeled) {
            labels.push_back(tr.label);
        }
    }
    sort(labels.begin(), labels.end());
    labels.erase(unique(labels.begin(), labels.end()), labels.end());
    return labels;
}
