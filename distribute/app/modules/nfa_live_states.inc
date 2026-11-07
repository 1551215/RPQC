static bool IncrementalNfaStateConsumesLabel(const RegexBodyNfa& nfa, int state) {
    return state >= 0 && static_cast<size_t>(state) < nfa.states.size() &&
        !nfa.states[static_cast<size_t>(state)].labeled.empty();
}

static bool IncrementalLiveNfaStatesEnabled() {
    return true;
}
