
#include<ctype.h>
#include<memory>
/*
using namespace std;
class Pattern {
private:
	bool isPrefix;
	bool isSuffix;
	//shared_ptr<DState> dStart;
	int referID = -1;
	//unordered_map<shared_ptr<NfaList>, shared_ptr<DState>,Hasher,NfalistCmp> allDStates;
	shared_ptr<Frag> setGroupstate(shared_ptr<Frag> f, bool isGroup, bool isReference,int referNo);
	static shared_ptr<DState> deadState;
	vector<shared_ptr<NFA_state>>l1;
	vector<shared_ptr<NFA_state>>l2;
	int listid=0;
	shared_ptr<Frag> Regex2NFA(shared_ptr<Regex> re);
	vector<shared_ptr<NFA_state>> startList(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state>>&l);
	void addState(vector<shared_ptr<NFA_state>>&l, shared_ptr<NFA_state> s);
	void step(vector<shared_ptr<NFA_state>>clist, int c, vector<shared_ptr<NFA_state>> &nlist);
	shared_ptr<DState> step(shared_ptr<DState> cState, int c);
	vector<shared_ptr<NFA_state>> list1(shared_ptr<NFA_state> outp);
	vector<shared_ptr<NFA_state>> append(vector<shared_ptr<NFA_state>>l1, vector<shared_ptr<NFA_state>>l2);
	void patch1(vector<shared_ptr<NFA_state>>&l, shared_ptr<NFA_state> s);
	void patch2(vector<shared_ptr<NFA_state>>&l, shared_ptr<NFA_state> s);

public:
	int referCount = 0;
	Pattern(shared_ptr<Frag> f);
	Pattern(string pattern);
	shared_ptr<NFA_state> matchState;
	shared_ptr<NFA_state> start;
	static shared_ptr<Pattern> compile(string pattern) { return make_shared<Pattern>(pattern); }
	string toString();
	bool match(string s);
	bool dfaMatch(string s);
	bool ismatch(vector<shared_ptr<NFA_state>> l);
	bool ismatch(shared_ptr<DState> ds);
	
	const static int Split = 256;
	const static int Tab = 280;
	const static int Newline = 281;
	const static int Return = 282;
	const static int Digit = 283;
	const static int WhiteSpace = 284;
	const static int NonWhiteSpace = 285;
	const static int SingleLetter = 286;
	const static int NonSingleLetter = 287;
	const static int NewPage = 288;
	const static int Escape = 289;
	const static int Boundary = 290;
	const static int NonBoundary = 291;
	const static int AnyChar = 292;
	const static int UnSupported = 293;
	const static int Scale = 295;
	const static int NegScale = 298;
	const static int Word = 296;
	const static int NonWord = 297;
	const static int Blank = 294;
	const static int GroupStart = 299;
	const static int GroupEnd = 300;
	const static int ReferStart = 301;
	const static int ReferEnd = 302;
	const static int LookAround = 303;
	const static int Digit=304;
};
*/