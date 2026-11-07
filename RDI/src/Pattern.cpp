//#include"Pattern.h"
////shared_ptr<DState> Pattern::deadState = make_shared<DState>();
////NFA_state* Pattern::blank = new NFA_state(Blank);
//Pattern::Pattern(string pattern)
//{
//	shared_ptr<Parser>parser = make_shared<Parser>(pattern);
//	referID = parser->referID;
//	referCount = parser->referCount;
//	if (referCount == 0)
//		return;
//	matchState =make_shared<NFA_state>(257);
//	shared_ptr<Regex> re = parser->parse();
//	//cout << re->toString()<<'\n';
//	isPrefix = re->getIsPrefix();
//	isSuffix = re->getIsSuffix();
//	shared_ptr<Frag> f = Regex2NFA(re);
//	patch1(f->out1, matchState);
//	patch2(f->out2, matchState);
//	this->start = f->start;
//	shared_ptr<NfaList> startNFA = make_shared<NfaList>(startList(start, l1));
//	//dStart =make_shared<DState>(startNFA);
//	//allDStates.insert(make_pair(startNFA, dStart));
//}
//Pattern::Pattern(shared_ptr<Frag> f):start(f->start),matchState(make_shared<NFA_state>(257))
//{
//	patch1(f->out1, matchState);
//	patch2(f->out2, matchState);
//	shared_ptr<NfaList> startNFA =make_shared<NfaList>(startList(start, l1));
//}
//shared_ptr<Frag> Pattern::setGroupstate(shared_ptr<Frag> f, bool isGroup, bool isReference,int referNo)
//{
//	if (isGroup)
//	{
//		//cout << "isGroup  ";
//		shared_ptr<NFA_state> new_start = make_shared<NFA_state>(GroupStart,f->start,nullptr);
//		new_start->referNo = 0;
//		shared_ptr<NFA_state> new_out = make_shared<NFA_state>(GroupEnd);
//		new_out->referNo = 0;
//		patch1(f->out1, new_out);
//		patch2(f->out2, new_out);
//		return make_shared<Frag>(new_start, list1(new_out), vector<shared_ptr<NFA_state>>());
//	}
//	else if (isReference)
//	{
//		shared_ptr<NFA_state> new_start = make_shared<NFA_state>(ReferStart, f->start, nullptr);
//		shared_ptr<NFA_state> new_out = make_shared<NFA_state>(ReferEnd);
//		new_start->referNo = referNo + 1;
//		new_out->referNo = referNo + 1;
//		patch1(f->out1, new_out);
//		patch2(f->out2, new_out);
//		return make_shared<Frag>(new_start, list1(new_out), vector<shared_ptr<NFA_state>>());
//	}
//	return f;
//}
//
//shared_ptr<Frag> Pattern::Regex2NFA(shared_ptr<Regex> re)
//{
//	//cout << "referID" << referID << "groupID:" << re->getGroupID();
//	bool isGroup = (referID!=-1)&&(re->getGroupID()==referID);
//	bool isReference = re->getIsReference();
//	int referNo = re->referNo;
//	switch (re->type)
//	{
//	case Regex::LookAround:
//	{
//		shared_ptr<class::LookAround> lookaround = std::dynamic_pointer_cast<class::LookAround>(re);
//		shared_ptr<Regex> tmp = static_pointer_cast<Regex>(lookaround);
//		shared_ptr<Frag> f = Regex2NFA(tmp);
//		shared_ptr<Pattern> p = make_shared<Pattern>(f);
//		shared_ptr<LookAroundState> state = make_shared<LookAroundState>(p, lookaround->ahead, lookaround->positive);
//		f = make_shared<Frag>(state, list1(state));
//		return f;
//	}
//	case Regex::Sequence:
//	{
//		shared_ptr<Sequence> sequence = std::dynamic_pointer_cast<Sequence>(re);
//		if (sequence->first == Regex::blank)
//		{
//			shared_ptr<Frag> f = Regex2NFA(sequence->second);
//			f=setGroupstate(f,isGroup, isReference,referNo);
//			return f;
//		}
//		else
//		{
//			shared_ptr<Frag> f1 = Regex2NFA(sequence->first);
//			shared_ptr <Frag> f2 = Regex2NFA(sequence->second);
//			/*
//			cout<<"\n01"<<" "<<f1->start->id<<":"<<f1->start->c << " " << f2->start->id <<":" << f2->start->c<<"\n";
//			for (auto w : f1->out1)
//				cout << w->id << " ";
//			cout << "\n";
//			for (auto w : f1->out2)
//				cout << w->id << " ";
//			cout << "\n\n";
//			*//*
//			patch1(f1->out1, f2->start);
//			patch2(f1->out2, f2->start);
//			shared_ptr<Frag> f=make_shared<Frag>(f1->start, f2->out1, f2->out2);
//			f=setGroupstate(f,isGroup, isReference, referNo);
//			return f;
//		}
//	}
//	case Regex::Choice:
//	{
//		shared_ptr<Choice> choice = dynamic_pointer_cast<Choice>(re);
//		shared_ptr<Frag> f1 = Regex2NFA(choice->thisOne);
//		shared_ptr<Frag> f2 = Regex2NFA(choice->thatOne);
//		shared_ptr<NFA_state> s = make_shared<NFA_state>(Split, f1->start, f2->start);
//		shared_ptr<Frag> f=make_shared<Frag>(s, append(f1->out1, f2->out1), append(f1->out2,
//			f2->out2));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::PlusRepetition:
//	{
//		shared_ptr<Regex> in = (dynamic_pointer_cast<PlusRepetition>(re))->internal;
//		shared_ptr<Frag> f = Regex2NFA(in);
//		shared_ptr<NFA_state> s =make_shared<NFA_state>(Split, f->start, nullptr);
//		patch1(f->out1, s);
//		patch2(f->out2, s);
//		f=make_shared<Frag>(f->start, vector<shared_ptr<NFA_state>>(), list1(s));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::StarRepetition:
//	{
//		shared_ptr<Regex> in = (dynamic_pointer_cast<StarRepetition>(re))->internal;
//		shared_ptr<Frag> f = Regex2NFA(in);
//		shared_ptr<NFA_state> s = make_shared<NFA_state>(Split, f->start, nullptr);
//		patch1(f->out1, s);
//		patch2(f->out2, s);
//		f=make_shared<Frag>(s, vector<shared_ptr<NFA_state>>(), list1(s));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::QuoraRepetition:
//	{
//		shared_ptr<Regex> in = (dynamic_pointer_cast<QuoraRepetition>(re))->internal;
//		shared_ptr<Frag> f = Regex2NFA(in);
//		shared_ptr<NFA_state> s = make_shared<NFA_state>(Split, f->start, nullptr);
//		f=make_shared<Frag>(s, f->out1, append(f->out2, list1(s)));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::Primitive:
//	{
//		char c = (dynamic_pointer_cast<Primitives>(re))->c;
//		shared_ptr<NFA_state> s =make_shared<NFA_state>(c);
//		shared_ptr<Frag> f=make_shared<Frag>(s, list1(s));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::AnyChar:
//	{
//		shared_ptr<NFA_state> s(new NFA_state(AnyChar));
//		shared_ptr<Frag> f(new Frag(s, list1(s)));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::SpecialChars:
//	{
//		char c = (dynamic_pointer_cast<SpecialChars>(re))->special;
//		shared_ptr<NFA_state> s;
//		switch (c)
//		{
//		case 's':
//			s = make_shared<NFA_state>(SingleLetter);
//			break;
//		case 'S':
//			s = make_shared<NFA_state>(NonSingleLetter);
//			break;
//		case 'w':
//			s = make_shared<NFA_state>(Word);
//			break;
//		case 'W':
//			s = make_shared<NFA_state>(NonWord);
//			break;
//		case 'd':
//			s = make_shared<NFA_state>(Digit);
//			break;
//		case 'r':
//			s = make_shared<NFA_state>('\r');
//			break;
//		case 'n':
//			s = make_shared<NFA_state>('\n');
//			break;
//		case 't':
//			s = make_shared<NFA_state>('\t');
//			break;
//		case '\\':
//			s= make_shared<NFA_state>('\\');
//			break;
//		case '\'':
//			s = make_shared<NFA_state>('\'');
//			break;
//		case '\"':
//			s = make_shared<NFA_state>('\"');
//			break;
//		case '?':
//		case '.':
//		case '*':
//		case '|':
//		case '+':
//		case '(':
//		case ')':
//		case '{':
//		case '}':
//		case '^':
//		case '$':
//		case '/':
//			s = make_shared<NFA_state>(c);
//			break;
//		case 'v':
//			s = make_shared<NFA_state>('\v');
//			break;
//		case 'f':
//			s = make_shared<NFA_state>('\f');
//			break;
//		default:
//			s = make_shared<NFA_state>(c);
//			break;
//		}
//		shared_ptr<Frag> f(new Frag(s, list1(s)));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::Blank:
//	{
//		shared_ptr<NFA_state> blank ( new NFA_state(Blank));
//		shared_ptr<Frag> f(new Frag(blank, list1(blank)));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	case Regex::MultiChoice:
//	{
//		shared_ptr<MultiChoice> mc =dynamic_pointer_cast<MultiChoice>(re);
//		shared_ptr<ScaleState> ss (new ScaleState(mc->scale));
//		shared_ptr<Frag> f(new Frag(ss, list1(ss)));
//		f=setGroupstate(f,isGroup, isReference, referNo);
//		return f;
//	}
//	default:
//		//cout << re->toString();
//		//abort();
//		return nullptr;
//	}
//
//}
//
//void Pattern::addState(vector<shared_ptr<NFA_state>> &l, shared_ptr<NFA_state> s)
//{
//	if (s == nullptr || s->lastlist1 == listid) return;
//	s->lastlist1 = listid;
//	if (s->c == Split)
//	{
//		addState(l, s->out1);
//		addState(l, s->out2);
//		return;
//	}
//	if (s->c == Blank)
//	{
//		addState(l, s->out1);
//		return;
//	}
//	l.push_back(s);
//}
//
//void Pattern::step(vector<shared_ptr<NFA_state>> clist, int c, vector<shared_ptr<NFA_state>> &nlist)
//{
//	int i;
//	shared_ptr<NFA_state> s;
//	listid++;
//	nlist.clear();
//	for (i = 0; i < clist.size(); i++)
//	{
//		s = clist[i];
//		if (s->c == c || (s->c == AnyChar && c != '\n' - ' ')
//			|| (s->c == SingleLetter &&( islower(c+' ') || isupper(c+' ')))
//			|| (s->c == NonSingleLetter && !(islower(c+' ') || isupper(c + ' ')))
//			||  (s->c==Word &&(islower(c+' ') || isupper(c+' ') || isdigit(c+' ') || c+' ' == '_')))
//			addState(nlist, s->out1);
//		else if (s->c == Scale)
//		{
//			shared_ptr<ScaleState> ss = dynamic_pointer_cast<ScaleState>(s);
//			if (ss->scale[c]) addState(nlist, ss->out1);
//		}
//	}
//}
//
//string Pattern::toString()
//{
//	return start->toString();
//}
//
//shared_ptr<DState> Pattern::step(shared_ptr<DState> cState, int c)
//{
//	if (cState->next[c] == deadState) { return deadState; }
//	if (cState->next[c] != nullptr) { return cState->next[c]; }
//	vector<shared_ptr<NFA_state>> clist;
//	vector<shared_ptr<NFA_state>> nlist;
//	for (int i = 0; i < cState->nfalist->ss.size(); i++)
//	{
//		clist.push_back(cState->nfalist->ss[i]);
//	}
//	step(clist, c, nlist);
//	if (nlist.empty())
//	{
//		return deadState;
//	}
//	else
//	{
//		shared_ptr<NfaList> list (new NfaList(nlist));
//		if (allDStates.find(list)==allDStates.end())
//		{
//			allDStates.insert(make_pair(list, make_shared<DState>(list)));
//		}
//		return cState->next[c] = allDStates[list];
//	}
//}
//
//vector<shared_ptr<NFA_state>> Pattern::list1(shared_ptr<NFA_state> outp)
//{
//	vector<shared_ptr<NFA_state>> list;
//	list.push_back(outp);
//	return list;
//}
//
//vector<shared_ptr<NFA_state>> Pattern::append(vector<shared_ptr<NFA_state>> l1, vector<shared_ptr<NFA_state>> l2)
//{
//	for (auto &state : l2)
//		l1.push_back(state);
//	return l1;
//}
//
//void Pattern::patch2(vector<shared_ptr<NFA_state>> &l, shared_ptr<NFA_state> s)
//{
//	for (auto& t : l)
//		t->out2 = s;
//}
//
//void Pattern::patch1(vector<shared_ptr<NFA_state>> &l, shared_ptr<NFA_state> s)
//{
//	for (auto &t:l)
//	{
//		t->out1 = s;
//	}
//}
//
//vector<shared_ptr<NFA_state>> Pattern::startList(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state>>&l)
//{
//	listid++;
//	l.clear();
//	addState(l, s);
//	return l;
//}
//
//bool Pattern::match(string s)
//{
//	vector<shared_ptr<NFA_state>> clist, nlist, t;
//	int end = isPrefix ? 1 : s.length();
//	for (int startIndex = 0; startIndex < end; startIndex++)
//	{
//		clist = startList(start, l1);
//		nlist = l2;
//		for (int i = startIndex; i < s.length(); i++)
//		{
//			char c = s.at(i);
//			step(clist, c - ' ', nlist);
//			t = clist;
//			clist = nlist;
//			nlist = t;
//			if (clist.empty()) break;
//			if (!isSuffix && ismatch(clist)) return true;
//		}
//		if (ismatch(clist)) return true;
//	}
//	return false;
//}
//
//bool Pattern::dfaMatch(string s)
//{
//	int end = isPrefix ? 1 : s.length();
//	for (int startIndex = 0; startIndex < end; startIndex++)
//	{
//		shared_ptr<DState> cState = dStart;
//		for (int i = startIndex; i < s.length(); i++)
//		{
//			char c = s.at(i);
//			cState = step(cState, c - ' ');
//			if (cState == deadState) break;
//			if (!isSuffix && ismatch(cState)) return true;
//		}
//		if (cState != deadState && ismatch(cState)) return true;
//	}
//	return false;
//}
//bool Pattern::ismatch(vector<shared_ptr<NFA_state>> l)
//{
//	int i;
//	for (i = 0; i < l.size(); i++)
//	{
//		if (l[i] == matchState) return true;
//	}
//	return false;
//}
//bool Pattern::ismatch(shared_ptr<DState> ds)
//{
//	for (int i = 0; i < ds->nfalist->ss.size(); i++)
//	{
//		if (ds->nfalist->ss[i] == matchState) return true;
//	}
//	return false;
//}
