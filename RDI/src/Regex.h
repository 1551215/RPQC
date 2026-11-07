#pragma once
#include <memory>
#include <string>
#include<iostream>
#include<bitset>
#include <unordered_set>
#include"NFA_state.h"
#include"Type.h"
#include"Parser.h"
using namespace std;

struct First {
	unordered_set<string> labels=unordered_set<string>();
	bool empty;
	void Union(First* first) {
		for(auto s:first->labels){
			this->labels.insert(s);
		}
	}
};

struct Last {
	unordered_set<string> labels;
	bool empty;
	void Union(Last* last) {
		labels.insert(last->labels.begin(), last ->labels.end());
	}
};
class Regex
{
public :
	int min_len = 0;
	int max_len = 0;
	bool isPrefix = false;
	bool isSuffix = false;
	int groupID = -1;
	bool isReference = false;
	int referNo = -1;
	shared_ptr<NFA_state> start;
	bool lazy = true;
	First* firstSet;
	Last* lastSet;
	shared_ptr<vector<shared_ptr<NFA_state> > > out1=make_shared<vector<shared_ptr<NFA_state> > >();
	shared_ptr<vector<shared_ptr<NFA_state> > > out2=make_shared<vector<shared_ptr<NFA_state> > >();
	shared_ptr<vector<shared_ptr<NFA_state> > > oldout1;
	shared_ptr<vector<shared_ptr<NFA_state> > > oldout2;
	virtual string toString()=0;
	virtual shared_ptr<Regex> copy()=0;
	bool getIsPrefix();
	bool getIsSuffix();
	bool getIsReference();
	int getGroupID();
	void setIsPrefix(bool b);
	void setIsSuffix(bool b);
	void setGroupID(int id);
	void setIsReference(bool b);
	enum RegexType{Blank,Choice,PlusRepetition,StarRepetition,QuoraRepetition,RangeRepetition,
		Sequence,Primitive,SpecialChars,AnyChar,Prefix,Suffix,MultiChoice,LookAround};
	RegexType type;
	static shared_ptr<Regex> blank;
	shared_ptr<vector<shared_ptr<NFA_state> > > list1(shared_ptr<NFA_state> outp);
	shared_ptr<vector<shared_ptr<NFA_state> > > append(shared_ptr <vector<shared_ptr<NFA_state> > >l1, shared_ptr<vector<shared_ptr<NFA_state> >
	>l2);
	/*void patch1(shared_ptr<vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s);
	void patch2(shared_ptr<vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s);*/
};

inline bool RegexCanBeEmpty(const shared_ptr<Regex>& re);

class Blank :public Regex
{
public:
	Blank() {
		this->type = RegexType::Blank; 
		shared_ptr<NFA_state> blank(new NFA_state(Type::Blank));
		this->start = blank;
		this->out1 = list1(blank);

		firstSet = new First();
		firstSet->empty = true;
		lastSet = new Last();
		lastSet->empty = true;
	}
	string toString();
	shared_ptr<Regex> copy();
};
//class AnyChar :public Regex
//{
//public:
//	AnyChar() { this->type = RegexType::AnyChar; 
//	shared_ptr<NFA_state> s(new NFA_state(Type::AnyChar));
//	this->lazy = true;
//	this->start = s;
//	this->out1 = list1(s);
//	min_len = max_len = 1;
//	}
//	string toString();
//	shared_ptr<Regex> copy();
//};

class Choice :public Regex
{
public:
	shared_ptr<Regex>thisOne;
	shared_ptr<Regex>thatOne;
	Choice(shared_ptr<Regex> thisOne, shared_ptr<Regex> thatOne)
	{
		this->thisOne = thisOne;
		this->thatOne = thatOne;
		//this->lazy = thisOne->lazy && thatOne->lazy;
		// if (this->lazy) {
			shared_ptr<NFA_state> s = make_shared<NFA_state>(Type::Split, thisOne->start, thatOne->start);
			this->start = s;
			this->out1 = append(thisOne->out1, thatOne->out1);
			this->out2 = append(thisOne->out2, thatOne->out2);
			this->type = RegexType::Choice;
			min_len = min(thisOne->min_len, thatOne->min_len);
			max_len = max(thisOne->max_len, thatOne->max_len);
		// }
		// else {
			// if (thisOne->lazy)
			// 	Parser::patchMatch(thisOne);
			// else if (thatOne->lazy)
			// 	Parser::patchMatch(thatOne);
		// }


		firstSet=new First();
		lastSet=new Last();
		firstSet->Union(thisOne->firstSet);
		firstSet->Union(thatOne->firstSet);
		firstSet->empty = thisOne->firstSet->empty || thatOne->firstSet->empty;
		lastSet->Union(thisOne->lastSet);
		lastSet->Union(thatOne->lastSet);
		lastSet->empty = thisOne->lastSet->empty || thatOne->lastSet->empty;
	}
	string toString();
	shared_ptr<Regex> copy();
};

//class MultiChoice :public Regex
//{
//#define ARRAY_SIZE  256
//private:
//	int c1 =  1;
//	int c2 = 0;
//public:
//	bitset<256> scale;
//	MultiChoice() {
//		this->type = RegexType::MultiChoice;
//		min_len = max_len = 1;
//	}
//	MultiChoice(char c1, char c2):c1(c1),c2(c2)
//	{
//		this->type = RegexType::MultiChoice;
//		if (c1 < 0 || c2>255)
//			throw 2;
//		for (int i = c1;i <= c2;i++)
//		{
//			scale .set(i);
//		}
//	}
//	string toString();
//	void andOp(shared_ptr<MultiChoice> other);
//	void orOp(shared_ptr<MultiChoice> other);
//	void reverse();
//	void generateNFA();
//	shared_ptr<Regex> copy();
//};

class PlusRepetition :public Regex
{
public:
	shared_ptr<Regex>internal;
	PlusRepetition(shared_ptr<Regex> re)
	{
		this->lazy = true;
		internal = re;
		this->type = RegexType::PlusRepetition;
		shared_ptr<NFA_state> s = make_shared<NFA_state>(Type::Split, internal->start, nullptr);
		Parser::patch1(internal->out1, s);
		Parser::patch2(internal->out2, s);
		this->start = internal->start;
		vector<shared_ptr<NFA_state> > new_vector;
		this->out1 = make_shared<vector<shared_ptr<NFA_state> > >(new_vector);
		this->out2 = list1(s);
		// if (internal->lazy)
		// 	Parser::patchMatch(internal);

		firstSet=new First();
		lastSet=new Last();
		firstSet=internal->firstSet;
		lastSet = internal->lastSet;
		
	}
	shared_ptr<Regex> copy();
	string toString();
};
class Prefix :public Regex
{
public:
	shared_ptr<Regex> internal;
	Prefix(shared_ptr<Regex> re) {
		this->internal = re;
		this->type = RegexType::Prefix;
	}
	string toString();
	shared_ptr<Regex> copy();
};

class Primitives :public Regex
{
public:
	string  label;
	Primitives(string label)
	{
		this->label = label;
		this->type = RegexType::Primitive;
		this->lazy = true;
		shared_ptr<NFA_state> s = make_shared<NFA_state>(Type::Basic,label);
		this->start = s;
		this->out1 = list1(s);
		min_len = max_len = 1;
		firstSet=new First();
		lastSet=new Last();
		firstSet->labels.insert(label);
		lastSet->labels.insert(label);
	}
	string toString();
	shared_ptr<Regex> copy();
};

class QuoraRepetition :public Regex
{
public:
	shared_ptr<Regex> internal;
	QuoraRepetition(shared_ptr<Regex> re)
	{
		this->lazy= true;
		this->internal = re;
		this->type = RegexType::QuoraRepetition;


		// if (internal->lazy)
		// 	Parser::patchMatch(internal);
		shared_ptr<NFA_state> s = make_shared<NFA_state>(Type::Split, re->start, nullptr);
		this->start = s;
		this->out1 = re->out1;
		this->out2 = append(re->out2, list1(s));

		firstSet=new First();
		lastSet=new Last();
		firstSet->Union(internal->firstSet);
		firstSet->empty = true;
		lastSet->Union(internal->lastSet);
		lastSet->empty = true;
	}
	string toString();
	shared_ptr<Regex> copy();
};

class RangeRepetition : public Regex
{
public:
	int min;
	int max;
	shared_ptr<Regex>  internal;
	RangeRepetition(shared_ptr<Regex> re,int min,int max):min(min),max(max)
	{
		this->lazy = true;
		this->internal = re;
		this->type = RegexType::RangeRepetition;
		Parser::patchMatch(internal);
		shared_ptr<Count_state> s=make_shared<Count_state>(Type::RangeRepetition,nullptr,nullptr,min,max);
		shared_ptr<NFA_state> st=make_shared<NFA_state>(Type::Blank,internal->start,nullptr);
		s->internal_start=st;
		s->body_can_be_empty = internal->firstSet->empty;
		this->start=s;
		this->out1 =list1(s);
		this->out2 = make_shared<vector<shared_ptr<NFA_state>>>();
		if (min == 0)
		{
			firstSet=new First();
			lastSet=new Last();
			firstSet->Union(internal->firstSet);
			firstSet->empty = true;
			lastSet->Union(internal->lastSet);
			lastSet->empty = true;
		}
		else {
			firstSet = internal->firstSet;
			lastSet= internal->lastSet;
		}
		s->first_labels = firstSet->labels;
		s->last_labels = lastSet->labels;
		//s->init_map(firstSet->labels, lastSet->labels);
	}
	string toString();
	shared_ptr<Regex> copy();
};
class Sequence :public Regex
{
public:
	shared_ptr<Regex> first;
	shared_ptr<Regex> second;
	Sequence(shared_ptr<Regex> first, shared_ptr<Regex> second)
	{
		this->first = first;
		this->second = second;
		this->type = RegexType::Sequence;
		this->lazy = first->lazy && second->lazy;
		// if (lazy)
		// {
			if (first == blank)
			{
				this->start = second->start;
				this->out1 = second->out1;
				this->out2 = second->out2;
			}
			this->start = first->start;
			Parser::patch1(first->out1, second->start);
			Parser::patch2(first->out2, second->start);
			this->out1 = second->out1;
			this->out2 = second->out2;
			
		// }
		// else {
		// 	if (first->lazy)
		// 		Parser::patchMatch(first);
		// 	else if (second->lazy)
		// 		Parser::patchMatch(second);
		// }

		firstSet=new First();
		lastSet=new Last();
		firstSet->Union(first->firstSet);
		if (first->firstSet->empty)
		{
			firstSet->Union(second->firstSet);
			firstSet->empty = second->firstSet->empty;
		}

		lastSet->Union(second->lastSet);
		if (second->lastSet->empty)
		{
			lastSet->Union(first->lastSet);
			lastSet->empty = first->lastSet->empty;
		}
	}
	string toString();
	shared_ptr<Regex> copy();
};

//class SpecialChars :public Regex
//{
//public:
//	char special;
//	SpecialChars(char c)
//	{
//		special = c;
//		this->type = RegexType::SpecialChars;
//		this->lazy = true;
//		shared_ptr<NFA_state> s;
//		switch (c)
//		{
//		case 's':
//			s = make_shared<NFA_state>(Type::SingleLetter);
//			break;
//		case 'S':
//			s = make_shared<NFA_state>(Type::NonSingleLetter);
//			break;
//		case 'w':
//			s = make_shared<NFA_state>(Type:: Word);
//			break;
//		case 'W':
//			s = make_shared<NFA_state>(Type::NonWord);
//			break;
//		case 'd':
//			s = make_shared<NFA_state>(Type::Digit);
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
//			s = make_shared<NFA_state>('\\');
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
//		case '[':
//		case ']':
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
//		this->start = s;
//		this->out1 = list1(s);
//	}
//	shared_ptr<Regex> copy();
//	string toString();
//};

class StarRepetition :public Regex
{
public:
	shared_ptr<Regex> internal;
	StarRepetition(shared_ptr<Regex> internal)
	{
		this->lazy= true;
		this->internal = internal;
		this->type = RegexType::StarRepetition;
		// if (internal->lazy)
		// 	Parser::patchMatch(internal);
		shared_ptr<NFA_state> s = make_shared<NFA_state>(Type::Split, internal->start, nullptr);
		Parser::patch1(internal->out1, s);
		Parser::patch2(internal->out2, s);
		this->start = s;
		this->out1 = make_shared<vector<shared_ptr<NFA_state> > >();
		this->out2 = list1(s);

		firstSet=new First();
		lastSet=new Last();
		firstSet->Union(internal->firstSet);
		firstSet->empty = true;
		lastSet->Union(internal->lastSet);
		lastSet->empty = true;
	}
	shared_ptr<Regex> copy();
	string toString();

};

class Suffix :public Regex
{
public:
	shared_ptr<Regex> internal;
	Suffix(shared_ptr<Regex> re)
	{
		this->internal = re;
		this->type = RegexType::Suffix;
	}
	shared_ptr<Regex> copy();
	string toString();
};
