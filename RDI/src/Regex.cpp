#include "Regex.h"
#include "Parser.h"
#include <queue>
#include <string>
shared_ptr<Regex> Regex::blank (new class::Blank());
void Regex::setIsPrefix(bool b)
{
	this->isPrefix = b;
}

void Regex::setIsSuffix(bool b)
{
	this->isSuffix = b;
}

void Regex::setGroupID(int id)
{
	groupID = id;
}

void Regex::setIsReference(bool b)
{
	isReference = b;
}

bool Regex::getIsPrefix()
{
	return isPrefix;
}

bool Regex::getIsSuffix()
{
	return isSuffix;
}


bool Regex::getIsReference()
{
	return isReference;
}

int Regex::getGroupID()
{
	return groupID;
}

string Blank::toString() {
	return "Blank";
}

//void MultiChoice::andOp(shared_ptr<MultiChoice> other)
//{
//	scale &= other->scale;
//}
//
//void MultiChoice::orOp(shared_ptr<MultiChoice> other)
//{
//	scale |= other->scale;
//}
//
//void MultiChoice::reverse()
//{
//	scale = ~scale;
//
//}
//void MultiChoice::generateNFA()
//{
//	shared_ptr<ScaleState> ss(new ScaleState(scale));
//	this->start = ss;
//	this->out1 = list1(ss);
//}
//shared_ptr<Regex> MultiChoice::copy()
//{
//	shared_ptr<MultiChoice>mc= make_shared<MultiChoice>();
//	mc->scale = scale;
//	mc->generateNFA();
//	return mc;
//}
//string MultiChoice::toString()
//{
//	return "[MultiChoice]";
//}

string Choice::toString()
{
	return "[choice in" + thisOne->toString() + " and " + thatOne->toString() + "]";
}

shared_ptr<Regex> Choice::copy()
{
	return make_shared<Choice>(thisOne->copy(), thatOne->copy());
}

// string AnyChar::toString()
// {
// 	return "[AnyCharacter]";
// }

// shared_ptr<Regex> AnyChar::copy()
// {
// 	return make_shared<AnyChar>();
// }

string PlusRepetition::toString()
{
	return "[+ repetition of "+internal->toString()+"]";
}

shared_ptr<Regex> PlusRepetition::copy()
{
	return make_shared<PlusRepetition>(internal->copy());
}

string Prefix::toString()
{
	return "[Prefix of "+internal->toString()+" ]";
}

shared_ptr<Regex> Prefix::copy()
{
	return shared_ptr<Prefix>();
}

string Primitives::toString()
{
	return ("Primitives:"+label);
}
//LookAround::LookAround(shared_ptr<Regex> re, bool ahead, bool positive,bool hasRefer)
//{
//	this->type = RegexType::LookAround;
//	this->r = re;
//	this->ahead = ahead;
//	this->positive = positive;
//	this->hasRefer = hasRefer;
//	shared_ptr<NFA_state> matchState = make_shared<NFA_state>(Type::MatchState);
//	patch1(re->out1, matchState);
//	patch2(re->out2, matchState);
//	shared_ptr<LookAroundState> la = make_shared<LookAroundState>(ahead, positive, re->start,matchState,hasRefer);
//	this->start = la;
//	this->out1 = list1(la);
//	this->out2 = make_shared<vector<shared_ptr<NFA_state>>>();
//}
shared_ptr<Regex> Primitives::copy()
{
	return make_shared<Primitives>(label);
}

string QuoraRepetition::toString()
{
	return "[? repetition of "+internal->toString()+"]";
}

shared_ptr<Regex> QuoraRepetition::copy()
{
	return make_shared<QuoraRepetition>(internal->copy());
}

shared_ptr<Regex> RangeRepetition::copy()
{
	return make_shared<RangeRepetition>(internal->copy(),this->min,this->max);
}
string Sequence::toString()
{
	return "["+first->toString() + "+"+second->toString()+"]";
}

shared_ptr<Regex> Sequence::copy()
{
	return make_shared<Sequence>(first->copy(), second->copy());
}

// string SpecialChars::toString()
// {
// 	return "[Special character:\\" + special;
// }

string StarRepetition::toString()
{
	return "[* repetition of" + internal->toString() + "]";
}

shared_ptr<Regex> StarRepetition::copy()
{
	return make_shared<StarRepetition>(internal->copy());
}


string RangeRepetition::toString()
{
	return "[RangeRepetition from " + to_string(min) +"to"+ to_string(max);
}


string Suffix::toString()
{
	return "[Suffix of " + internal->toString() + "]";
}



shared_ptr<Regex> Suffix::copy()
{
	return make_shared<Suffix>(internal->copy());
}

shared_ptr<Regex> Blank::copy()
{
	return make_shared<Blank>();
}

// shared_ptr<Regex> SpecialChars::copy()
// {
// 	return make_shared<SpecialChars>(special);
// }
//shared_ptr<Regex> LookAround::copy()
//{
//	return make_shared<LookAround>(r, ahead, positive);
//}
//string LookAround::toString()
//{
//	return "[LookAround]";
//}
shared_ptr<vector<shared_ptr<NFA_state>>> Regex::list1(shared_ptr<NFA_state> outp)
{
	shared_ptr<vector<shared_ptr<NFA_state>>> list(new vector<shared_ptr<NFA_state>>);
	list->push_back(outp);
	return list;
}

shared_ptr<vector<shared_ptr<NFA_state>>> Regex::append(shared_ptr<vector<shared_ptr<NFA_state>>> l1, shared_ptr<vector<shared_ptr<NFA_state>>> l2)
{
	for (auto& state : *l2)
		if(state!=nullptr)
			l1->push_back(state);
	return l1;
}


//void Regex::setGroupState(bool isGroup, bool isReference, int referNo)
//{
//	if (isGroup)
//	{
//		//cout << "isGroup  ";
//		shared_ptr<NFA_state> new_start = make_shared<NFA_state>(Type::GroupStart, start, nullptr);
//		new_start->referNo = 0;
//		shared_ptr<NFA_state> new_out = make_shared<NFA_state>(Type::GroupEnd);
//		new_out->referNo = 0;
//		Parser::patch1(out1, new_out);
//		Parser::patch2(out2, new_out);
//		this->oldout1 = out1;
//		this->oldout2 = out2;
//		this->start = new_start;
//		this->out1 = list1(new_out);
//		this->out2 = make_shared<vector<shared_ptr<NFA_state>>>();
//	}
//	else if (isReference)
//	{
//		shared_ptr<NFA_state> new_start = make_shared<NFA_state>(Type::ReferStart, start, nullptr);
//		shared_ptr<NFA_state> new_out = make_shared<NFA_state>(Type::ReferEnd);
//		new_start->referNo = referNo + 1;
//		new_out->referNo = referNo + 1;
//		this->oldout1 = out1;
//		this->oldout2 = out2;
//		Parser::patch1(out1, new_out);
//		Parser::patch2(out2, new_out);
//		this->start = new_start;
//		this->out1 = list1(new_out);
//		this->out2 = make_shared<vector<shared_ptr<NFA_state>>>();
//	}
//}
