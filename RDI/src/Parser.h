#pragma once
#include<stdlib.h>
#include<iostream>
#include<vector>
#include<assert.h>
#include<map>
#include "Type.h"
#include"NFA_state.h"
using namespace std;
class Regex;
class RangeRepetition;
class Parser
{
private:
	int position = 0;
	int groupID = 0;
	bool isPrefix = false;//�Ƿ�Ϊǰ׺
	bool isSuffix = false;//�Ƿ�Ϊ��׺
	std::string input;
	int input_size;
	char peek();
	void eat(char c);
	string next();
	bool more();
	shared_ptr<Regex> regex();
	shared_ptr<Regex> term();
	shared_ptr<Regex> factor();
	shared_ptr<Regex> base();
	//shared_ptr<Regex> multichoice();
	//shared_ptr<Regex> singlechoice(bool in=false);
	//shared_ptr<Regex> andchoice();
public:
	shared_ptr<Regex> re = nullptr;
	shared_ptr<NFA_state> matchState;
	int referID = -1;
	int referCount = 0;
	int stateNo;
	int CGs;
	bool hasRefer = false;
	Parser(std::string in);
	shared_ptr<Regex>parse();
	//shared_ptr<Regex> blank = static_pointer_cast<Regex>(make_shared<Blank>());
	map<int, shared_ptr<Regex> > group;
	vector<shared_ptr<RangeRepetition>> rangeRepetitions;
	vector<shared_ptr<NFA_state> > list1(shared_ptr<NFA_state> outp);
	//vector<shared_ptr<NFA_state>> append(vector<shared_ptr<NFA_state>>l1, vector<shared_ptr<NFA_state>>l2);
	static void patch1(shared_ptr<vector<shared_ptr<NFA_state>>>& l, shared_ptr<NFA_state> s);
	static void patch1(shared_ptr<vector<shared_ptr<NFA_state>>>& l, shared_ptr<NFA_state> s,int op);
	static void patch2(shared_ptr<vector<shared_ptr<NFA_state>>>& l, shared_ptr<NFA_state> s);
	static void patch2(shared_ptr<vector<shared_ptr<NFA_state>>>& l, shared_ptr<NFA_state> s,int op);
	static void patchMatch(shared_ptr<Regex> r);
};
