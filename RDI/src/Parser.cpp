#pragma once
#include "Parser.h"
#include "Regex.h"
#include <bitset>
#include <iostream>
#include <memory>
#include "Type.h"

#ifndef RANGE_SINGLE_ENABLE_OPEN_RANGE
#define RANGE_SINGLE_ENABLE_OPEN_RANGE 0
#endif

#ifndef RANGE_SINGLE_DEBUG_OPEN_RANGE
#define RANGE_SINGLE_DEBUG_OPEN_RANGE 0
#endif

Parser::Parser(string in)
{
	this->input = in;
	this->input_size = input.length();
	re = regex();
	matchState = make_shared<NFA_state>(257);
	patch1(re->out1, matchState);
	patch2(re->out2, matchState);
}

shared_ptr<Regex> Parser::parse()
{
	return re;
}

char Parser::peek()
{
	while(position<input_size&&input[position]=='/'){
		position++;
	}
	if (position >= input_size) 
	{
		// cout<<"debug peek:"<<position<<endl;
		// cout<<"debug peek2:"<<input_size<<endl;
		return '\0';
	}
	return input.at(position);
}

void Parser::eat(char c)
{
	char next = peek();
	if (next == c) position++;
	else {
		//cout << "Expected " <<c << ";got "<< next;
		//abort();
	}
}

string Parser::next()
{
	char c = peek();
	string label = "";
	//cout<<"debug:"<<c<<endl;
	while (position<input_size&&( isalpha(input[position]) || isdigit(input[position])))
	{
		label += input[position];
		position++;
	}
	return label;
}

bool Parser::more()
{
	while(position<input_size&&input[position]==' '){
		position++;
	}
	// cout<<"debug more:"<<position<<endl;
	// cout<<"debug more2:"<<input_size<<endl;
	return position < input_size;
}

shared_ptr<Regex> Parser::regex()
{
	// cout<<"debug:regex"<<endl;
	shared_ptr<Regex> term = Parser::term();
	if (more() && peek() == '|')
	{
		// cout<<"debug:nxtRegex"<<endl;
		eat('|');
		shared_ptr<Regex> regex = Parser::regex();
		shared_ptr<Regex> temp=static_pointer_cast<Regex>(make_shared<Choice>(term, regex));
		return temp;
	}
	else {
		return term;
	}
}


shared_ptr<Regex> Parser::term()
{
	//cout<<"debug:term"<<endl;
	bool sign = false;
	shared_ptr<Regex> factor(new Blank());
	if (!factor->lazy)
		sign = true;
	while (more() &&( peek() != '|') && peek() != ')')
	{
		shared_ptr<Regex> nextFactor;
		if(sign)
			nextFactor = Parser::term();
		else
			nextFactor = Parser::factor();
		//cout<<"debug:"<<nextFactor->toString()<<endl;
		if (!nextFactor->lazy && factor->lazy)
			sign = true;
		factor = make_shared<Sequence>(factor, nextFactor);
	}
	return factor;
}

shared_ptr<Regex> Parser::factor()
{
	//cout<<"debug:factor"<<endl;
	shared_ptr<Regex> base = Parser::base();
	while (more() && (peek() == '*' || peek() == '+' || peek() == '?')||peek()=='{')
	{
		char c = peek();
		switch (c)
		{
		case '*':
		{
			base = make_shared<StarRepetition>(base);
			eat('*');
			break;
		}
		case '+':
		{
			base = make_shared<PlusRepetition>(base);
			eat('+');
			break;
		}
		case '?':
		{
			base = make_shared<QuoraRepetition>(base);
			eat('?');
			break;
		}
		case '{':
		{
			eat('{');
			int min=0,max=-1;
			while (more() && isdigit(peek()))
			{
				c = peek();
				min = min * 10 + c-'0';
				eat(c);
			}
			if (more() && peek()==',')
			{
				eat(',');
				max = 0;
				bool saw_upper_bound = false;
				while (more() && isdigit(peek()))
				{
					c = peek();
					max = max * 10 + c-'0';
					eat(c);
					saw_upper_bound = true;
				}
#if RANGE_SINGLE_ENABLE_OPEN_RANGE
				if (!saw_upper_bound) {
					max = -1;
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
					std::cerr << "[range-single][parser] parsed open range {"
					          << min << ",} as max=-1" << std::endl;
#endif
				}
#endif
			}
			else {
				max = min;
			}
			eat('}');
			shared_ptr<RangeRepetition>rangeR = make_shared<RangeRepetition>(base,min,max);
			base=rangeR;
			rangeRepetitions.emplace_back(rangeR);
			base->lazy=false;
			break;
		}
		default:
			break;
		}
	}
	return base;
}

shared_ptr<Regex> Parser::base()
{
	//cout<<"debug:base"<<endl;
	bool uncapure = false;
	//cout<<"debug base:"<<peek()<<endl; 
	switch (peek())
	{
	case '(':
	{
		eat('(');
		char nxt = peek();
		//cout<<"debug:"<<1<<endl;
		shared_ptr<Regex> r = regex();
		eat(')');
		return r;
	}
	// case '.':
	// {
	// 	eat('.');
	// 	return make_shared<class::AnyChar>();
	// }
	default:
	{	
		string label=next();
		//cout<<"debug:"+label<<endl;
		shared_ptr<Regex> nex =make_shared<Primitives>(label);
		return nex;
	}
	}
}

vector<shared_ptr<NFA_state> > Parser::list1(shared_ptr<NFA_state> outp)
{
	vector<shared_ptr<NFA_state> > list;
	list.push_back(outp);
	return list;
}

static vector<shared_ptr<NFA_state> > append(vector<shared_ptr<NFA_state> > l1, vector<shared_ptr<NFA_state> > l2)
{
	for (auto& state : l2)
		l1.push_back(state);
	return l1;
}

void Parser::patch2(shared_ptr <vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s)
{
	for (auto& t : *l)
	{
		t->out2 = s;
		s->reverse_out.emplace_back(t);
	}
}

void Parser::patch2(shared_ptr <vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s,int op)
{
	for (auto& t : *l)
	{
		t->out2 = s;
		t->op2 = op;
		s->reverse_out.emplace_back(t);
	}
}

void Parser::patchMatch(shared_ptr<Regex> r)
{
	shared_ptr<NFA_state> matchState = make_shared<NFA_state>(Type::MatchState);
	patch1(r->out1, matchState);
	patch2(r->out2, matchState);
}

void Parser::patch1(shared_ptr<vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s)
{
	for (auto& t : *l)
	{
		t->out1 = s;
		s->reverse_out.emplace_back(t);
	}
}

void Parser::patch1(shared_ptr<vector<shared_ptr<NFA_state> > >& l, shared_ptr<NFA_state> s,int op)
{
	for (auto& t : *l)
	{
		t->out1 = s;
		t->op1 = op;
		s->reverse_out.emplace_back(t);
	}
}
