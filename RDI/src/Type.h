#pragma once
#include<climits>
#include<ctime>
class Type {
public:
	inline const static int Split = 256;
	inline const static int MatchState = 257;
	inline const static int RevMatchState = 258;
	inline const static int CountSplit = 259;
	inline const static int Tab = 280;
	inline const static int Newline = 281;
	inline const static int Return = 282;
	inline const static int Digit = 283;
	inline const static int WhiteSpace = 284;
	inline const static int NonWhiteSpace = 285;
	inline const static int SingleLetter = 286;
	inline const static int NonSingleLetter = 287;
	inline const static int NewPage = 288;
	inline const static int Escape = 289;
	inline const static int Boundary = 290;
	inline const static int NonBoundary = 291;
	inline const static int AnyChar = 292;
	inline const static int UnSupported = 293;
	inline const static int Scale = 295;
	inline const static int NegScale = 298;
	inline const static int Word = 296;
	inline const static int NonWord = 297;
	inline const static int Blank = 294;
	inline const static int RangeRepetition=299;
	inline const static int Basic =300;
};

class Operation {
public:
	const static int Copy = 0;
	const static int SetPos0 = 1;
	const static int Shift = 2;
	const static int Check = 3;
	const static int CheckZeroCount = 4;
};