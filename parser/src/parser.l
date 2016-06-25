%{

#include "machine.h"
#include "parser_code.h"
#include "app_locale.h"
using namespace ai::app;
#include "parser.cpp.h"

%}

%Start A quote sl_comments ml_comments

D	[0-9]
d	[1-9]
O	[0-7]
H	[0-9a-fA-F]

word	[_a-zA-Z][_a-zA-Z0-9]*

%{

static int getkeyword(const char *word);
static void count();
static void set_data1(const string& text);
static void set_data2(bool is_unsigned, parser_atomic_type atomic_type, const string& text);
static void set_data3(const string& text);
static void set_data4(const string& text);

using namespace std;
using namespace ai::app;

namespace ai
{
namespace app
{

int inquote = 0;
int incomments = 0;
string tminline;
int line_no = 0;
int column = 0;
string tmp_str;
string prefix_str;

}
}

%}

%%
	BEGIN A;

<A>"("		{ set_data1(yytext); return LCURVE; }
<A>")"		{ set_data1(yytext); return RCURVE; }
<A>"["		{ set_data1(yytext); return LBRACKET; }
<A>"]"		{ set_data1(yytext); return RBRACKET; }

<A>"!"		{ set_data1(yytext); return NOT; }
<A>"~"		{ set_data1(yytext); return BITNOT; }
<A>"++"		{ set_data1(yytext); return INC; }
<A>"--"		{ set_data1(yytext); return DEC; }
<A>"sizeof"	{ set_data1(yytext); return SIZEOF; }

<A>"*"		{ set_data1(yytext); return MUL; }
<A>"/"		{ set_data1(yytext); return DIV; }
<A>"%"		{ set_data1(yytext); return MOD; }

<A>"+"		{ set_data1(yytext); return ADD; }
<A>"-"		{ set_data1(yytext); return SUB; }

<A>"<<"		{ set_data1(yytext); return LSHIFT; }
<A>">>"		{ set_data1(yytext); return RSHIFT; }

<A>"<"		{ set_data1(yytext); return LT; }
<A>"<="		{ set_data1(yytext); return LE; }
<A>">"		{ set_data1(yytext); return GT; }
<A>">="		{ set_data1(yytext); return GE; }

<A>"=="		{ set_data1(yytext); return EQ; }
<A>"!="		{ set_data1(yytext); return NEQ; }

<A>"&"		{ set_data1(yytext); return BITAND; }

<A>"^"		{ set_data1(yytext); return BITXOR; }

<A>"|"		{ set_data1(yytext); return BITOR; }

<A>"&&"		{ set_data1(yytext); return AND; }

<A>"||"		{ set_data1(yytext); return OR; }

<A>"?"		{ set_data1(yytext); return QUESTION; }
<A>":"		{ set_data1(yytext); return COLON; }

<A>"="		{ set_data1(yytext); return ASSIGN; }
<A>"+="		{ set_data1(yytext); return ADD_ASSIGN; }
<A>"-="		{ set_data1(yytext); return SUB_ASSIGN; }
<A>"*="		{ set_data1(yytext); return MUL_ASSIGN; }
<A>"/="		{ set_data1(yytext); return DIV_ASSIGN; }
<A>"%="		{ set_data1(yytext); return MOD_ASSIGN; }
<A>"<<="	{ set_data1(yytext); return LSHIFT_ASSIGN; }
<A>">>="	{ set_data1(yytext); return RSHIFT_ASSIGN; }
<A>"&="		{ set_data1(yytext); return BITAND_ASSIGN; }
<A>"^="		{ set_data1(yytext); return BITXOR_ASSIGN; }
<A>"|="		{ set_data1(yytext); return BITOR_ASSIGN; }

<A>","		{ set_data1(yytext); return COMMA; }

<A>"\."		{ set_data1(yytext); return DOT; }
<A>"{"		{ set_data1(yytext); return LBRACE; }
<A>"}"		{ set_data1(yytext); return RBRACE; }
<A>";"		{ set_data1(yytext); return SEMICOLON; }

<A>'([^']|(\\.))'					{ set_data2(false, PARSER_TYPE_CHAR, yytext); return CCONSTANT; }
<A>(("0"[xX]{H}+)|("0"{O}+)|({D}+))	{ set_data2(false, PARSER_TYPE_INT, yytext); return ICONSTANT; }
<A>{D}+[uU]						{ set_data2(true, PARSER_TYPE_INT, yytext); return UICONSTANT; }
<A>{D}+[lL]						{ set_data2(false, PARSER_TYPE_LONG, yytext); return LCONSTANT; }
<A>{D}+[uU][lL]					{ set_data2(true, PARSER_TYPE_LONG, yytext); return ULCONSTANT; }
<A>(({D}+\.{D}*)|({D}*\.{D}+))[fF]	{ set_data2(false, PARSER_TYPE_FLOAT, yytext); return FCONSTANT; }
<A>(((({D}+\.{D}*)|({D}*\.{D}+))[eE]([-+]?){D}+)|((({D}+\.{D}*)|({D}*\.{D}+))([dD]?))) { set_data2(false, PARSER_TYPE_DOUBLE, yytext); return DCONSTANT; }

<A>{word}	{
			int tok;

			set_data4(yytext);
			if ((tok = getkeyword(yytext)) >= 0)
				return tok;

			return IDENT;
		}

<A>[ \b\t\r\n]	{
			// echo whitespace character
			count();
			prefix_str += yytext[0];
		}

<A>\"	{
			BEGIN quote;
			inquote = 1;
			count();
			tmp_str = "";
		}

<quote>\\\"	{
			// echo an escaped quote
			count();
			tmp_str += yytext;
		}

<quote>\"	{
			// end of quoted string
			BEGIN A;
			inquote = 0;
			set_data3(tmp_str);
			return STRING;
		}

<quote>.	{
			// echo any other single character
			count();
			tmp_str += yytext[0];
		}

<A>"//"	{
			BEGIN sl_comments;
			incomments = 1;
			count();
			prefix_str += "//";
		}

<sl_comments>\n	{
			// end of comments string
			BEGIN A;
			incomments = 0;
			prefix_str += '\n';
			count();
		}

<A>"/*"	{
			BEGIN ml_comments;
			incomments = 1;
			count();
			prefix_str += "/*";
		}

<ml_comments>"*/"	{
			// end of comments string
			BEGIN A;
			incomments = 0;
			count();
			prefix_str += "*/";
		}

<sl_comments,ml_comments>.	{
			// echo any other single character
			count();
			prefix_str += yytext[0];
		}

.		{
			count();
			return BADTOKEN;
		}

%%

int yywrap()
{
	return 1;
}

struct keyword_t
{
	const char *name;
	int tok;
};

keyword_t keywords[] = {
	{ "break", BREAK },
	{ "case", CASE },
	{ "char", TYPE_CHAR },
	{ "continue", CONTINUE },
	{ "default", DEFAULT },
	{ "do", DO },
	{ "double", TYPE_DOUBLE },
	{ "else", ELSE },
	{ "float", TYPE_FLOAT },
	{ "for", FOR },
	{ "goto", GOTO },
	{ "if", IF },
	{ "int", TYPE_INT },
	{ "long", TYPE_LONG },
	{ "NULL", NULL_OP },
	{ "return", RETURN },
	{ "short", TYPE_SHORT },
	{ "switch", SWITCH },
	{ "time_t", TYPE_TIME_T },
	{ "unsigned", TYPE_UNSIGNED },
	{ "while", WHILE }
};

static int getkeyword(const char *word)
{
	int first = 0;
	int last;
	int middle;
	int comp;
	keyword_t *kw;

	kw = keywords;
	last = sizeof(keywords) / sizeof(keyword_t) - 1;

	do {
		// binary search for keyword
		middle = (first + last) >> 1;
		comp = ::strcmp(word, kw[middle].name);
		if(comp == 0)
			return kw[middle].tok;

		if (comp < 0)
			last = middle - 1;
		if (comp > 0)
			first = middle + 1;
	} while (first <= last);

	return -1;
}

static void count()
{
	for (int i = 0; yytext[i] != '\0'; i++) {
		if (yytext[i] == '\n') {
			column = 0;
			tminline = "";
			line_no++;
		} else if (yytext[i] == '\t')
			column += 8 - (column % 8);
		else
			column++;
	}

	// ECHO
	tminline += yytext;
}

static void set_data1(const string& text)
{
	count();

	yylval.sval = new parser_string_t(text);
	yylval.sval->set_line_no(line_no);
	yylval.sval->set_column(column);
	yylval.sval->set_is_text(false);
	yylval.sval->set_is_unsigned(false);
	yylval.sval->set_atomic_type(PARSER_TYPE_UNKNOWN);
	yylval.sval->set_pointers(0);
	yylval.sval->set_prefix(prefix_str);
	prefix_str = "";
}

static void set_data2(bool is_unsigned, parser_atomic_type atomic_type, const string& text)
{
	count();

	yylval.sval = new parser_string_t(text);
	yylval.sval->set_line_no(line_no);
	yylval.sval->set_column(column);
	yylval.sval->set_is_text(true);
	yylval.sval->set_is_unsigned(is_unsigned);
	yylval.sval->set_atomic_type(atomic_type);
	yylval.sval->set_pointers(0);
	yylval.sval->set_prefix(prefix_str);
	prefix_str = "";
}

static void set_data3(const string& text)
{
	count();

	yylval.sval = new parser_string_t(text);
	yylval.sval->set_line_no(line_no);
	yylval.sval->set_column(column);
	yylval.sval->set_is_text(true);
	yylval.sval->set_is_unsigned(false);
	yylval.sval->set_atomic_type(PARSER_TYPE_CHAR);
	yylval.sval->set_pointers(1);
	yylval.sval->set_prefix(prefix_str);
	prefix_str = "";
}

static void set_data4(const string& text)
{
	count();

	yylval.sval = new parser_string_t(text);
	yylval.sval->set_line_no(line_no);
	yylval.sval->set_column(column);
	yylval.sval->set_is_text(false);
	yylval.sval->set_is_unsigned(false);
	yylval.sval->set_atomic_type(PARSER_TYPE_UNKNOWN);
	yylval.sval->set_pointers(0);
	yylval.sval->set_prefix(prefix_str);
	prefix_str = "";
}

