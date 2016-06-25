%{

#include <string>
#include <vector>
#include <stack>
#include <boost/algorithm/string.hpp>
#include "machine.h"
#include "parser_code.h"
#include "prsp_ctx.h"

using namespace ai::app;

namespace ai
{
namespace app
{

extern int errcnt;
extern int inquote;
extern int incomments;
extern string tminline;
extern int column;

}
}

extern int yylex();

%}

%token <sval> BADTOKEN
%token <sval> COLON LBRACE RBRACE SEMICOLON NULL_OP
%token <sval> SWITCH CASE BREAK CONTINUE DEFAULT DO WHILE FOR GOTO RETURN
%token <sval> TYPE_CHAR TYPE_SHORT TYPE_INT TYPE_LONG TYPE_FLOAT TYPE_DOUBLE TYPE_TIME_T TYPE_UNSIGNED
%token <sval> CCONSTANT ICONSTANT UICONSTANT LCONSTANT ULCONSTANT FCONSTANT DCONSTANT
%token <sval> IDENT STRING

%nonassoc <sval> IF
%nonassoc <sval> ELSE
%left <sval> COMMA
%right <sval> QUESTION
%right <sval> ASSIGN ADD_ASSIGN SUB_ASSIGN MUL_ASSIGN DIV_ASSIGN MOD_ASSIGN LSHIFT_ASSIGN RSHIFT_ASSIGN BITAND_ASSIGN BITXOR_ASSIGN BITOR_ASSIGN
%left <sval> OR
%left <sval> AND
%left <sval> BITOR
%left <sval> BITXOR
%left <sval> BITAND
%left <sval> EQ NEQ
%left <sval> LT LE GT GE
%left <sval> LSHIFT RSHIFT
%left <sval> ADD SUB
%left <sval> MUL DIV MOD
%left <sval> DPOINTER
%right <sval> NOT BITNOT INC DEC UMINUS UADD DOT POINTER ADDRESS SIZEOF
%nonassoc <sval> LCURVE RCURVE LBRACKET RBRACKET

%type <stmts> stmts;
%type <stmt> stmt
%type <declare> declare_stmt
%type <item> type_item
%type <pointers> pointers;
%type <type> type_def
%type <exprs> array;
%type <variable> variable
%type <ctrl> control_stmt
%type <expr_stmt> expr_stmt
%type <if_stmt> if_stmt
%type <for_stmt> for_stmt
%type <switch_stmt> switch_stmt
%type <while_stmt> while_stmt
%type <dowhile_stmt> dowhile_stmt
%type <goto_stmt> goto_stmt
%type <break_stmt> break_stmt
%type <continue_stmt> continue_stmt
%type <return_stmt> return_stmt
%type <blank_stmt> blank_stmt
%type <question_stmt> question_stmt
%type <compound_stmt> compound_stmt
%type <label_stmt> label_stmt
%type <expr> expr extend_expr
%type <switch_items> switch_items
%type <switch_item> switch_item
%type <sval> number_type
%type <args> args

%start stmts

%union
{
	parser_string_t *sval;
	parser_item_t *item;
	parser_pointers_t *pointers;
	parser_type_t *type;
	parser_variable_t *variable;
	parser_expr_t *expr;
	parser_exprs_t *exprs;
	parser_stmt_t *stmt;
	parser_declare_t *declare;
	parser_ctrl_t *ctrl;
	parser_expr_stmt_t *expr_stmt;
	parser_if_stmt_t *if_stmt;
	parser_for_stmt_t *for_stmt;
	parser_switch_stmt_t *switch_stmt;
	parser_while_stmt_t *while_stmt;
	parser_dowhile_stmt_t *dowhile_stmt;
	parser_goto_stmt_t *goto_stmt;
	parser_break_stmt_t *break_stmt;
	parser_continue_stmt_t *continue_stmt;
	parser_return_stmt_t *return_stmt;
	parser_blank_stmt_t *blank_stmt;
	parser_question_stmt_t *question_stmt;
	parser_compound_stmt_t *compound_stmt;
	parser_label_stmt_t *label_stmt;
 	parser_stmts_t *stmts;
	parser_args_t *args;
	parser_cases_t *switch_items;
	parser_case_t *switch_item;
}

%%

stmts:
	stmt
	{
		$$ = new parser_stmts_t();
		($$)->push_front($1);

		prsp_ctx *PRSP = prsp_ctx::instance();
		PRSP->_PRSP_stmts = $$;
	}
	| stmt stmts
	{
		$$ = $2;
		($$)->push_front($1);

		prsp_ctx *PRSP = prsp_ctx::instance();
		PRSP->_PRSP_stmts = $$;
	}
	;

stmt:
	declare_stmt
	{
		$$ = $1;
	}
	| control_stmt
	{
		$$ = $1;
	}
	;

declare_stmt:
	type_def variable SEMICOLON
	{
		$$ = new parser_declare_t($1, $2, NULL);
		($$)->push_str($3);
	}
	| type_def variable ASSIGN expr SEMICOLON
	{
		$$ = new parser_declare_t($1, $2, $4);
		($$)->push_str($3);
		($$)->push_str($5);
	}
	| type_def variable array SEMICOLON
	{
		($1)->set_exprs($3);
		$$ = new parser_declare_t($1, $2, NULL);
		($$)->push_str($4);
	}
	| type_def variable array ASSIGN expr SEMICOLON
	{
		($1)->set_exprs($3);
		$$ = new parser_declare_t($1, $2, $5);
		($$)->push_str($4);
		($$)->push_str($6);
	}
	;

type_def:
	type_item
	{
		$$ = new parser_type_t($1, NULL, NULL);
	}
	| type_item pointers
	{
		$$ = new parser_type_t($1, $2, NULL);
	}
	;

type_item:
	TYPE_CHAR
	{
		$$ = new parser_item_t(false, PARSER_TYPE_CHAR);
		($$)->push_str($1);
	}
	| TYPE_UNSIGNED TYPE_CHAR
	{
		$$ = new parser_item_t(true, PARSER_TYPE_CHAR);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	| TYPE_SHORT
	{
		$$ = new parser_item_t(false, PARSER_TYPE_SHORT);
		($$)->push_str($1);
	}
	| TYPE_UNSIGNED TYPE_SHORT
	{
		$$ = new parser_item_t(true, PARSER_TYPE_SHORT);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	| TYPE_INT
	{
		$$ = new parser_item_t(false, PARSER_TYPE_INT);
		($$)->push_str($1);
	}
	| TYPE_UNSIGNED TYPE_INT
	{
		$$ = new parser_item_t(true, PARSER_TYPE_INT);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	| TYPE_UNSIGNED
	{
		$$ = new parser_item_t(true, PARSER_TYPE_INT);
		($$)->push_str($1);
	}
	| TYPE_LONG
	{
		$$ = new parser_item_t(false, PARSER_TYPE_LONG);
		($$)->push_str($1);
	}
	| TYPE_UNSIGNED TYPE_LONG
	{
		$$ = new parser_item_t(true, PARSER_TYPE_LONG);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	| TYPE_FLOAT
	{
		$$ = new parser_item_t(false, PARSER_TYPE_FLOAT);
		($$)->push_str($1);
	}
	| TYPE_DOUBLE
	{
		$$ = new parser_item_t(false, PARSER_TYPE_DOUBLE);
		($$)->push_str($1);
	}
	| TYPE_TIME_T
	{
		$$ = new parser_item_t(false, PARSER_TYPE_TIME_T);
		($$)->push_str($1);
	}
	;

pointers:
	MUL %prec DPOINTER
	{
		$$ = new parser_pointers_t();
		($$)->push_str($1);
		($$)->set_pointers(($$)->get_pointers() + 1);
	}
	| MUL pointers %prec DPOINTER
	{
		$$ = $2;
		($$)->push_str($1);
		($$)->set_pointers(($$)->get_pointers() + 1);
	}
	;

variable:
	IDENT
	{
		$$ = new parser_variable_t(NULL, NULL, $1);
	}
	| IDENT DOT IDENT
	{
		$$ = new parser_variable_t(NULL, $1, $3);
		($$)->push_str($2);
	}
	| IDENT DOT IDENT DOT IDENT
	{
		$$ = new parser_variable_t($1, $3, $5);
		($$)->push_str($2);
		($$)->push_str($4);
	}
	;

array:
	LBRACKET expr RBRACKET
	{
		$$ = new parser_exprs_t();
		($$)->push_front($2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	| LBRACKET expr RBRACKET array
	{
		$$ = $4;
		($$)->push_front($2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	;

control_stmt:
 	expr_stmt
 	{
 		$$ = $1;
 	}
	| if_stmt
	{
 		$$ = $1;
 	}
	| for_stmt
	{
 		$$ = $1;
 	}
	| switch_stmt
	{
 		$$ = $1;
 	}
	| while_stmt
	{
 		$$ = $1;
 	}
	| dowhile_stmt
	{
 		$$ = $1;
 	}
	| goto_stmt
	{
 		$$ = $1;
 	}
	| break_stmt
	{
 		$$ = $1;
 	}
	| continue_stmt
	{
 		$$ = $1;
 	}
	| return_stmt
	{
 		$$ = $1;
 	}
	| blank_stmt
	{
 		$$ = $1;
 	}
	| question_stmt
	{
 		$$ = $1;
 	}
	| compound_stmt
	{
 		$$ = $1;
 	}
	| label_stmt
	{
 		$$ = $1;
 	}
	;

expr_stmt:
	expr SEMICOLON
	{
 		$$ = new parser_expr_stmt_t();
 		($$)->push_front($1);
 		($$)->push_str($2);
 	}
	| expr COMMA expr_stmt
	{
		$$ = $3;
		($$)->push_front($1);
		($$)->push_str($2);
	}
	;

if_stmt:
	IF LCURVE expr RCURVE control_stmt %prec IF
	{
		$$ = new parser_if_stmt_t($3, $5, NULL);
		($$)->push_str($1);
		($$)->push_str($2);
		($$)->push_str($4);
	}
	| IF LCURVE expr RCURVE control_stmt ELSE control_stmt %prec ELSE
	{
		$$ = new parser_if_stmt_t($3, $5, $7);
		($$)->push_str($1);
		($$)->push_str($2);
		($$)->push_str($4);
		($$)->push_str($6);
	}
	;

for_stmt:
	FOR LCURVE extend_expr SEMICOLON extend_expr SEMICOLON extend_expr RCURVE control_stmt
	{
		$$ = new parser_for_stmt_t($3, $5, $7, $9);
		($$)->push_str($1);
		($$)->push_str($2);
		($$)->push_str($4);
		($$)->push_str($6);
		($$)->push_str($8);
	}
	;

extend_expr:
	expr
	{
		$$ = $1;
	}
	|
	{
		$$ = NULL;
	}
	;

switch_stmt:
	SWITCH LCURVE expr RCURVE LBRACE switch_items RBRACE
	{
		$$ = new parser_switch_stmt_t($3, $6);
		($$)->push_str($1);
		($$)->push_str($2);
		($$)->push_str($4);
		($$)->push_str($5);
		($$)->push_str($7);
	}
	;

switch_items:
	switch_item
	{
		$$ = new parser_cases_t();
		($$)->push_front($1);
	}
	| switch_item switch_items
	{
		$$ = $2;
		($$)->push_front($1);
	}
	;

switch_item:
	CASE number_type COLON stmts
	{
		$$ = new parser_case_t($2, $4);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	| DEFAULT COLON stmts
	{
		$$ = new parser_case_t(NULL, $3);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	;

number_type:
	CCONSTANT
	{
		$$ = $1;
 	}
	| ICONSTANT
	{
		$$ = $1;
	}
	| UICONSTANT
	{
		$$ = $1;
	}
	| LCONSTANT
	{
		$$ = $1;
	}
	| ULCONSTANT
	{
		$$ = $1;
	}
	;

while_stmt:
	WHILE LCURVE expr RCURVE control_stmt
	{
		$$ = new parser_while_stmt_t($3, $5);
		($$)->push_str($1);
		($$)->push_str($2);
		($$)->push_str($4);
	}
	;

dowhile_stmt:
	DO control_stmt WHILE LCURVE expr RCURVE SEMICOLON
	{
		$$ = new parser_dowhile_stmt_t($2, $5);
		($$)->push_str($1);
		($$)->push_str($3);
		($$)->push_str($4);
		($$)->push_str($6);
		($$)->push_str($7);
	}
	;

goto_stmt:
	GOTO IDENT SEMICOLON
	{
		$$ = new parser_goto_stmt_t($2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	;

break_stmt:
	BREAK SEMICOLON
	{
		$$ = new parser_break_stmt_t();
		($$)->push_str($1);
		($$)->push_str($2);
	}
	;

continue_stmt:
	CONTINUE SEMICOLON
	{
		$$ = new parser_continue_stmt_t();
		($$)->push_str($1);
		($$)->push_str($2);
	}
	;

return_stmt:
	RETURN SEMICOLON
	{
		$$ = new parser_return_stmt_t(NULL);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	| RETURN expr SEMICOLON
	{
		$$ = new parser_return_stmt_t($2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	;

compound_stmt:
	LBRACE stmts RBRACE
	{
		$$ = new parser_compound_stmt_t($2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	| LBRACE RBRACE
	{
		$$ = new parser_compound_stmt_t(NULL);
		($$)->push_str($1);
		($$)->push_str($2);
	}
	;

blank_stmt:
	SEMICOLON
	{
		$$ = new parser_blank_stmt_t();
		($$)->push_str($1);
	}
	;

question_stmt:
	expr QUESTION expr COLON expr SEMICOLON
	{
		$$ = new parser_question_stmt_t($1, $3, $5);
		($$)->push_str($2);
		($$)->push_str($4);
		($$)->push_str($6);
	}
	;

label_stmt:
	IDENT COLON
	{
		$$ = new parser_label_stmt_t($1);
		($$)->push_str($2);
	}
	;

expr:
	CCONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| ICONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| UICONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| LCONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| ULCONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| FCONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| DCONSTANT
	{
		$$ = new parser_expr_t($1);
	}
	| STRING
	{
		$$ = new parser_expr_t($1);
	}
	| variable
	{
		$$ = new parser_expr_t($1);
	}
	| expr ASSIGN expr
	{
		$$ = new parser_expr_t($1, ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr ADD_ASSIGN expr
	{
		$$ = new parser_expr_t($1, ADD_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr SUB_ASSIGN expr
	{
		$$ = new parser_expr_t($1, SUB_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr MUL_ASSIGN expr
	{
		$$ = new parser_expr_t($1, MUL_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr DIV_ASSIGN expr
	{
		$$ = new parser_expr_t($1, DIV_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr MOD_ASSIGN expr
	{
		$$ = new parser_expr_t($1, MOD_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr LSHIFT_ASSIGN expr
	{
		$$ = new parser_expr_t($1, LSHIFT_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr RSHIFT_ASSIGN expr
	{
		$$ = new parser_expr_t($1, RSHIFT_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr BITAND_ASSIGN expr
	{
		$$ = new parser_expr_t($1, BITAND_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr BITXOR_ASSIGN expr
	{
		$$ = new parser_expr_t($1, BITXOR_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr BITOR_ASSIGN expr
	{
		$$ = new parser_expr_t($1, BITOR_ASSIGN, $3);
		($$)->push_str($2);
	}
	| expr OR expr
	{
		$$ = new parser_expr_t($1, OR, $3);
		($$)->push_str($2);
	}
	| expr AND expr
	{
		$$ = new parser_expr_t($1, AND, $3);
		($$)->push_str($2);
	}
	| expr BITOR expr
	{
		$$ = new parser_expr_t($1, BITOR, $3);
		($$)->push_str($2);
	}
	| expr BITAND expr
	{
		$$ = new parser_expr_t($1, BITAND, $3);
		($$)->push_str($2);
	}
	| expr EQ expr
	{
		$$ = new parser_expr_t($1, EQ, $3);
		($$)->push_str($2);
	}
	| expr NEQ expr
	{
		$$ = new parser_expr_t($1, NEQ, $3);
		($$)->push_str($2);
	}
	| expr LT expr
	{
		$$ = new parser_expr_t($1, LT, $3);
		($$)->push_str($2);
	}
	| expr LE expr
	{
		$$ = new parser_expr_t($1, LE, $3);
		($$)->push_str($2);
	}
	| expr GT expr
	{
		$$ = new parser_expr_t($1, GT, $3);
		($$)->push_str($2);
	}
	| expr GE expr
	{
		$$ = new parser_expr_t($1, GE, $3);
		($$)->push_str($2);
	}
	| expr LSHIFT expr
	{
		$$ = new parser_expr_t($1, LSHIFT, $3);
		($$)->push_str($2);
	}
	| expr RSHIFT expr
	{
		$$ = new parser_expr_t($1, RSHIFT, $3);
		($$)->push_str($2);
	}
	| expr ADD expr
	{
		$$ = new parser_expr_t($1, ADD, $3);
		($$)->push_str($2);
	}
	| expr SUB expr
	{
		$$ = new parser_expr_t($1, SUB, $3);
		($$)->push_str($2);
	}
	| expr MUL expr
	{
		$$ = new parser_expr_t($1, MUL, $3);
		($$)->push_str($2);
	}
	| expr DIV expr
	{
		$$ = new parser_expr_t($1, DIV, $3);
		($$)->push_str($2);
	}
	| expr MOD expr
	{
		$$ = new parser_expr_t($1, MOD, $3);
		($$)->push_str($2);
	}
	| NOT expr
	{
		$$ = new parser_expr_t(NOT, $2);
		($$)->push_str($1);
	}
	| BITNOT expr
	{
		$$ = new parser_expr_t(BITNOT, $2);
		($$)->push_str($1);
	}
	| INC expr
	{
		$$ = new parser_expr_t(INC, $2);
		($$)->push_str($1);
	}
	| expr INC
	{
		$$ = new parser_expr_t($1, INC);
		($$)->push_str($2);
	}
	| DEC expr
	{
		$$ = new parser_expr_t(DEC, $2);
		($$)->push_str($1);
	}
	| expr DEC
	{
		$$ = new parser_expr_t($1, DEC);
		($$)->push_str($2);
	}
	| ADD expr %prec UADD
	{
		$$ = new parser_expr_t(UADD, $2);
		($$)->push_str($1);
	}
	| SUB expr %prec UMINUS
	{
		$$ = new parser_expr_t(UMINUS, $2);
		($$)->push_str($1);
	}
	| MUL expr %prec POINTER
	{
		$$ = new parser_expr_t(POINTER, $2);
		($$)->push_str($1);
	}
	| BITAND expr %prec ADDRESS
	{
		$$ = new parser_expr_t(ADDRESS, $2);
		($$)->push_str($1);
	}
	| SIZEOF expr
	{
		$$ = new parser_expr_t(SIZEOF, $2);
		($$)->push_str($1);
	}
	| IDENT LCURVE args RCURVE
	{
		$$ = new parser_expr_t($1, $3);
		($$)->push_str($2);
		($$)->push_str($4);
	}
	| IDENT LCURVE RCURVE
	{
		$$ = new parser_expr_t($1, NULL);
		($$)->push_str($2);
		($$)->push_str($3);
	}
	| expr array
	{
		$$ = new parser_expr_t($1, $2);
	}
	| LCURVE expr RCURVE
	{
		$$ = new parser_expr_t(LCURVE, $2);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	| LCURVE type_def RCURVE expr
	{
		$$ = new parser_expr_t($2, $4);
		($$)->push_str($1);
		($$)->push_str($3);
	}
	;

args:
	expr
	{
		$$ = new parser_args_t();
		($$)->push_front($1);
	}
	| expr COMMA args
	{
		$$ = $3;
		($$)->push_front($1);
		($$)->push_str($2);
	}
	;

%%

#include <string>
#include <vector>
#include "machine.h"

using namespace ai::sg;
using namespace ai::app;
using namespace std;

void yyerror(const char *s)
{
	if (inquote) {
		cerr << "\n" << (_("ERROR: end of input reached without terminating quote\n")).str(APPLOCALE);
	} else if (incomments) {
		cerr << "\n" << (_("ERROR: end of input reached without terminating */\n")).str(APPLOCALE);
	} else {
		cerr << "\n" << tminline << "\n";
		cerr << std::setfill('^') << std::setw(column) << "" << "\n";
		cerr << std::setfill('^') << std::setw(column) << s << "\n";
	}
	errcnt++;
}

