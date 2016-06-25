#ifndef YYERRCODE
#define YYERRCODE 256
#endif

#define BADTOKEN 257
#define COLON 258
#define LBRACE 259
#define RBRACE 260
#define SEMICOLON 261
#define NULL_OP 262
#define SWITCH 263
#define CASE 264
#define BREAK 265
#define CONTINUE 266
#define DEFAULT 267
#define DO 268
#define WHILE 269
#define FOR 270
#define GOTO 271
#define RETURN 272
#define TYPE_CHAR 273
#define TYPE_SHORT 274
#define TYPE_INT 275
#define TYPE_LONG 276
#define TYPE_FLOAT 277
#define TYPE_DOUBLE 278
#define TYPE_TIME_T 279
#define TYPE_UNSIGNED 280
#define CCONSTANT 281
#define ICONSTANT 282
#define UICONSTANT 283
#define LCONSTANT 284
#define ULCONSTANT 285
#define FCONSTANT 286
#define DCONSTANT 287
#define IDENT 288
#define STRING 289
#define IF 290
#define ELSE 291
#define COMMA 292
#define QUESTION 293
#define ASSIGN 294
#define ADD_ASSIGN 295
#define SUB_ASSIGN 296
#define MUL_ASSIGN 297
#define DIV_ASSIGN 298
#define MOD_ASSIGN 299
#define LSHIFT_ASSIGN 300
#define RSHIFT_ASSIGN 301
#define BITAND_ASSIGN 302
#define BITXOR_ASSIGN 303
#define BITOR_ASSIGN 304
#define OR 305
#define AND 306
#define BITOR 307
#define BITXOR 308
#define BITAND 309
#define EQ 310
#define NEQ 311
#define LT 312
#define LE 313
#define GT 314
#define GE 315
#define LSHIFT 316
#define RSHIFT 317
#define ADD 318
#define SUB 319
#define MUL 320
#define DIV 321
#define MOD 322
#define DPOINTER 323
#define NOT 324
#define BITNOT 325
#define INC 326
#define DEC 327
#define UMINUS 328
#define UADD 329
#define DOT 330
#define POINTER 331
#define ADDRESS 332
#define SIZEOF 333
#define LCURVE 334
#define RCURVE 335
#define LBRACKET 336
#define RBRACKET 337
typedef union
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
} YYSTYPE;
extern YYSTYPE yylval;
