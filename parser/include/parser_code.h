#if !defined(__PARSER_CODE_H__)
#define __PARSER_CODE_H__

#include "machine.h"
#include "user_exception.h"
#include "prsp_ctx.h"

using namespace std;
using namespace ai::sg;

namespace ai
{
namespace app
{

class parser_string_t;
class parser_expr_t;
class parser_args_t;
class parser_exprs_t;
class parser_stmts_t;

// �����������
enum parser_ctrl_enum
{
	PARSER_CTRL_EXPR,
	PARSER_CTRL_IF,
	PARSER_CTRL_FOR,
	PARSER_CTRL_SWITCH,
	PARSER_CTRL_WHILE,
	PARSER_CTRL_DOWHILE,
	PARSER_CTRL_GOTO,
	PARSER_CTRL_BREAK,
	PARSER_CTRL_CONTINUE,
	PARSER_CTRL_RETURN,
	PARSER_CTRL_BLANK,
	PARSER_CTRL_QUESTION,
	PARSER_CTRL_COMPOUND,
	PARSER_CTRL_LABEL
};

// �������
enum parser_stmt_enum
{
	PARSER_STMT_DECLARE,
	PARSER_STMT_CTRL
};

// ��������
enum parser_variable_enum
{
	PARSER_VARIABLE_INPUT,
	PARSER_VARIABLE_OUTPUT,
	PARSER_VARIABLE_TEMPORARY
};

// ���й���Ļ���
class parser_base
{
public:
	parser_base();
	virtual ~parser_base();

	void set_prefix(const string& _prefix);
	const string& get_prefix() const;
	void push_str(parser_string_t *str);

	const parser_raw_type& get_raw_type() const;
	void set_line_no(int _line_no);
	int get_line_no() const;
	void set_column(int _column);
	int get_column() const;
	void set_is_text(bool _is_text);
	bool get_is_text() const;
	void set_is_unsigned(bool _is_unsigned);
	bool get_is_unsigned() const;
	void set_atomic_type(parser_atomic_type _atomic_type);
	parser_atomic_type get_atomic_type() const;
	void set_pointers(int _pointers);
	int get_pointers() const;

	virtual void preparse(int flags = 0);
	virtual string gen_code(const parser_raw_type *_raw_type = NULL) = 0;
	string insert_str(const string& src, const string& str);
	string convert(const parser_raw_type *dst_type, const parser_raw_type *src_type, const string& value);

protected:
	prsp_ctx *PRSP;
	// �հ׺�ע��
	string prefix;
	// δ���б���������հ׺�ע��
	vector<parser_string_t *> strs;
	parser_raw_type raw_type;
};

// �ַ�����
class parser_string_t : public parser_base
{
public:
	parser_string_t(const string& _value);
	~parser_string_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
	void to_lower();
	const string& get_value() const;

private:
	string value;
};

// ������
class parser_item_t : public parser_base
{
public:
	parser_item_t(bool _is_unsigned, parser_atomic_type _atomic_type);
	~parser_item_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
};

// ָ����
class parser_pointers_t : public parser_base
{
public:
	parser_pointers_t();
	~parser_pointers_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
};

// �����࣬����ԭ�����ͣ�ָ�룬����
class parser_type_t : public parser_base
{
public:
	parser_type_t(parser_item_t *_item, parser_pointers_t *_pointers, parser_exprs_t *_exprs);
	~parser_type_t();

	void preparse(int flags = 0);
	void set_exprs(parser_exprs_t *_exprs);
	string gen_code(const parser_raw_type *_raw_type = NULL);
	string gen_code2();

private:
	parser_item_t *item;
	parser_pointers_t *pointers;
	parser_exprs_t *exprs;
};

// ��������
class parser_variable_t : public parser_base
{
public:
	parser_variable_t(parser_string_t *_direction, parser_string_t *_table, parser_string_t *_variable);
	~parser_variable_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);
	string get_value() const;

private:
	parser_string_t *direction;
	parser_string_t *table;
	parser_string_t *variable;

	string code_prefix;
};

// ���������б�
class parser_args_t : public parser_base
{
public:
	parser_args_t();
	~parser_args_t();

	void push_front(parser_expr_t *arg);
	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	deque<parser_expr_t *> args;
};

// ���ʽ��
class parser_expr_t : public parser_base
{
public:
	parser_expr_t(parser_string_t *_value);
	parser_expr_t(parser_variable_t *_variable);
	parser_expr_t(parser_variable_t *_variable, int _op, parser_expr_t *_right);
	parser_expr_t(int _op, parser_expr_t *_right);
	parser_expr_t(parser_expr_t *_left, int _op);
	parser_expr_t(parser_expr_t *_left, int _op, parser_expr_t *_right);
	parser_expr_t(parser_string_t *_ident, parser_args_t *_args);
	parser_expr_t(parser_expr_t *_left, parser_exprs_t *_exprs);
	parser_expr_t(parser_type_t *_type, parser_expr_t *_right);
	~parser_expr_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	void set_type(const parser_raw_type *ltype, const parser_raw_type *rtype);
	void set_ctype(const parser_raw_type *ltype, const parser_raw_type *rtype);
	void combine_type(const parser_raw_type& ltype, const parser_raw_type& rtype);

	// ���캯������
	int expr_type;
	// ����
	parser_string_t *value;
	// ����
	parser_variable_t *variable;
	// Ԫ����
	parser_expr_t *left;
	int op;
	parser_expr_t *right;
	// ����
	parser_string_t *ident;
	parser_args_t *args;
	// ����
	parser_exprs_t *exprs;
	// ת������
	parser_type_t *type;
	// ת�������е�����
	parser_raw_type cltype;
	parser_raw_type crtype;
};

// ���ʽ����
class parser_exprs_t : public parser_base
{
public:
	parser_exprs_t();
	~parser_exprs_t();

	void push_front(parser_expr_t *expr);
	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);
	const deque<parser_expr_t *>& get_exprs() const;

private:
	deque<parser_expr_t *> exprs;
};

// ������
class parser_stmt_t : public parser_base
{
public:
	parser_stmt_t();
	~parser_stmt_t();

	parser_stmt_enum get_stmt_type() const;

protected:
	parser_stmt_enum stmt_type;
};

// �������
class parser_stmts_t : public parser_base
{
public:
	parser_stmts_t();
	~parser_stmts_t();

	void push_front(parser_stmt_t *stmt);
	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	deque<parser_stmt_t *> stmts;
};

// ���������
class parser_declare_t : public parser_stmt_t
{
public:
	parser_declare_t(parser_type_t *_type, parser_variable_t *_variable, parser_expr_t *_expr);
	~parser_declare_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_type_t *type;
	parser_variable_t *variable;
	parser_expr_t *expr;
};

// ���������
class parser_ctrl_t : public parser_stmt_t
{
public:
	parser_ctrl_t();
	~parser_ctrl_t();
};

// ���ʽ�����
class parser_expr_stmt_t : public parser_ctrl_t
{
public:
	parser_expr_stmt_t();
	~parser_expr_stmt_t();

	void push_front(parser_expr_t *_expr);
	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_exprs_t exprs;
};

// ���������
class parser_if_stmt_t : public parser_ctrl_t
{
public:
	parser_if_stmt_t(parser_expr_t *_cond, parser_stmt_t *_if_stmt, parser_stmt_t *_else_stmt);
	~parser_if_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *cond;
	parser_stmt_t *if_stmt;
	parser_stmt_t *else_stmt;
};

// for�����
class parser_for_stmt_t : public parser_ctrl_t
{
public:
	parser_for_stmt_t(parser_expr_t *_init, parser_expr_t *_cond, parser_expr_t *_modify, parser_stmt_t *_stmt);
	~parser_for_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *init;
	parser_expr_t *cond;
	parser_expr_t *modify;
	parser_stmt_t *stmt;
};

// case�־���
class parser_case_t : public parser_base
{
public:
	parser_case_t(parser_string_t *_number_type, parser_stmts_t *_stmts);
	~parser_case_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_string_t *number_type;
	parser_stmts_t *stmts;
};

// case�־�����
class parser_cases_t : public parser_base
{
public:
	parser_cases_t();
	~parser_cases_t();

	void push_front(parser_case_t *item);
	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	deque<parser_case_t *> cases;
};

// switch�����
class parser_switch_stmt_t : public parser_ctrl_t
{
public:
	parser_switch_stmt_t(parser_expr_t *_cond, parser_cases_t *_cases);
	~parser_switch_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *cond;
	parser_cases_t *cases;
};

// while�����
class parser_while_stmt_t : public parser_ctrl_t
{
public:
	parser_while_stmt_t(parser_expr_t *_cond, parser_ctrl_t *_stmt);
	~parser_while_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *cond;
	parser_ctrl_t *stmt;
};

// dowhile�����
class parser_dowhile_stmt_t : public parser_ctrl_t
{
public:
	parser_dowhile_stmt_t(parser_ctrl_t *_stmt, parser_expr_t *_cond);
	~parser_dowhile_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_ctrl_t *stmt;
	parser_expr_t *cond;
};

// goto�����
class parser_goto_stmt_t : public parser_ctrl_t
{
public:
	parser_goto_stmt_t(parser_string_t *_ident);
	~parser_goto_stmt_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_string_t *ident;
};

// break�����
class parser_break_stmt_t : public parser_ctrl_t
{
public:
	parser_break_stmt_t();
	~parser_break_stmt_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
};

// continue�����
class parser_continue_stmt_t : public parser_ctrl_t
{
public:
	parser_continue_stmt_t();
	~parser_continue_stmt_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
};

// return�����
class parser_return_stmt_t : public parser_ctrl_t
{
public:
	parser_return_stmt_t(parser_expr_t *_expr);
	~parser_return_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *expr;
};

// ���������
class parser_compound_stmt_t : public parser_ctrl_t
{
public:
	parser_compound_stmt_t(parser_stmts_t *_stmts);
	~parser_compound_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_stmts_t *stmts;
};

// �������
class parser_blank_stmt_t : public parser_ctrl_t
{
public:
	parser_blank_stmt_t();
	~parser_blank_stmt_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);
};

// ѡ�������
class parser_question_stmt_t : public parser_ctrl_t
{
public:
	parser_question_stmt_t(parser_expr_t *_cond, parser_expr_t *_expr1, parser_expr_t *_expr2);
	~parser_question_stmt_t();

	void preparse(int flags = 0);
	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_expr_t *cond;
	parser_expr_t *expr1;
	parser_expr_t *expr2;
};

// ��ǩ�����
class parser_label_stmt_t : public parser_ctrl_t
{
public:
	parser_label_stmt_t(parser_string_t *_ident);
	~parser_label_stmt_t();

	string gen_code(const parser_raw_type *_raw_type = NULL);

private:
	parser_string_t *ident;
};

}
}

#endif

