#include "prsp_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

boost::once_flag prsp_ctx::once_flag = BOOST_ONCE_INIT;
prsp_ctx * prsp_ctx::_instance = NULL;

prsp_ctx * prsp_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, prsp_ctx::init_once);
	return _instance;
}

prsp_ctx::prsp_ctx()
{
	_PRSP_stmts = NULL;

	parser_raw_type raw_type;

	raw_type.set_is_text(false);
	raw_type.set_is_unsigned(false);
	raw_type.set_atomic_type(PARSER_TYPE_INT);
	raw_type.set_pointers(0);
	_PRSP_func_types["strcpy"] = raw_type;
	_PRSP_func_types["strncpy"] = raw_type;
	_PRSP_func_types["memcpy"] = raw_type;
	_PRSP_func_types["sprintf"] = raw_type;
	_PRSP_func_types["atoi"] = raw_type;

	raw_type.set_is_text(false);
	raw_type.set_is_unsigned(false);
	raw_type.set_atomic_type(PARSER_TYPE_LONG);
	raw_type.set_pointers(0);
	_PRSP_func_types["atol"] = raw_type;

	raw_type.set_is_text(false);
	raw_type.set_is_unsigned(false);
	raw_type.set_atomic_type(PARSER_TYPE_DOUBLE);
	raw_type.set_pointers(0);
	_PRSP_func_types["atof"] = raw_type;
}

prsp_ctx::~prsp_ctx()
{
}

void prsp_ctx::init_once()
{
	_instance = new prsp_ctx();
}

}
}

