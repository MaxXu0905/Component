#include "svcp_ctx.h"

namespace ai
{
namespace app
{

boost::once_flag svcp_ctx::once_flag = BOOST_ONCE_INIT;
svcp_ctx * svcp_ctx::_instance = NULL;

svcp_ctx * svcp_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, svcp_ctx::init_once);
	return _instance;
}

svcp_ctx::svcp_ctx()
{
}

svcp_ctx::~svcp_ctx()
{
}

void svcp_ctx::init_once()
{
	_instance = new svcp_ctx();
}

}
}

