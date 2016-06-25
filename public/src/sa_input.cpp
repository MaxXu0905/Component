#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

sa_input::sa_input(sa_base& _sa, int _flags)
	: sa(_sa),
	  flags(_flags),
	  adaptor(SAP->_SAP_adaptors[_sa.get_id()]),
	  master(SAT->_SAT_dblog[_sa.get_id()].master)
{
}

sa_input::~sa_input()
{
}

void sa_input::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_input::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

bool sa_input::open()
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

void sa_input::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	master.error_type = (master.record_error == 0 ? ERROR_TYPE_SUCCESS : ERROR_TYPE_PARTIAL);
	master.completed = (completed ? 2 : 0);
}

void sa_input::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.completed = 1;
}

void sa_input::complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.completed = 1;
}

void sa_input::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.error_type = ERROR_TYPE_FATAL;
	master.completed = 1;
}

void sa_input::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

bool sa_input::validate()
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

int sa_input::read()
{
	throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported API")).str(APPLOCALE));
}

void sa_input::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif
}

void sa_input::iclean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_input::global_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

DECLARE_SA_INPUT(0, sa_input)

}
}

