#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

sa_output::sa_output(sa_base& _sa, int _flags)
	: sa(_sa),
	  flags(_flags),
	  adaptor(SAP->_SAP_adaptors[sa.get_id()]),
	  is_open(false)
{
}

sa_output::~sa_output()
{
}

void sa_output::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_output::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_output::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	is_open = true;
}

int sa_output::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	if (!is_open)
		open();

	return retval;
}

int sa_output::write(std::string& input_str, std::string& err_str)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_str={1}") % input_str).str(APPLOCALE), &retval);
#endif

	if (!is_open)
		open();

	return retval;
}


void sa_output::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	if (!is_open)
		return;
}

void sa_output::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	is_open = false;
}

void sa_output::complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	is_open = false;
}

void sa_output::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	is_open = false;
}

void sa_output::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_output::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	is_open = false;
}

void sa_output::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif
}

void sa_output::global_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

DECLARE_SA_OUTPUT(OTYPE_INVALID | OTYPE_ERROR | OTYPE_TARGET | OTYPE_SUMMARY | OTYPE_DISTR | OTYPE_STAT, sa_output)

}
}

