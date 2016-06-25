#include "sa_internal.h"
namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;

sa_ostarget::sa_ostarget(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	isrc_mgr = dynamic_cast<sa_isocket *>(sa.get_sa_input());
	if (isrc_mgr == NULL) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: source class must be derived from sa_isocket")).str(APPLOCALE));
		exit(1);
	}
}

sa_ostarget::~sa_ostarget()
{
}

void sa_ostarget::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

int sa_ostarget::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	const vector<output_field_t>& output_fields = adaptor.output_fields;
	const char **output = const_cast<const char **>(sa.get_output());

	basio::ip::tcp::socket& socket = isrc_mgr->socket();

	ostringstream fmt;
	for (int i = 0; i < output_fields.size(); i++) {
		if (i > 0)
			fmt << ',';
		fmt << std::setw(output_fields[i].field_size) << output[i];
	}
	fmt << '\n';
	string msg = fmt.str();

	boost::system::error_code error;
	socket.send(basio::buffer(msg), 0, error);
	if (error)
		GPP->write_log((_("WARN: Can't send message to socket, {1}") % error).str(APPLOCALE));

	retval++;
	return retval;
}

void sa_ostarget::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif
}

void sa_ostarget::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_ostarget::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_ostarget::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

DECLARE_SA_OUTPUT(OTYPE_TARGET, sa_ostarget)

}
}

