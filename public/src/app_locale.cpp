#include "app_locale.h"
#include "user_exception.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

const char DOMAIN_NAME[] = "app";

app_locale app_locale::_instance;

app_locale::app_locale()
{
	char *ptr = ::getenv("APPROOT");
	if (ptr == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: APPROOT environment not set")).str(APPLOCALE));

	std::string path = ptr;
	path += "/etc";

	gen.add_messages_path(path);
	gen.add_messages_domain(DOMAIN_NAME);

	ptr = ::getenv("SGLANG");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		::putenv(strdup("LANG=zh_CN.GBK"));
	_locale = gen("");
}

app_locale::~app_locale()
{
}

std::locale& app_locale::locale()
{
	return _instance._locale;
}

}
}

