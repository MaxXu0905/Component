#if !defined(__APP_LOCALE_H__)
#define __APP_LOCALE_H__

#include <boost/locale.hpp>

#if defined(_)
#undef _
#endif

#define _(text) boost::locale::format(boost::locale::translate(text))
#define APPLOCALE ai::app::app_locale::locale()

namespace ai
{
namespace app
{

class app_locale
{
public:
	app_locale();
	~app_locale();

	static std::locale& locale();

private:
	boost::locale::generator gen;
	std::locale _locale;

	static app_locale _instance;
};

}
}

#endif

