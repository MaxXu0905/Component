#include "parser_config.h"
#include "gpp_ctx.h"
#include "app_locale.h"
#include <boost/program_options.hpp>

namespace po = boost::program_options;
using namespace ai::sg;
using namespace ai::app;
using namespace std;

int main(int argc, char *argv[])
{
	string input_file;
	string output_file;
	gpp_ctx *GPP = gpp_ctx::instance();

	GPP->set_procname(argv[0]);

	po::options_description desc((_("Allowed options")).str(APPLOCALE));
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("input,i", po::value<string>(&input_file)->required(), (_("specify input XML configuration file, 1st positional parameter takes same effect.")).str(APPLOCALE).c_str())
		("output,o", po::value<string>(&output_file)->required(), (_("specify output XML configuration file")).str(APPLOCALE).c_str())
	;

	po::positional_options_description pdesc;
	pdesc.add("input", 1);

	try {
		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(desc).positional(pdesc).run(), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			exit(0);
		}

		po::notify(vm);
	} catch (exception& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		exit(1);
	}

	try {
		parser_config parser_mgr;

		parser_mgr.run(input_file, output_file);
		return 0;
	} catch (exception& ex) {
		cout << ex.what() << std::endl;
		return -1;
	}

	cout << (_("INFO: Parse finished successfully")).str(APPLOCALE) << "\n\n";
	::exit(0);
}

