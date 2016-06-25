#include "sgssh_sgip.h"

namespace ai
{
namespace sgssh_sgip
{

using namespace std;
using namespace ai::sg;

sgip::sgip()
{
	SGT = sgt_ctx::instance();
}
sgip::~sgip()
{
}


void sgip::load_sgconf()
{
//	SGT = sgt_ctx::instance();
	sg_config& cfg_mgr = sg_config::instance(SGT);

	for (sg_config::net_iterator net_iter = cfg_mgr.net_begin(); net_iter != cfg_mgr.net_end(); ++net_iter) {
		string naddr = net_iter->naddr;
		if (memcmp(naddr.c_str(), "//", 2) != 0)
			continue;

		string::size_type pos = naddr.find(":");
		if (pos == string::npos)
			continue;

		string address = naddr.substr(2, pos - 2);
		sgip_addr.insert(address);
	//	cout << " address is  " << address << std::endl;
	}

}

}
}
