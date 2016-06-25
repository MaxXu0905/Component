#if !defined(__SA_ITEXT_H__)
#define __SA_ITEXT_H__

#include "sa_istream.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

int const RECORD_FLAG_HAS_HEAD = 0x1;	// file has head
int const RECORD_FLAG_HAS_TAIL = 0x2;	// file has tail

class sa_itext : public sa_istream
{
public:
	sa_itext(sa_base& _sa, int _flags);
	~sa_itext();

protected:
	virtual int read();
	virtual int do_record();
	virtual bool to_fields(const input_record_t& input_record) = 0;
	int get_record_serial(const char *record) const;

	int record_flag;
};

}
}

#endif

