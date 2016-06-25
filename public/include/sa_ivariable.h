#if !defined(__SA_IVARIABLE_H__)
#define __SA_IVARIABLE_H__

#include "sa_istream.h"

namespace ai
{
namespace app
{

class sa_ivariable : public sa_itext
{
public:
	sa_ivariable(sa_base& _sa, int _flags);
	~sa_ivariable();

private:
	bool to_fields(const input_record_t& input_record);

	char escape;
};

}
}

#endif

