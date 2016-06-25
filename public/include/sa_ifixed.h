#if !defined(__SA_IFIXED_H__)
#define __SA_IFIXED_H__

#include "sa_itext.h"

namespace ai
{
namespace app
{

class sa_ifixed : public sa_itext
{
public:
	sa_ifixed(sa_base& _sa, int _flags);
	~sa_ifixed();

protected:
	virtual bool to_fields(const input_record_t& input_record);
};

}
}

#endif

