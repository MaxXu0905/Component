#if !defined(__SVCP_CTX_H__)
#define __SVCP_CTX_H__

#include "sa_struct.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

struct svc_parms_t
{
	std::string svc_key;
	int version;
	bool disable_global;
};

struct svc_adaptor_t
{
	svc_parms_t parms;					// ͨ�ò���

	// Global definition.
	field_map_t global_map;				// ȫ�ֱ������±�Ķ�Ӧ��ϵ
	std::vector<int> input_globals;			// ��Ϊ���봫�ݵ�ȫ�ֱ����б�
	std::vector<int> output_globals;			// ��Ϊ������ݵ�ȫ�ֱ����б�
	std::vector<int> global_sizes;				// ȫ�ֱ�����������

	// Input definition.
	std::vector<field_map_t> input_maps;		// ����������±�Ķ�Ӧ��ϵ
	std::vector<int> input_sizes;				// ���������������

	// Output definition.
	field_map_t output_map;				// ����������±�Ķ�Ӧ��ϵ
	std::vector<int> output_sizes;			// ���������������
};

class svcp_ctx
{
public:
	static svcp_ctx * instance();
	~svcp_ctx();

	// SVC_KEY�����ö����ӳ���ϵ
	boost::unordered_map<std::string, svc_adaptor_t> _SVCP_adaptors;

private:
	svcp_ctx();

	static void init_once();

	static boost::once_flag once_flag;
	static svcp_ctx *_instance;
};

}
}

#endif

