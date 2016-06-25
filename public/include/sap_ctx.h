#if !defined(__SAP_CTX_H__)
#define __SAP_CTX_H__

#include "sa_internal.h"

namespace bi = boost::interprocess;
using namespace ai::sg;

namespace ai
{
namespace app
{

struct sap_ctx
{
public:
	static sap_ctx * instance();
	~sap_ctx();

	void dump(ostream& os);
	sa_base * sa_base_factory(const std::string& com_key);
	sa_input * sa_input_factory(sa_base& sa, int flags, const std::string& class_name);
	sa_output * sa_output_factory(sa_base& sa, int flags, const std::string& class_name);

	pid_t _SAP_pid;			// ��ǰ����pid

	std::vector<sa_base_creator_t> _SAP_base_creator;		// sa_base��������
	std::vector<sa_input_creator_t> _SAP_input_creator;	// sa_input��������
	std::vector<sa_output_creator_t> _SAP_output_creator;	// sa_output��������

	bool _SAP_nclean;						// �Ƿ�������ڵ�����ļ�
	int _SAP_undo_id;						// ����ID
	sa_resource_t _SAP_resource;			// SAȫ�ֲ���
	sa_global_t _SAP_global;				// ȫ�ֱ�������
	std::vector<sa_adaptor_t> _SAP_adaptors;	// ����SA�Ĳ�������
	compiler _SAP_cmpl;					// compiler
	std::vector<void *> _SAP_lib_handles;		// ��̬����

	bool _SAP_auto_mkdir;					// �Զ�����Ŀ¼
	int _SAP_record_no;

private:
	sap_ctx();

	static void init_once();

	static boost::once_flag once_flag;
	static sap_ctx *_instance;
};

}
}

#endif

