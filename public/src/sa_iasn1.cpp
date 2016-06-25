#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

void parent_children_t::clear()
{
	parent_var = false;
	children_var = false;
	parent_serial = -1;
	parent_type = -1;
	parent_start = -1;
	header_len = 0;
	parent_len = 0;
	children_len = 0;
}

sa_iasn1::sa_iasn1(sa_base& _sa, int _flags)
	: sa_istream(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;

	iter = args.find("max_block_len");
	if (iter == args.end()) {
		max_block_len = MAX_BLOCK_LEN;
	} else {
		max_block_len = atoi(iter->second.c_str());
		if (max_block_len < 0)
			max_block_len = MAX_BLOCK_LEN;
	}

	iter = args.find("file_root_tag");
	if (iter == args.end()) {
		file_root_tag = -1;
	} else {
		const string& file_root_tag_str = iter->second;
		if (strncasecmp(file_root_tag_str.c_str(), "0x", 2) == 0)
			file_root_tag = strtol(file_root_tag_str.c_str(), NULL, 16);
		else
			file_root_tag = atoi(file_root_tag_str.c_str());
	}

	iter = args.find("body_node_tag");
	if (iter == args.end()) {
		body_node_tag = -1;
	} else {
		const string& file_root_tag_str = iter->second;
		if (strncasecmp(file_root_tag_str.c_str(), "0x", 2) == 0)
			body_node_tag = strtol(file_root_tag_str.c_str(), NULL, 16);
		else
			body_node_tag = atoi(file_root_tag_str.c_str());
	}

	iter = args.find("block_data_tag");
	if (iter == args.end()) {
		block_data_tag = -1;
	} else {
		const string& file_root_tag_str = iter->second;
		if (strncasecmp(file_root_tag_str.c_str(), "0x", 2) == 0)
			block_data_tag = strtol(file_root_tag_str.c_str(), NULL, 16);
		else
			block_data_tag = atoi(file_root_tag_str.c_str());
	}

	iter = args.find("block_filler");
	if (iter == args.end())
		block_filler = -1;
	else
		block_filler = atoi(iter->second.c_str());

	iter = args.find("skip_record_len");
	if (iter == args.end()) {
		skip_record_len = 0;
	} else {
		skip_record_len = atoi(iter->second.c_str());
		if (skip_record_len < 0)
			skip_record_len = 0;
	}

	body_one_cdr = false;
	block_one_cdr = false;

	iter = args.find("options");
	if (iter != args.end()) {
		boost::char_separator<char> sep(" \t\b");
		tokenizer tokens(iter->second, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string str = *iter;

			if (strcasecmp(str.c_str(), "BODY_ONE_CDR") == 0)
				body_one_cdr = true;
			else if (strcasecmp(str.c_str(), "BLOCK_ONE_CDR") == 0)
				block_one_cdr = true;
		}
	}

	has_body_tag = (body_node_tag != -1);
	has_block_tag = (block_data_tag != -1);

	const vector<input_record_t>& input_records = adaptor.input_records;
	BOOST_FOREACH(const input_record_t& input_record, input_records) {
		const vector<input_field_t>& field_vector = input_record.field_vector;

		if (field_vector[0].field_name != "record_tag")
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: first field_name in adaptor.input.table.fields must be record_tag")).str(APPLOCALE));

		if (field_vector[0].factor2 < 10)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: first field's factor2 in adaptor.input.table.fields must be at least 10")).str(APPLOCALE));
	}

	asnfile_ptr = NULL;
	block_buf = new char[block_len];
}

sa_iasn1::~sa_iasn1()
{
	delete []block_buf;
	delete asnfile_ptr;
}

int sa_iasn1::read()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif
	asn1& asnfile = *asnfile_ptr;

	try {
		while (!asnfile.eof()) {
			AsnTag tag_id;
			AsnLen tag_len = 0;
			AsnLen len_len = 0;
			AsnLen content_len;

			if (skip_record_len > 0)
				asnfile.buf_skip(skip_record_len);

			tag_id = asnfile.dec_tag(tag_len, block_filler);
			dout << "tag_id = 0x" << std::setbase(16) << tag_id << std::endl;
			dout << "block_tag = 0x" << std::setbase(16) << block_tag << std::endl;
			dout << "body_tag = 0x" << std::setbase(16) << body_tag << std::endl;
			dout << "skip_record_len = " << std::setbase(10) << skip_record_len << std::endl;

			if (tag_id == block_filler) { // skip remain bytes input the block
				if (has_block_tag) {
					dout << "record_tag = 0x" << std::setbase(16) << record_tag << std::endl;
					dout << "buffer_len = " << std::setbase(10) << buffer_len << std::endl;
					if (block_one_cdr && buffer_len > 0) {
						retval = do_record(record_tag, buffer_len);
						return retval;
					}

					record_tag = -1;
					buffer_len = 0;
					block_tag = -1;
					block_len = 0;
					continue;
				} else {
					int skip_len = asnfile.tell() % max_block_len;
					if (skip_len > 0) {
						skip_len = max_block_len - skip_len;
						asnfile.buf_skip(skip_len);
						continue;
					}
				}
			}

			if (has_block_tag && block_filler != -1)
				content_len = asnfile.dec_len(len_len, true);
			else
				content_len = asnfile.dec_len(len_len);

			if (content_len < 0 && block_filler == -1)
				throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: tag = {1} and len is variable, but block_filler is -1") % tag_id).str(APPLOCALE));

			dout << "content_len = 0x" << std::setbase(16) << content_len << std::endl;

			if (has_file_tag) { // First time, and it have a file root tag
				if (tag_id != file_root_tag || asnfile.tell() + content_len > end_pos) {
					file_correct = false;
					break;
				}
				has_file_tag = false;
				continue;
			}

			if (has_block_tag && block_tag != block_data_tag) {
				if (tag_id == block_data_tag &&
					(!has_body_tag || body_tag != body_node_tag)) {
					block_tag = block_data_tag;
					block_len = content_len;
					block_start = asnfile.tell();
					continue;
				}
			}

			// pay attention that body_node_tag may be -1, then it doesn't match any tag
			if (has_block_tag && block_tag == block_data_tag && block_one_cdr) {
				if (has_body_tag && body_tag != body_node_tag && record_tag == -1) {
					if (tag_id != body_node_tag) {
						if (content_len > 0)
							asnfile.buf_skip(content_len);
						continue;
					}

					body_tag = body_node_tag;
					body_len = content_len;
					body_tag_len = tag_len;
					body_len_len = len_len;
					body_start = asnfile.tell();
					continue;
				}
			} else {
				if (has_body_tag && body_tag != body_node_tag) {
					if (tag_id != body_node_tag) {
						if (content_len > 0)
							asnfile.buf_skip(content_len);
						continue;
					}

					body_tag = body_node_tag;
					body_len = content_len;
					body_tag_len = tag_len;
					body_len_len = len_len;
					body_start = asnfile.tell();
					continue;
				}
			}

			record_len = content_len;
			if (has_body_tag && body_tag == body_node_tag && body_one_cdr) {
				record_len =  body_len;
				record_len -= tag_len;
				record_len -= len_len;
			}

			if (has_block_tag && block_tag == block_data_tag && block_one_cdr) {
				asnfile.buf_copy(block_buf + buffer_len, record_len);
				buffer_len += record_len;
			} else {
				asnfile.buf_copy(block_buf, record_len);
				retval = do_record(tag_id, record_len);
				return retval;
			}

			if (has_block_tag && block_tag == block_data_tag &&
				block_one_cdr && record_tag == -1)
				record_tag = tag_id;

			if (has_body_tag) {
				if (body_tag == body_node_tag && asnfile.tell() - body_start >= body_len)
					body_tag = -1;
			}

			if (has_block_tag) {
				if (block_tag == block_data_tag && block_len >= 0 && asnfile.tell() - block_start >= block_len) {
					block_tag = -1;
					block_len = 0;
				}
			}
		}
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, ex.what());
		retval = SA_FATAL;
		return retval;
	}

	retval = SA_EOF;
	return retval;
}

int sa_iasn1::do_record(AsnTag tag_, AsnLen len_)
{
	char tag_str[32];
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, (_("tag_={1}, len_={2}") % tag_ % len_).str(APPLOCALE), &retval);
#endif

	::sprintf(tag_str, "0x%x", tag_);
	int record_serial = get_record_serial(tag_str);

	if (record_serial < 0) {
		retval = SA_INVALID;
		return retval;
	}

	const input_record_t& input_record = adaptor.input_records[record_serial];

	try {
		asn2record(input_record, tag_str, len_);
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, ex.what());
		retval = SA_FATAL;
	}

	return retval;
}

void sa_iasn1::asn2record(const input_record_t& input_record, const char *tag_str, AsnLen len_)
{
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, (_("tag_str={1}, len_={2}") % tag_str % len_).str(APPLOCALE), NULL);
#endif
	vector<input_field_t>::const_iterator iter;
	char **input = sa.get_input();
	size_t field_start;
	asn1 asnrec(block_buf, len_, max_block_len);
	stack<parent_children_t> parent_children;

	// For ASN.1 format, the first field must be record_tag
	strcpy(input[FIXED_INPUT_SIZE], tag_str);

	while (!asnrec.eof()) {
		AsnLen tag_len = 0;
		AsnLen len_len = 0;
		AsnLen tmp_len;
		AsnLen len;

		AsnTag tag = asnrec.dec_tag(tag_len, block_filler);
		dout << "asn2record tag1 = 0x" << std::setbase(16) << tag << std::endl;

		input_field_t field;
		field.factor1 = tag;
		if (parent_children.empty())
			field.parent_serial = -1;
		else
			field.parent_serial = parent_children.top().parent_serial;

		set<input_field_t>::const_iterator iter = input_record.field_set.find(field);
		if (iter == input_record.field_set.end() && tag == block_filler) {
			if (!parent_children.empty() && parent_children.top().parent_len != 0) {
				if (parent_children.top().parent_var && parent_children.top().children_var)
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: Cannot support if parent and children are all variable.")).str(APPLOCALE));

				if (parent_children.top().children_var) {
					parent_children.top().children_len = asnrec.tell();
					parent_children.top().children_len -= parent_children.top().parent_start;
					parent_children.top().children_len += parent_children.top().header_len;

					if (parent_children.top().parent_len == parent_children.top().children_len)
						parent_children.pop();
				} else {
					parent_children.top().children_len += tag_len;
				}

				if (parent_children.top().parent_var) {
					parent_children.top().parent_len = asnrec.tell();
					parent_children.top().parent_len -= parent_children.top().parent_start;
					parent_children.top().parent_len += parent_children.top().header_len;
					// so pop the top element
					parent_children.pop();
				}
			}

			continue;
		}

		while (!parent_children.empty() && parent_children.top().parent_len != 0
			&& parent_children.top().parent_len == parent_children.top().children_len)
			parent_children.pop();

		if (body_one_cdr && block_filler != -1 && body_node_tag != -1)
			len = asnrec.dec_len(len_len, true);
		else
			len = asnrec.dec_len(len_len);

		field_start = asnrec.tell();
		dout << "asn2record len = 0x" << std::setbase(16) << len << std::endl;

		if (len < 0) {
			if (block_filler != -1) {
				if (iter == input_record.field_set.end()) {
					while (!asnrec.eof()) {
						tag = asnrec.dec_tag(tag_len, block_filler);
						if (tag == block_filler)
							break;
					}
					len = asnrec.tell() - field_start;
				} else if (!parent_children.empty()) {
					parent_children.top().children_var = true;
					if (parent_children.top().parent_var && parent_children.top().children_var)
						throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: Cannot support if parent and children are all variable.")).str(APPLOCALE));
				}
			} else {
				throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: tag = {1} and len is variable, but block_filler is -1") % tag).str(APPLOCALE));
			}
		}

		dout << "parent_children size = [" << std::setbase(10) << parent_children.size() << "]" << std::endl;
		if (!parent_children.empty())
			dout << "last parent serial = [" << std::setbase(10) << parent_children.top().parent_serial << "]" << std::endl;

		if (!parent_children.empty() && parent_children.top().parent_len != 0) {
			parent_children.top().children_len += tag_len;
			parent_children.top().children_len += len_len;
			if (len > 0)
				parent_children.top().children_len += len;
			if (parent_children.top().parent_var == false &&
				parent_children.top().children_var == false &&
				parent_children.top().parent_len < parent_children.top().children_len)
				throw bad_asn1(__FILE__, __LINE__, 22, (_("ERROR: parent length not correct")).str(APPLOCALE));
		}

		dout << "asn2record tag2 = 0x" << std::setbase(16) << tag << std::endl;
		dout << "asn2record len2 = 0x" << std::setbase(16) << len << std::endl;
		dout << "asnrec.tell() = 0x" << std::setbase(16) << asnrec.tell() << std::endl;

		if (iter != input_record.field_set.end()) {
			dout << "matched tag = 0x" << std::setbase(16) << iter->factor1 << std::endl;
			dout << "encode_type = " << iter->encode_type << std::endl;
			dout << "parent_serial = " << std::setbase(10) << iter->parent_serial << std::endl;

			if (len == 0) {
				::strcpy(input[iter->field_serial], "1");
				continue;
			}

			char buf[MAX_BLOCK_LEN];
			char seq_buf[MAX_BLOCK_LEN];

			if (len > MAX_BLOCK_LEN)
				throw bad_msg(__FILE__, __LINE__, 412, (_("ERROR: field length exceeds MAX_BLOCK_SIZE")).str(APPLOCALE));

			switch (iter->encode_type) {
			case ENCODE_ADDRSTRING:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.tbcd2asc(buf + 1, seq_buf, len * 2 - 2);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.tbcd2asc(buf + 1, input[iter->field_serial], len * 2 - 2);
					}
				}
				break;
			case ENCODE_BCD:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.bcd2asc(buf, seq_buf, len * 2);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.bcd2asc(buf, input[iter->field_serial], len * 2);
					}
				}
				break;
			case ENCODE_TBCD:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.tbcd2asc(buf, seq_buf, len * 2);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
 						}
					} else {
						asnrec.tbcd2asc(buf, input[iter->field_serial], len * 2);
					}
				}
				break;
			case ENCODE_REALBCD:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.realbcd2asc(buf, seq_buf, len * 2);
						if (::strlen(input[iter->field_serial]) == 0) {
							strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
 						}
					} else {
						asnrec.realbcd2asc(buf, input[iter->field_serial], len * 2);
					}
				}
				break;
			case ENCODE_REALTBCD:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.realtbcd2asc(buf, seq_buf, len * 2);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.realtbcd2asc(buf, input[iter->field_serial], len * 2);
					}
				}
				break;
			case ENCODE_BCDD:
				if (len * 2 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
				} else {
					asnrec.buf_copy(buf, len);
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.tbcd2asc(buf + 2, seq_buf, len * 2 - 4);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.tbcd2asc(buf + 2, input[iter->field_serial], len * 2 - 4);
					}
				}
				break;
			case ENCODE_OCTETSTRING:
				try {
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.dec_octet_string_content(len, seq_buf, iter->factor2, tmp_len);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.dec_octet_string_content(len, input[iter->field_serial], iter->factor2, tmp_len);
					}
				} catch (bad_asn1& ex) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it") % iter->field_name ).str(APPLOCALE));
				}
				break;
			case ENCODE_INTEGER:
				AsnInt tmpint;
				asnrec.dec_int_content(tag, len, tmpint, tmp_len);
				tmpint = sprintf(buf, "%ld", (long)tmpint) - 1;
				if (tmpint > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % tmpint).str(APPLOCALE));
				} else {
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], buf);
						}
					} else {
						::strcpy(input[iter->field_serial], buf);
					}
				}
				break;
			case ENCODE_UNINTEGER:
				UAsnInt tmpint1;
				asnrec.dec_uint_content(tag, len, tmpint1, tmp_len);
				tmpint1 = ::sprintf(buf, "%lu", (unsigned long)tmpint1) - 1;
				if (tmpint1 > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % tmpint1).str(APPLOCALE));
				} else {
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], buf);
						}
					} else {
						::strcpy(input[iter->field_serial], buf);
					}
				}
				break;
			case ENCODE_ASCII:
				if (len > iter->factor2) {
					throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % len).str(APPLOCALE));
				} else {
					if (!parent_children.empty() &&
						parent_children.top().parent_type == ENCODE_SEQUENCEOF) {
						asnrec.dec_string_content(tag, len, seq_buf, tmp_len);
						if (::strlen(input[iter->field_serial]) == 0) {
							::strcpy(input[iter->field_serial], seq_buf);
						} else {
							if ((::strlen(input[iter->field_serial]) + ::strlen(seq_buf)) > (iter->factor2 - 1))
								throw bad_param(__FILE__, __LINE__, 34, (_("ERROR: field_name {1} factor2 is too small. Increase it to at least {2}") % iter->field_name % (len * 2)).str(APPLOCALE));
							else
								::sprintf(input[iter->field_serial], "%s,%s", input[iter->field_serial], seq_buf);
						}
					} else {
						asnrec.dec_string_content(tag, len, input[iter->field_serial], tmp_len);
					}
				}
				break;
			case ENCODE_ASN:
			case ENCODE_SEQUENCEOF:
				parent_children_t pc;
				pc.clear();
				if (len < 0)
					pc.parent_var = true;
				pc.parent_serial = iter->field_serial;
				pc.parent_type = iter->encode_type;
				pc.parent_start = asnrec.tell();
				pc.header_len = tag_len;
				pc.header_len += len_len;
				pc.parent_len = pc.header_len;
				if (len > 0)
					pc.parent_len += len;
				pc.children_len = pc.header_len;
				if (len == 0) {
					break;
				} else {
					parent_children.push(pc);
					continue;
				}
				break;
			default:
				throw bad_param(__FILE__, __LINE__, 384, (_("ERROR: unknown encode_type {1}") % iter->encode_type).str(APPLOCALE));
			}
		} else {
			asnrec.buf_skip(len);
		}

		while (!parent_children.empty() && parent_children.top().parent_len != 0 &&
			parent_children.top().parent_len == parent_children.top().children_len) {
			parent_children.pop();
		}
	}
}

int sa_iasn1::get_record_serial(const char *record) const
{
	int& input_serial = sa.get_input_serial();
	input_serial = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, (_("record={1}") % record).str(APPLOCALE), &input_serial);
#endif
	char **global = sa.get_global();

	for (int i = 0; i < record_serial_idx.size(); i++) {
		if ((*record_serial_func[i])(NULL, global, &record, 0) == 0) {
			input_serial = i;
			return input_serial;
		}
	}

	return input_serial;
}

void sa_iasn1::post_open()
{
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	delete asnfile_ptr;
	asnfile_ptr = new asn1(is.get(), max_block_len);
	body_tag = -1;
	block_tag = -1;
	record_tag = -1;
	body_len = 0;
	body_tag_len = 0;
	body_len_len = 0;
	body_start = 0;
	block_len = 0;
	block_start = 0;
	record_len = 0;
	buffer_len = 0;
	has_file_tag = (file_root_tag != -1);
}

DECLARE_SA_INPUT(0, sa_iasn1)

}
}

