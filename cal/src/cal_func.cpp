#include <iostream>
#include "cal_func.h"

using namespace std;


extern "C"
{


int month_between(const char* date1, const char* date2)
{
	char buf[32];
	if (strlen(date1)<6 || strlen(date2)<6)
		return 0;
	memcpy(buf, date1, 4);
	buf[4] = '\0';
	int year1 = atoi(buf);
	memcpy(buf, date1 + 4, 2);
	buf[2] = '\0';
	int month1 = atoi(buf);
	memcpy(buf, date2, 4);
	buf[4] = '\0';
	int year2 = atoi(buf);
	memcpy(buf, date2 + 4, 2);
	buf[2] = '\0';
	int month2 = atoi(buf);	
	return (year1 * 12 + month1) - (year2 * 12 + month2);
}


}


