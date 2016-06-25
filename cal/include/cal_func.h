

#if !defined(__CAL_FUNC_H__)
#define __CAL_FUNC_H__

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <values.h>
#include <assert.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>

/*
    The returning value of all functions returning int is defined below:
    On success: return 0;
    On error: return error_code, which is combined of error_type('03') + error_no(table_id, 4).
*/

#if defined(__cplusplus)

extern "C"
{

#endif


int month_between(const char* date1, const char* date2);


#if defined(__cplusplus)

}

#endif



#endif

