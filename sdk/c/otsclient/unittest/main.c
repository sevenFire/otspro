#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "Automated.h"
#include "Basic.h"
#include "time.h"
//#include <windows.h>  

/** @brief Test cases for all functions	*/
extern CU_TestInfo case_ots_client[];

CU_SuiteInfo suites[] = {
    {"Testing case_ots_client:", NULL, NULL, case_ots_client},
	CU_SUITE_INFO_NULL
};

/**
 * @brief	main function
 * @param	[in] argc parameter number
 * @param	[in] argv[] input parameters. If the parameters are null, the result
 will be output to console. If the parameter is the name of a xml, the result 
 will be output to a xml file.

 * @version	2014/03/31  Initial version.
*/
int main(int argc, char* argv[])
{
	CU_BasicRunMode mode = CU_BRM_VERBOSE;
	CU_ErrorAction error_action = CUEA_IGNORE;
	CU_pSuite pSuite;

	srand( (unsigned int)time(NULL));
	// initialize registry
	if (CUE_SUCCESS != CU_initialize_registry()) 
	{
		printf("\nInitialization of Test Registry failed: %s", 
			CU_get_error_msg());
		return CU_get_error();
	}

	// add suite
	pSuite = CU_add_suite("CUnit test suite", NULL, NULL);
	if (NULL == pSuite)
	{
		CU_cleanup_registry();
		return CU_get_error();
	}
	if(CUE_SUCCESS != CU_register_suites(suites)) {
		fprintf(stderr, "Register suites failed - %s ", CU_get_error_msg());
	}

	if (argc > 1)	// output to xml file
	{
		CU_set_output_filename(argv[1]);
		CU_list_tests_to_file();
		CU_automated_run_tests();
	}
	else			// output in console
	{
		CU_basic_set_mode(mode);
		CU_set_error_action(error_action);
		printf("\nTests completed with return value %d.\n", 
			CU_basic_run_tests());
	}

	// clean up registry
	CU_cleanup_registry();
	return 0;
}
