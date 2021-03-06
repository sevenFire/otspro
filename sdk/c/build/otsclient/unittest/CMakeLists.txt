# Set the minimum required version of cmake for the project
CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

###########################  For modify begin #################################
# Set a name for the entire project
PROJECT (otsclient_ut)

INCLUDE($ENV{VOBBASE_Home}/xInsight/Source/ots/sdk/c/build/CMakeCommon.cmake)
SET(TARGET_NAME otsclient_ut)
SET(TARGET_TYPE "exe")	# exe or lib
#SET(LIB_TYPE SHARED)	# STATIC or SHARED

#Setting Source Files
SET(SRCDIR $ENV{VOBBASE_Home}/xInsight/Source/ots/sdk/c/otsclient)
SET(SRCS ${SRCS}
	${SRCDIR}/unittest/otsclient_ut.c
	${SRCDIR}/unittest/main.c
	${SRCDIR}/unittest/http_curl_stub.cpp
	${SRCDIR}/src/base64.cpp
	${SRCDIR}/src/cJSON.c
	${SRCDIR}/src/client.cpp
	${SRCDIR}/src/common.cpp
	${SRCDIR}/src/iterator.cpp
	${SRCDIR}/src/record.cpp
	)

# Add include directories to the build
INCLUDE_DIRECTORIES(${SRCDIR}/include ${SRCDIR}/src $ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient/include/cunit $ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient/include/curl)

# Specify directories in which the linker will look for libraries
LINK_DIRECTORIES($ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient/lib $ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient/lib/cunit)

SET(LINK_LIBS CUnit)

SET(TARGET_DIR $ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient)
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TARGET_DIR}/bin)
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${TARGET_DIR}/lib)
SET(CMAKE_LIBRARY_OUPUT_DIRECTORY ${TARGET_DIR}/lib)

###########################  For modify end ###################################
IF(${CMAKE_SYSTEM_NAME} MATCHES Windows)
	ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
	SET(LINK_LIBS ${LINK_LIBS} Ws2_32)
	IF(MSVC)
    	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4996 /wd4273")
	ENDIF(MSVC)
ELSE(${CMAKE_SYSTEM_NAME} MATCHES Windows)

ENDIF(${CMAKE_SYSTEM_NAME} MATCHES Windows)

INCLUDE($ENV{VOBBASE_Home}/xInsight/Source/ots/sdk/c/build/CMakeCommonEXE.cmake)

