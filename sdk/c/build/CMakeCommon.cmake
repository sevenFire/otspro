#####################################################
CMAKE_POLICY(PUSH)
IF(POLICY  CMP0015)
	cmake_policy(SET CMP0015 OLD)
ENDIF(POLICY  CMP0015)

IF(POLICY  CMP0011)
	cmake_policy(SET CMP0011 NEW)
ENDIF(POLICY  CMP0011)

IF(${CMAKE_SYSTEM_NAME} MATCHES Windows)
 if( CMAKE_SIZEOF_VOID_P EQUAL 8 )
  ADD_DEFINITIONS(-DWIN64)
 endif( CMAKE_SIZEOF_VOID_P EQUAL 8 )
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES Windows)


#ADD_DEFINITIONS(-D_CRT_SECURE_NO_DEPRECATE)
ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
ADD_DEFINITIONS(-DQT_NO_QT_INCLUDE_WARN)
#ADD_DEFINITIONS(-D_UNICODE)

IF(UNIX)
	IF(${CMAKE_SYSTEM_NAME} MATCHES SunOS)
		SET(CMAKE_CXX_FLAGS '-pthreads')
		ADD_DEFINITIONS(-DSUNOS)
	ENDIF(${CMAKE_SYSTEM_NAME} MATCHES SunOS)

	IF(${CMAKE_SYSTEM_NAME} MATCHES HP-UX)
		SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} +DD64 -g -mt -D_INCLUDE_LONGLONG -lpthread")
		SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} +DD64 -g -mt -AC99 -D_INCLUDE_LONGLONG -lpthread")
	ENDIF(${CMAKE_SYSTEM_NAME} MATCHES HP-UX)
ENDIF(UNIX)


if(MSVC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4996")
endif()

SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO   "/Od")
MESSAGE("[proj,target]****" ${PROJECT_NAME} ,${TARGET_NAME})

IF(${CMAKE_SYSTEM_NAME} MATCHES Windows)
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4098 /wd4099 /wd4273 /wd4535 /wd4217")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4098 /wd4099 /wd4273 /wd4535 ")
    SET(LINK_LIBS ${LINK_LIBS} Ws2_32 iphlpapi)
#not setting means no
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES Windows)

#Setting Executable and Library Output Dir
SET(CMAKE_CURRENT_BINARY_DIR $ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient)
SET(EXE_DIR bin)
SET(LIB_DIR lib)
SET(INC_DIR include)

#####################################################
## Set Executable Path For Unit Tests and samples. ##
#####################################################

SET(UNITTEST_DIR unittest)
SET(SAMPLE_DIR samples)

#####################################################
## USE THIS COMMAND TO TURN DEBUG INFO(-g) ON:     ##
##      cmake -DCOMPILE_WITH_DEBUG_INFO:BOOL=ON .  ##
#####################################################
OPTION(COMPILE_WITH_DEBUG_INFO "Compile with Debuging info" ON)
IF(COMPILE_WITH_DEBUG_INFO)
	#MESSAGE("Compile with Debuging info")
	SET(CMAKE_BUILD_TYPE "Release")
ENDIF(COMPILE_WITH_DEBUG_INFO)


FIND_PATH(PATH_TO_SRC_ROOT CMakeCommon.cmake ./ )
#Setting Default Include Directorys
IF($(CMAKE_BUILD_TYPE) MATCHES Debug)
	LINK_DIRECTORIES($ENV{VOBBASE_Home}/xInsight/Setup/master/c/otsclient/lib/Debug)
ELSE($(CMAKE_BUILD_TYPE) MATCHES Debug)
ENDIF($(CMAKE_BUILD_TYPE) MATCHES Debug)

FIND_FILE(PATH_TO_CMAKE_LISTS ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt .)
#MESSAGE("Current Source Dir:" ${CMAKE_CURRENT_SOURCE_DIR})

CMAKE_POLICY(POP)
