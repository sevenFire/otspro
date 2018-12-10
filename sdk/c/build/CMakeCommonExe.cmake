#If Qt project, set compile items
#IF (CMAKE_SYSTEM_NAME MATCHES Windows)
#	SET(CMAKE_CXX_FLAGS_DEBUG "/MDd /D_DEBUG /Zi /Ob0 /RTC1")
#	SET(CMAKE_CXX_FLAGS_RELEASE "/MD /O2 /Ob2 /Zi /D NDEBUG /EHa")
#ENDIF (CMAKE_SYSTEM_NAME MATCHES Windows)

#Setting Executable and Library Output Path
SET(EXECUTABLE_OUTPUT_PATH ${EXE_DIR})
SET(LIBRARY_OUTPUT_PATH ${EXE_DIR})

#Build Executables
ADD_EXECUTABLE(${TARGET_NAME} ${SRCS})
IF(UNIX)
ELSE(UNIX)
	#SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES LINK_FLAGS "/MANIFESTUAC:\"level='requireAdministrator' uiAccess='false'\"") 
	SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES LINK_FLAGS "/ignore:4049 /ignore:4217" ) 
ENDIF(UNIX)


IF (CMAKE_SYSTEM_NAME MATCHES Windows)
	SET(CMAKE_C_FLAGS_RELEASE     "/Zi /MD")
	SET(CMAKE_CXX_FLAGS_RELEASE   "${CMAKE_CXX_FLAGS_RELEASE} /Zi /MD")
ENDIF (CMAKE_SYSTEM_NAME MATCHES Windows)

#Setting Librarys needed in linking 
TARGET_LINK_LIBRARIES(${TARGET_NAME} ${LINK_LIBS})
SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${LIB_DIR})

IF(TARGET_VERSION)
	SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES VERSION ${TARGET_VERSION})
ENDIF(TARGET_VERSION)
