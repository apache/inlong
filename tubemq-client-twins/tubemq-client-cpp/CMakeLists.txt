cmake_minimum_required (VERSION 3.1)

project (TubeMQ)


INCLUDE_DIRECTORIES(include)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/log4cplus/include)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/third_party/log4cplus/include)
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/third_party/log4cplus/lib)

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(third_party)
ADD_SUBDIRECTORY(example)


if (UNIX)
    SET(CMAKE_CXX_FLAGS_DEBUG   "-O1 -g -ggdb -D_DEBUG")
    SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -ggdb -DNDEBUG")
    SET(DEPENDENT_LIBRARIES log4cplus pthread)
else (UNIX)
    SET(DEPENDENT_LIBRARIES log4cplus)
endif (UNIX)






