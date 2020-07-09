cmake_minimum_required (VERSION 3.1)


set(CMAKE_C_FLAGS "-O2 -g -Wall -Werror -Wsign-compare -fno-strict-aliasing -fPIC")
set(CMAKE_CXX_FLAGS "-std=c++11 -O2 -g -Wall -Werror -Wsign-compare -fno-strict-aliasing -fPIC")

AUX_SOURCE_DIRECTORY(. CURRENT_DIR_SRCS)                                        
ADD_LIBRARY(tubemq STATIC ${CURRENT_DIR_SRCS})   
TARGET_LINK_LIBRARIES (tubemq)



