include_directories(${CMAKE_CURRENT_SOURCE_DIR}/)

file(GLOB UNIT_TEST_MVCC_SRCS "./*.cc")

add_library(UNIT_TEST_MVCC_OBJS OBJECT ${UNIT_TEST_MVCC_SRCS})

add_dependencies(UNIT_TEST_MVCC_OBJS ${DEPEND_LIBS})