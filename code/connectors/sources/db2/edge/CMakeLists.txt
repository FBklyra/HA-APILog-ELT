# Db2 LUW source plugin (aleltsrc_db2.dll). Built from edge-agent/core/CMakeLists.txt.
# SPDX-License-Identifier: Apache-2.0
if(NOT TARGET aleltcommon)
  message(FATAL_ERROR "Build this from edge-agent/core (it needs the aleltcommon target)")
endif()

if(NOT EXISTS "${DB2_HOME}/include/db2ApiDf.h" OR NOT EXISTS "${DB2_HOME}/lib/db2api.lib")
  message(FATAL_ERROR "Db2 client SDK not found under DB2_HOME='${DB2_HOME}' (need include/db2ApiDf.h, lib/db2api.lib, lib/db2cli.lib)")
endif()

add_library(aleltsrc_db2 SHARED
  src/db2_plugin.c
  src/db2_lri.c
  src/db2_sql.c
  src/db2_build.c)
# IBM headers/libraries come from the installed client (never copied into the repo).
target_include_directories(aleltsrc_db2 PRIVATE src SYSTEM "${DB2_HOME}/include")
target_link_libraries(aleltsrc_db2 PRIVATE aleltcommon "${DB2_HOME}/lib/db2api.lib" "${DB2_HOME}/lib/db2cli.lib")
alelt_harden(aleltsrc_db2)
set_target_properties(aleltsrc_db2 PROPERTIES PREFIX "")

if(ALELT_BUILD_TESTS)
  add_executable(db2tests
    tests/test_db2.c
    src/db2_lri.c
    src/db2_sql.c
    src/db2_build.c)
  target_include_directories(db2tests PRIVATE src)
  target_compile_definitions(db2tests PRIVATE "ALELT_DB2_SRC_DIR=\"${CMAKE_CURRENT_SOURCE_DIR}/src\"")
  target_link_libraries(db2tests PRIVATE aleltcommon alelttestfw)
  alelt_harden(db2tests)
  add_test(NAME db2_plugin_unit COMMAND db2tests)
endif()
