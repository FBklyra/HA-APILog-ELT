# ALELT edge agent (aleltagent.exe) + source plugins. SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.21)
project(alelt_agent VERSION 0.1.0 LANGUAGES C RC)

if(NOT WIN32)
  message(FATAL_ERROR "The ALELT agent currently targets Windows x64 only")
endif()

option(ALELT_BUILD_DB2_PLUGIN "Build the Db2 source plugin (needs a Db2 client SDK)" ON)
option(ALELT_BUILD_TESTS "Build unit tests" ON)
option(ALELT_FUZZ "Build libFuzzer targets (clang-cl only)" OFF)

# Db2 client SDK: headers/libs are used from the installed client, never vendored.
if(DEFINED ENV{DB2PATH} AND NOT DB2_HOME)
  file(TO_CMAKE_PATH "$ENV{DB2PATH}" _db2)
  set(DB2_HOME "${_db2}" CACHE PATH "Db2 client install (contains include/ and lib/)")
endif()
set(DB2_HOME "D:/IBM/SQLLIB" CACHE PATH "Db2 client install (contains include/ and lib/)")

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

set(ALELT_IS_CLANG_CL OFF)
if(CMAKE_C_COMPILER_ID STREQUAL "Clang")
  set(ALELT_IS_CLANG_CL ON)
endif()

# Hardening + warnings-as-errors for every target in this tree.
function(alelt_harden tgt)
  target_compile_definitions(${tgt} PRIVATE _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN UNICODE _UNICODE)
  target_compile_options(${tgt} PRIVATE /W4 /WX /guard:cf)
  if(NOT ALELT_IS_CLANG_CL)
    target_compile_options(${tgt} PRIVATE /sdl)
  endif()
  get_target_property(_type ${tgt} TYPE)
  if(NOT _type STREQUAL "STATIC_LIBRARY")
    target_link_options(${tgt} PRIVATE /guard:cf /DYNAMICBASE /NXCOMPAT /CETCOMPAT)
  endif()
endfunction()

# ---------------------------------------------------------------- common library (also linked by plugins)
add_library(aleltcommon STATIC
  src/common/alelt_buf.c
  src/common/alelt_json.c
  src/common/alelt_util.c
  src/common/alelt_sha256.c
  src/common/alelt_zlib.c)
target_include_directories(aleltcommon PUBLIC include)
target_link_libraries(aleltcommon PUBLIC bcrypt)
alelt_harden(aleltcommon)

# ---------------------------------------------------------------- Event Log message table
find_program(ALELT_MC NAMES mc.exe mc HINTS "${CMAKE_RC_COMPILER}/.." REQUIRED)
set(_msg_out "${CMAKE_CURRENT_BINARY_DIR}/msg")
add_custom_command(
  OUTPUT "${_msg_out}/aleltagent_msg.rc" "${_msg_out}/aleltagent_msg.h"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_msg_out}"
  COMMAND "${ALELT_MC}" -h "${_msg_out}" -r "${_msg_out}" "${CMAKE_CURRENT_SOURCE_DIR}/res/aleltagent_msg.mc"
  DEPENDS res/aleltagent_msg.mc
  COMMENT "mc aleltagent_msg.mc"
  VERBATIM)
add_custom_target(aleltagent_msg DEPENDS "${_msg_out}/aleltagent_msg.rc")

# ---------------------------------------------------------------- agent core (testable part)
add_library(aleltagentcore STATIC
  src/agent/log.c
  src/agent/config.c
  src/agent/jobobj.c
  src/agent/command.c
  src/agent/spool.c
  src/agent/http.c
  src/agent/devtoken.c
  src/agent/hub.c
  src/agent/governor.c
  src/agent/ha.c
  src/agent/guard.c
  src/agent/agent.c
  src/agent/service.c)
target_include_directories(aleltagentcore PUBLIC src/agent)
target_link_libraries(aleltagentcore PUBLIC aleltcommon winhttp crypt32 advapi32 shell32 ole32 ws2_32)
alelt_harden(aleltagentcore)

add_executable(aleltagent src/agent/main.c res/aleltagent.rc)
add_dependencies(aleltagent aleltagent_msg)
target_include_directories(aleltagent PRIVATE "${_msg_out}")
target_link_libraries(aleltagent PRIVATE aleltagentcore)
alelt_harden(aleltagent)

if(ALELT_BUILD_TESTS)
  enable_testing()
  add_library(alelttestfw STATIC tests/alelttest.c)
  target_include_directories(alelttestfw PUBLIC tests)
  alelt_harden(alelttestfw)
endif()

# ---------------------------------------------------------------- source plugins
if(ALELT_BUILD_DB2_PLUGIN)
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../../connectors/sources/db2/edge" db2edge)
endif()

# ---------------------------------------------------------------- tests
if(ALELT_BUILD_TESTS)
  add_executable(alelttests
    tests/test_main.c
    tests/test_json.c
    tests/test_cmd.c
    tests/test_hash.c
    tests/test_zlib.c
    tests/test_spool.c
    tests/test_governor.c
    tests/test_config.c
    tests/test_guard.c
    tests/test_ha.c
    tests/test_cred.c)
  target_link_libraries(alelttests PRIVATE aleltagentcore alelttestfw)
  alelt_harden(alelttests)
  add_test(NAME aleltagent_unit COMMAND alelttests)

  # Mock source plugin (test harness only, never packaged): synthetic log + injectable faults
  add_library(aleltsrc_mock SHARED tests/mock_plugin/aleltsrc_mock.c)
  target_link_libraries(aleltsrc_mock PRIVATE aleltcommon)
  alelt_harden(aleltsrc_mock)
  # kept out of bin/ (release scripts package bin/*.dll); tests copy aleltagent.exe next to it
  set_target_properties(aleltsrc_mock PROPERTIES PREFIX "" RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testbin")

  find_package(Python3 COMPONENTS Interpreter QUIET)
  if(Python3_FOUND)
    add_test(NAME zlib_cross_check
             COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/tests/verify_zlib.py" $<TARGET_FILE:aleltzlibcli>)
    # agent + mock source plugin vs mock_hub (AHS-06/09/12/14/15/23, PROBE while the source is down) and two
    # agents active/passive vs the real hub (XC-26). Exit 2 = prerequisites missing -> skipped.
    add_test(NAME e2e_mock_plugin
             COMMAND Python3::Interpreter "${CMAKE_CURRENT_SOURCE_DIR}/tests/e2e_mock_plugin.py" --build "${CMAKE_BINARY_DIR}")
    add_test(NAME e2e_ha_two_agents
             COMMAND Python3::Interpreter "${CMAKE_CURRENT_SOURCE_DIR}/tests/e2e_ha_two_agents.py" --build "${CMAKE_BINARY_DIR}")
    set_tests_properties(e2e_mock_plugin e2e_ha_two_agents PROPERTIES SKIP_RETURN_CODE 2 TIMEOUT 600 RUN_SERIAL TRUE)
    add_executable(aleltzlibcli tests/tools/aleltzlibcli.c)
    target_link_libraries(aleltzlibcli PRIVATE aleltcommon)
    alelt_harden(aleltzlibcli)
  endif()
endif()

if(ALELT_FUZZ)
  if(NOT ALELT_IS_CLANG_CL)
    message(FATAL_ERROR "ALELT_FUZZ requires clang-cl (use the 'fuzz-clang' preset)")
  endif()
  foreach(f fuzz_cmd fuzz_json)
    add_executable(${f} tests/fuzz/${f}.c)
    target_compile_options(${f} PRIVATE -fsanitize=fuzzer,address)
    target_link_options(${f} PRIVATE -fsanitize=fuzzer,address)
    target_link_libraries(${f} PRIVATE aleltagentcore)
  endforeach()
endif()
