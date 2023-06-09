cmake_minimum_required(VERSION 3.18)
project(wsrp)

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(Seastar_CXX_DIALECT "gnu++20" CACHE STRING "")
add_subdirectory(seastar)

add_executable (wsrp
	main.cc)
target_link_libraries (wsrp
	PRIVATE Seastar::seastar)
