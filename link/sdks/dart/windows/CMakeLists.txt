cmake_minimum_required(VERSION 3.18)
project(kalam_link_library VERSION 0.1.3 LANGUAGES C)

# Pre-built shared library to be bundled into the Flutter Windows app.
# The .dll is compiled by build.sh and committed.
set(kalam_link_dart_bundled_libraries
    "${CMAKE_CURRENT_SOURCE_DIR}/lib/kalam_link_dart.dll"
    PARENT_SCOPE
)
