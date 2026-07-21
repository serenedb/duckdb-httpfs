set(HTTPFS_SOURCES
    hffs.cpp
    s3fs.cpp
    httpfs.cpp
    http_state.cpp
    hash_functions.cpp
    create_secret_functions.cpp
    httpfs_extension.cpp
    s3_multi_part_upload.cpp)
if(NOT EMSCRIPTEN)
  set(HTTPFS_SOURCES ${HTTPFS_SOURCES} crypto.cpp
                     httpfs_connection_caching.cpp httpfs_curl_client.cpp)
endif()

add_library(httpfs_library OBJECT ${HTTPFS_SOURCES})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:httpfs_library>
    PARENT_SCOPE)
