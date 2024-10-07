UNITTEST_FOR(contrib/libs/simdjson)

SRCS(
    simdjson_ut.cpp
)

CFLAGS(
    -mprfchw
# -mavx2
# -mavx512f
)

DATA(
    arcadia_tests_data/contrib/libs/simdjson/ut/twitter.json
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
