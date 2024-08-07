LIBRARY()

SRCS(
    kqp_workload_service.cpp
)

PEERDIR(
    ydb/core/cms/console

    ydb/core/kqp/workload_service/actors
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    tables
)

RECURSE_FOR_TESTS(
    ut
)
