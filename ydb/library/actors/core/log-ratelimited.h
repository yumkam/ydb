#pragma once
#include <ydb/library/actors/core/log.h>
namespace NActors {

struct TLogRateLimiter {
    TInstant LastReported;
    ui64 Skipped {};
};

}

#define LOG_LOG_S_RATELIMITED(actorCtxOrSystem, priority, component, sampleBy, stream, rateLimit, limit) \
    do {                                                                                                 \
        if (IS_CTX_LOG_PRIORITY_ENABLED(actorCtxOrSystem, priority, component, sampleBy)) {              \
            auto now = TInstant::Now();                                                                  \
            if (now - rateLimit.LastReported < limit) {                                                  \
                ++rateLimit.Skipped;                                                                     \
            } else {                                                                                     \
                TStringBuilder logStringBuilder;                                                         \
                logStringBuilder << stream;                                                              \
                if (rateLimit.Skipped) logStringBuilder << " (skipped " << rateLimit.Skipped << " similar events)";     \
                ::NActors::MemLogAdapter(                                                                \
                    actorCtxOrSystem, priority, component, std::move(logStringBuilder));                 \
                rateLimit.LastReported = now;                                                            \
                rateLimit.Skipped = 0;                                                                   \
            }                                                                                            \
        }                                                                                                \
    } while (0) /**/


#define LOG_DEBUG_S_RATELIMITED(actorCtxOrSystem, component, stream, rateLimit, limit)  \
    LOG_LOG_S_RATELIMITED(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, 0ull, stream, rateLimit, limit)
#define LOG_TRACE_S_RATELIMITED(actorCtxOrSystem, component, stream, rateLimit, limit)  \
    LOG_LOG_S_RATELIMITED(actorCtxOrSystem, NActors::NLog::PRI_TRACE, component, 0ull, stream, rateLimit, limit)
