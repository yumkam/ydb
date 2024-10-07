#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <vector>
#include <set>
#include <random>
#include <iostream>
#include <simdjson.h>

#include <util/system/fs.h>
#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>
#include <util/stream/file.h>

using namespace std::chrono_literals;

static volatile bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {
    uint64_t rdtsc() {
        uint32_t a, d;
        asm volatile("lfence;rdtsc":"=a"(a),"=d"(d));
        return a|(((uint64_t)d)<<32);
    }
}

Y_UNIT_TEST_SUITE(TestSimdjson) {

Y_UNIT_TEST(TestSimdjsonQuickstart) {
    using namespace simdjson;
    CTEST << "simdjson v" << SIMDJSON_VERSION << Endl;
    CTEST << "Detected the best implementation for your machine: " << simdjson::get_active_implementation()->name();
    CTEST << "(" << simdjson::get_active_implementation()->description() << ")" << Endl;
    {
        ondemand::parser parser;
        padded_string json = padded_string::load("twitter.json");
        ondemand::document tweets = parser.iterate(json);
        CTEST << uint64_t(tweets["search_metadata"]["count"]) << " results." << Endl;
    }
    {
        TFileInput fi("out.json");
        uint64_t tsum = 0;
        size_t klen = 0;
        size_t vlen = 0;
        ondemand::parser parser;
        char buffer[1*1024*1024];
        std::string res;
        std::string msgs;
        size_t total_size = 0;
        for (;;) {
            auto size = fi.Read(buffer, sizeof(buffer));
            if (size <= 0) break;
            total_size += size;
            auto eol = (char *)memrchr(buffer, '\n', size);
            if (eol == nullptr) {
                res.append(buffer, buffer + size);
                continue;
            }
            msgs = std::move(res);
            msgs.append(buffer, eol);
            res.assign(eol + 1, buffer + size);
            auto t0 = rdtsc();
            //CTEST << std::string_view(msgs) << Endl;
            simdjson::ondemand::document_stream docs = parser.iterate_many(msgs);
            for (auto && doc : docs) {
                for (auto item: doc.get_object()) {
                    //CTEST << "Key" << Endl << item.escaped_key().value() << Endl;
                    klen += item.escaped_key().value().size();
                    if (item.value().is_string()) {
                        //CTEST << "IsString" << Endl;
                        vlen += item.value().get_string().value().size();
                    } else {
                        //CTEST << "!IsString" << Endl;
                        int64_t v = item.value().get_int64();
                        if (!v)
                            vlen++;
                        else
                            for (; v; v/= 10)
                                ++vlen;
                    }
                }
            }
            auto t1 = rdtsc();
            tsum += t1 - t0;
        }
        CTEST << tsum << ' ' << total_size << ' ' << klen << ' ' << vlen << Endl;
    }
}
}
