#include "kikimr_lookup_actor.h"
//#include "yql_generic_base_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <ydb/core/protos/kqp_lookup_source.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <util/string/escape.h>
#include <ydb/core/util/backoff.h>

using namespace NKikimr;

namespace {
template <typename T>
T ExtractFromConstFuture(const NThreading::TFuture<T>& f) {
    // We want to avoid making a copy of data stored in a future.
    // But there is no direct way to extract data from a const future
    // So, we make a copy of the future, that is cheap. Then, extract the value from this copy.
    // It destructs the value in the original future, but this trick is legal and documented here:
    // https://docs.yandex-team.ru/arcadia-cpp/cookbook/concurrency
    return NThreading::TFuture<T>(f).ExtractValueSync();
}
template <class TProto>
NYql::TIssues IssuesFromProtoMessage(const TProto& message) {
    NYql::TIssues issues;
    IssuesFromMessage(message.issues(), issues);
    return issues;
}

} // namespace {

namespace NYql::NDq {

    using namespace NActors;

    template <typename TDerived, typename TEvState = std::monostate>
    class TKikimrBaseActor: public NActors::TActorBootstrapped<TDerived> {
    protected: // Events
        // Event ids
        enum EEventIds: ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvYdbExecuteDataQueryResponse = EvBegin,
            EvYdbCreateSessionResponse,
            EvYdbDeleteSessionResponse,
            EvError,
            EvRetry,
            EvEnd
        };

        static_assert(EEventIds::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        // Beware: destroys future value
        template <typename TResponse, typename TResult, enum EEventIds EvId>
        struct TEvYdbResponse: NActors::TEventLocal<TEvYdbResponse<TResponse, TResult, EvId>, EvId> {
            explicit TEvYdbResponse(const NThreading::TFuture<TResponse>& responseFuture, TEvState state)
                : State(std::move(state))
            {
                try {
                    auto response = ExtractFromConstFuture(responseFuture);
                    //Cerr << "Response: {{{ " << response.DebugString() << "\n}}}" << Endl;
                    Status = response.operation().status();
                    Issues = IssuesFromProtoMessage(response.operation());
                    response.operation().result().UnpackTo(&Result);
                } catch(std::exception& ex) {
                    Status = Ydb::StatusIds::INTERNAL_ERROR;
                    Issues.AddIssue(TIssue(TStringBuilder() << "Got unexpected exception: " << ex.what()));
                }
            }

            TEvState State;
            TResult Result;
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };

        using TEvYdbExecuteDataQueryResponse = TEvYdbResponse<Ydb::Table::ExecuteDataQueryResponse, Ydb::Table::ExecuteQueryResult, EvYdbExecuteDataQueryResponse>;
        using TEvYdbCreateSessionResponse = TEvYdbResponse<Ydb::Table::CreateSessionResponse, Ydb::Table::CreateSessionResult, EvYdbCreateSessionResponse>;
        //using TEvYdbDeleteSessionResponse = TEvYdbResponse<Ydb::Table::DeleteSessionResponse, Ydb::Table::DeleteSessionResult, EvYdbDeleteSessionResponse>;

    protected: // TODO move common logic here
    };
    namespace {
        constexpr ui32 RetriesLimit = 20; // TODO lookup parameters or PRAGMA?
        constexpr TDuration MinRetryDelay = TDuration::MilliSeconds(10);
        constexpr TDuration RetriesTimeout = TDuration::Minutes(3); // TODO lookup parameters or PRAGMA?

        const NKikimr::NMiniKQL::TStructType* MergeStructTypes(const NKikimr::NMiniKQL::TTypeEnvironment& env, const NKikimr::NMiniKQL::TStructType* t1, const NKikimr::NMiniKQL::TStructType* t2) {
            Y_ABORT_UNLESS(t1);
            Y_ABORT_UNLESS(t2);
            NKikimr::NMiniKQL::TStructTypeBuilder resultTypeBuilder{env};
            for (ui32 i = 0; i != t1->GetMembersCount(); ++i) {
                resultTypeBuilder.Add(t1->GetMemberName(i), t1->GetMemberType(i));
            }
            for (ui32 i = 0; i != t2->GetMembersCount(); ++i) {
                resultTypeBuilder.Add(t2->GetMemberName(i), t2->GetMemberType(i));
            }
            return resultTypeBuilder.Build();
        }

        struct TLookupState {
            using TPtr = std::shared_ptr<TLookupState>;
            std::weak_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> Request;
            // ^^^ must not be lock()ed without bound mkql allocator (e.g. in future
            // handlers)
            // ^^^ TODO: consider possible (temporal) circular ownership via Future and lambda capture
            TBackoff Backoff;
            TInstant SentTime;
            size_t FullscanLimit = 0;
            size_t ResultRows = 0;
            TString SessionId;
        };
    } // namespace

    class TKikimrLookupActor
        : public NYql::NDq::IDqAsyncLookupSource,
          public TKikimrBaseActor<TKikimrLookupActor, TLookupState::TPtr> {
        using TBase = TKikimrBaseActor<TKikimrLookupActor, TLookupState::TPtr>;
        typedef TLookupState TEvState;

        struct TEvLookupRetry : NActors::TEventLocal<TEvLookupRetry, EvRetry> {
            explicit TEvLookupRetry(TLookupState::TPtr state)
                : State(std::move(state))
            {
            }

            TLookupState::TPtr State;
        };

    public:
        TKikimrLookupActor(
            NActors::TActorId&& parentId,
            ::NMonitoring::TDynamicCounterPtr taskCounters,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
            NKqpProto::TKikimrLookupSource&& lookupSource,
            const NKikimr::NMiniKQL::TStructType* keyType,
            const NKikimr::NMiniKQL::TStructType* payloadType,
            const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory,
            const size_t maxKeysInRequest,
            bool isMultiMatches = false)
            : ParentId(std::move(parentId))
            , Alloc(alloc)
            , KeyTypeHelper(keyTypeHelper)
            , LookupSource(std::move(lookupSource))
            , KeyType(keyType)
            , PayloadType(payloadType)
            , SelectResultType(MergeStructTypes(typeEnv, keyType, payloadType))
            , HolderFactory(holderFactory)
            , ColumnDestinations(CreateColumnDestination())
            , MaxKeysInRequest(std::min(maxKeysInRequest, size_t{100}))
            , IsMultiMatches(isMultiMatches)
        {
            InitMonCounters(taskCounters);
            {
                TStringBuilder out;
                MakeSelectWithKeys(out);
                SelectWithKeys = std::move(out);
            }
        }

        ~TKikimrLookupActor() {
            Free();
        }

    private:
        void Free() {
            auto guard = Guard(*Alloc);
            if (InFlight) {
                // If request fails on (unrecoverable) error or cancelled, we may end up with non-zero InFlight
                InFlight->Sub(LocalInFlight);
            }
            LocalInFlight = 0;
            KeyTypeHelper.reset();
        }
        void InitMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
            if (!taskCounters) {
                return;
            }
            auto component = taskCounters->GetSubgroup("component", "LookupSrc");
            Count = component->GetCounter("Reqs", true);
            Fullscans = component->GetCounter("Fullscans", true);
            Keys = component->GetCounter("Keys", true);
            ResultChunks = component->GetCounter("Chunks", true);
            ResultRows = component->GetCounter("Rows", true);
            ResultBytes = component->GetCounter("Bytes", true);
            AnswerTime = component->GetCounter("AnswerUs", true);
            CpuTime = component->GetCounter("CpuUs", true);
            InFlight = component->GetCounter("InFlight");
        }
    public:

        void Bootstrap() {
            //auto dsi = LookupSource.data_source_instance();
#if 0
            YQL_CLOG(INFO, ProviderGeneric) << "New generic proivider lookup source actor(ActorId=" << SelfId() << ") for"
                                            << " kind=" << NYql::EGenericDataSourceKind_Name(dsi.kind())
                                            << ", endpoint=" << dsi.endpoint().ShortDebugString()
                                            << ", database=" << dsi.database()
                                            << ", use_tls=" << ToString(dsi.use_tls())
                                            << ", protocol=" << NYql::EGenericProtocol_Name(dsi.protocol())
                                            << ", table=" << LookupSource.table();
#endif
            Become(&TKikimrLookupActor::StateFunc);
        }

        static constexpr char ActorName[] = "KIKIMR_PROVIDER_LOOKUP_ACTOR";

    private: // IDqAsyncLookupSource
        size_t GetMaxSupportedKeysInRequest() const override {
            return MaxKeysInRequest;
        }
        size_t GetMaxSupportedFullscanRequest() const override {
            return MaxSupportedFullscanRequest;
        }
        void AsyncLookup(std::weak_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request) override {
            auto guard = Guard(*Alloc);
            CreateRequest(request.lock(), 0);
        }
        void PassAway() override {
            for (auto&& sessionId: SessionIds) {
                SendDeleteSession(std::move(sessionId));
            }
            SessionIds.clear();
            Free();
            TBase::PassAway();
        }

    private: // events
        STRICT_STFUNC_EXC(StateFunc,
            hFunc(TEvLookupRequest, Handle)
            hFunc(TEvYdbExecuteDataQueryResponse, Handle)
            hFunc(TEvYdbCreateSessionResponse, Handle)
            //hFunc(TEvYdbDeleteSessionResponse, Handle)
            //hFunc(TEvReadSplitsFinished, Handle)
            hFunc(TEvLookupRetry, Handle)
            hFunc(NActors::TEvents::TEvPoison, Handle)
            , ExceptionFunc(std::exception, HandleException)
        )

#if 0
        void Handle(TEvListSplitsPart::TPtr ev) {
            auto response = std::move(ev->Get()->Response);
            Y_ENSURE(response.splits_size() == 1, response.splits_size() << " == " << 1);
            auto& split = response.splits(0);
            NConnector::NApi::TReadSplitsRequest readRequest;

            if (error) {
                SendError(TActivationContext::ActorSystem(), SelfId(), std::move(error));
                return;
            }
            Connector->ReadSplits(readRequest, RequestTimeout).Subscribe([
                    actorSystem = TActivationContext::ActorSystem(),
                    selfId = SelfId(),
                    state = std::move(ev->Get()->State)
            ](const NConnector::TReadSplitsStreamIteratorAsyncResult& asyncResult) {
                YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got ReadSplitsStreamIterator from Connector";
                auto result = ExtractFromConstFuture(asyncResult);
                if (result.Status.Ok()) {
                    auto ev = new TEvReadSplitsIterator(std::move(result.Iterator));
                    ev->State = std::move(state);
                    actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                } else {
                    SendRetryOrError(actorSystem, selfId, result.Status, state);
                }
            });
        }

        void Handle(TEvReadSplitsIterator::TPtr ev) {
            ev->Get()->State->ReadSplitsIterator = std::move(ev->Get()->Iterator);
            ReadNextData(std::move(ev->Get()->State));
        }

        void Handle(TEvReadSplitsPart::TPtr ev) {
            auto state = std::move(ev->Get()->State);
            Y_DEBUG_ABORT_UNLESS(state->ReadSplitsIterator);
            ProcessReceivedData(ev->Get()->Response, state);
            if (state->FullscanLimit > 0 && state->ResultRows == state->FullscanLimit) {
                FinalizeRequest(std::move(state));
                return;
            }
            ReadNextData(std::move(state));
        }

        void Handle(TEvReadSplitsFinished::TPtr ev) {
            FinalizeRequest(std::move(ev->Get()->State));
        }
#endif

        void Handle(TEvLookupRetry::TPtr ev) {
            if (LocalInFlight == 0) { // already passed away
                //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " Retry after PassAway";
                return;
            }
            auto guard = Guard(*Alloc);
            auto state = std::move(ev->Get()->State);
            if (state->FullscanLimit > 0) {
                if (auto request = state->Request.lock()) {
                    request->erase(request->begin(), request->end());
                } else {
                    //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " Retry: parent MIA";
                }
            } else if (IsMultiMatches) {
                if (auto request = state->Request.lock()) {
                    for (auto& [_, value]: *request) {
                        value = NUdf::TUnboxedValue();
                    }
                } else {
                    //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " Retry: parent MIA";
                }
            }
            state->ResultRows = 0;
            SendRequest(std::move(state));
        }

        void Handle(NActors::TEvents::TEvPoison::TPtr) {
            PassAway();
        }

        void Handle(IDqAsyncLookupSource::TEvLookupRequest::TPtr ev) {
            auto guard = Guard(*Alloc);
            CreateRequest(ev->Get()->Request.lock(), ev->Get()->FullscanLimit);
        }

        static bool IsRetryableError(Ydb::StatusIds::StatusCode status) {
            switch(status) {
                case Ydb::StatusIds::ABORTED:
                case Ydb::StatusIds::UNAVAILABLE:
                case Ydb::StatusIds::OVERLOADED:
                case Ydb::StatusIds::TIMEOUT:
                case Ydb::StatusIds::BAD_SESSION:
                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::CANCELLED:
                case Ydb::StatusIds::UNDETERMINED:
                case Ydb::StatusIds::SESSION_BUSY:
                    return true;
                default:
                    return false;
            }
        }

        void SendRetryOrError(TLookupState::TPtr state, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
#if 0
            LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, "DescribeResourceId: SelfId=" << selfId << " GetSession failed"
                    << ", status# " << result.GetStatus()
                    << ", issues# " << result.GetIssues().ToOneLineString()
                    << ", iteration# " << backoff->GetIteration());
#endif
            if (IsRetryableError(status) && state->Backoff.HasMore()) {
                Schedule(state->Backoff.Next(), new TEvLookupRetry(std::move(state)));
                return;
            }
            SendError(status, std::move(issues));
        }

        void HandleException(const std::exception& ex) {
            //Cerr << ex.what() << Endl;
            SendError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << ex.what());
        }

        void SendError(Ydb::StatusIds::StatusCode status, const TString& issue) {
            NYql::TIssues issues;
            issues.AddIssue(TIssue(issue));
            SendError(status, std::move(issues));
        }

        void SendError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
            Send(ParentId, new IDqComputeActorAsyncInput::TEvAsyncInputError(-1, std::move(issues), YdbStatusToDqStatus(status, EStatusCompatibilityLevel::WithUnauthorized)));
        }

    private:
        static TDuration GetCpuTimeDelta(ui64 startCycleCount) {
            return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - startCycleCount));
        }

        void CreateRequest(std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request, size_t fullscanLimit) {
            if (!request) {
                //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " CreateRequest: parent MIA";
                return;
            }
            Y_DEBUG_ABORT_UNLESS(request->empty() == (fullscanLimit > 0));
            //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " Got LookupRequest for " << request->size() << " keys";
            Y_ABORT_IF((request->empty() == (fullscanLimit == 0)) || request->size() > MaxKeysInRequest);
            if (Count) {
                Count->Inc();
                InFlight->Inc();
                Keys->Add(request->size());
                if (fullscanLimit > 0) {
                    Fullscans->Inc();
                }
            }
            ++LocalInFlight;

            auto state = std::make_shared<TLookupState>(TLookupState {
                .Request = request,
                .Backoff = TBackoff(RetriesLimit, MinRetryDelay, RetriesTimeout),
                .SentTime = TInstant::Now(),
                .FullscanLimit = fullscanLimit
            });
            SendRequest(std::move(state));
        }

        // must be called with bound Alloc
        void SendRequest(TLookupState::TPtr state) {
            auto startCycleCount = GetCycleCountFast();

            if (state->SessionId.empty()) { // reuse or create session
                if (SessionIds.empty()) {
                    SendCreateSession(std::move(state));
                    return;
                }
                state->SessionId = std::move(SessionIds.back());
                SessionIds.pop_back();
            }

            using TRequest = Ydb::Table::ExecuteDataQueryRequest;
            using TResponse = Ydb::Table::ExecuteDataQueryResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(FillSelect(state), AppData()->TenantName, /*token=*/Nothing(), actorSystem);
            result.Subscribe([actorSystem, selfId, state = std::move(state)](const NThreading::TFuture<TResponse>& future) mutable {
                actorSystem->Send(selfId, new TEvYdbExecuteDataQueryResponse(future, std::move(state)));
            });
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) {
                CpuTime->Add(cputime);
            }
            Cerr << "SendRequest time " << cputime;
        }

        void Handle(TEvYdbExecuteDataQueryResponse::TPtr ev) {
            auto state = std::move(ev->Get()->State);
            switch(ev->Get()->Status) {
                case Ydb::StatusIds::SUCCESS:
                    break;

                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::BAD_SESSION:
                    state->SessionId.clear();
                    [[fallthrough]];
                default:
                    SendRetryOrError(std::move(state), ev->Get()->Status, ev->Get()->Issues);
                    return;
            }
            ProcessReceivedData(ev->Get()->Result, state);
            FinalizeRequest(state);
        }

        void SendCreateSession(TLookupState::TPtr state) {
            using TRequest = Ydb::Table::CreateSessionRequest;
            using TResponse = Ydb::Table::CreateSessionResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;

            TRequest request;
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData()->TenantName, /*token=*/Nothing(), actorSystem);
            result.Subscribe([actorSystem, selfId, state] (const NThreading::TFuture<TResponse>& future) mutable {
                actorSystem->Send(selfId, new TEvYdbCreateSessionResponse(future, std::move(state)));
            });
        }

        void Handle(TEvYdbCreateSessionResponse::TPtr ev) {
            auto state = std::move(ev->Get()->State);
            Y_ENSURE(state->SessionId.empty());
            if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
                SendRetryOrError(std::move(state), ev->Get()->Status, ev->Get()->Issues);
                return;
            }
            state->SessionId = std::move(*ev->Get()->Result.mutable_session_id());
            auto guard = Guard(*Alloc);
            SendRequest(std::move(state));
        }

        void SendDeleteSession(TString sessionId) {
            using TRequest = Ydb::Table::DeleteSessionRequest;
            using TResponse = Ydb::Table::DeleteSessionResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;

            TRequest request;
            request.set_session_id(std::move(sessionId));
            auto actorSystem = TActivationContext::ActorSystem();
            [[maybe_unused]]
            auto selfId = SelfId();
            [[maybe_unused]]
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData()->TenantName, /*token=*/Nothing(), actorSystem);
#if 0 // don't wait for results
            result.Subscribe([actorSystem, selfId, state = std::move(state)](const NThreading::TFuture<TResponse>& f) mutable {
                actorSystem->Send(selfId, new TEvYdbDeleteSessionResponse(f));
            });
#endif
        }
        
#if 0
        void Handle(TEvYdbDeleteSessionResponse::TPtr /*ev*/) {
        }
#endif

        static NUdf::TUnboxedValue YdbValueToUnboxedValue(NYdb::TValueParser& columnParser, const NKikimr::NMiniKQL::TType *type) {
            NUdf::TUnboxedValue v;
            bool is_optional = type->IsOptional();
            if (is_optional) {
                columnParser.OpenOptional();
                if (columnParser.IsNull()) {
                    columnParser.CloseOptional();
                    return v;
                }
                type = AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType();
            }
            if (type->IsData()) {
                auto dataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot();
                Y_ENSURE(dataSlot);
                using namespace NYql::NUdf;
                switch (*dataSlot) {
                    case NYql::NUdf::EDataSlot::Bool:
                        v = TUnboxedValuePod(columnParser.GetBool());
                        break;
                    case NYql::NUdf::EDataSlot::Int8:
                        v = TUnboxedValuePod(columnParser.GetInt8());
                        break;
                    case NYql::NUdf::EDataSlot::Int16:
                        v = TUnboxedValuePod(columnParser.GetInt16());
                        break;
                    case NYql::NUdf::EDataSlot::Int32:
                        v = TUnboxedValuePod(columnParser.GetInt32());
                        break;
                    case NYql::NUdf::EDataSlot::Int64:
                        v = TUnboxedValuePod(columnParser.GetInt64());
                        break;
                    case NYql::NUdf::EDataSlot::Uint8:
                        v = TUnboxedValuePod(columnParser.GetUint8());
                        break;
                    case NYql::NUdf::EDataSlot::Uint16:
                        v = TUnboxedValuePod(columnParser.GetUint16());
                        break;
                    case NYql::NUdf::EDataSlot::Uint32:
                        v = TUnboxedValuePod(columnParser.GetUint32());
                        break;
                    case NYql::NUdf::EDataSlot::Uint64:
                        v = TUnboxedValuePod(columnParser.GetUint64());
                        break;
                    case NYql::NUdf::EDataSlot::Double:
                        v = TUnboxedValuePod(columnParser.GetDouble());
                        break;
                    case NYql::NUdf::EDataSlot::Float:
                        v = TUnboxedValuePod(columnParser.GetFloat());
                        break;
                    case NYql::NUdf::EDataSlot::String:
                        v = NKikimr::NMiniKQL::ValueFromString(*dataSlot, columnParser.GetString());
                        break;
                    case NYql::NUdf::EDataSlot::Utf8:
                        v = NKikimr::NMiniKQL::ValueFromString(*dataSlot, columnParser.GetUtf8());
                        break;
                    case NYql::NUdf::EDataSlot::Json:
                        v = NKikimr::NMiniKQL::ValueFromString(*dataSlot, columnParser.GetJson());
                        break;
                    case NYql::NUdf::EDataSlot::Timestamp:
                        v = TUnboxedValuePod(columnParser.GetTimestamp().MicroSeconds());
                        break;
                    case NYql::NUdf::EDataSlot::Interval:
                        v = TUnboxedValuePod(columnParser.GetInterval());
                        break;

                    default:
                        throw yexception() << "Unimplemented DataType slot " << *dataSlot;
                        break;
                }
            } else {
                throw yexception() << "Unimplemented type " << type->GetKindAsStr();
            }
            if (is_optional) {
                columnParser.CloseOptional();
            }
            return v;
        }

        void ProcessReceivedData(Ydb::Table::ExecuteQueryResult& result, TLookupState::TPtr state) {
            auto startCycleCount = GetCycleCountFast();
            auto guard = Guard(*Alloc);
            auto request = state->Request.lock();
            if (!request) {
                //YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " ProcessReceivedData: parent MIA";
                return;
            }
#if 0
            NKikimr::NArrow::NSerialization::TSerializerContainer deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer(); // todo move to class' member
            Y_ENSURE(!resp.arrow_ipc_streaming().empty());
            const auto& data = deser->Deserialize(resp.arrow_ipc_streaming());
            Y_ENSURE(data.ok(), data.status().ToString());
            const auto& value = data.ValueOrDie();
            Y_ENSURE(static_cast<ui32>(value->num_columns()) == ColumnDestinations.size(), value->num_columns() << " == " << ColumnDestinations.size());
            std::vector<NKikimr::NMiniKQL::TUnboxedValueVector> columns(ColumnDestinations.size());
            for (size_t i = 0; i != columns.size(); ++i) {
                Y_ENSURE(value->column_name(i) == (ColumnDestinations[i].first == EColumnDestination::Key ? KeyType : PayloadType)->GetMemberName(ColumnDestinations[i].second));
                columns[i] = NArrow::ExtractUnboxedValues(value->column(i), SelectResultType->GetMemberType(i), HolderFactory);
            }

            auto height = columns[0].size();
            Y_DEBUG_ABORT_UNLESS(state->FullscanLimit == 0 || state->FullscanLimit > state->ResultRows);
            if (state->FullscanLimit > 0 && height > state->FullscanLimit - state->ResultRows) {
                YQL_CLOG(WARN, ProviderGeneric) << "ActorId=" << SelfId() << " YQ-5124 Workaround for unimplemented LIMIT invoked " << height << " > " << state->FullscanLimit << " - " << state->ResultRows;
                height = state->FullscanLimit - state->ResultRows;
            }
            state->ResultRows += height;
            for (size_t i = 0; i != height; ++i) {
                NUdf::TUnboxedValue* keyItems;
                NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(KeyType->GetMembersCount(), keyItems);
                NUdf::TUnboxedValue* outputItems;
                NUdf::TUnboxedValue output = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), outputItems);
                for (size_t j = 0; j != columns.size(); ++j) {
                    (ColumnDestinations[j].first == EColumnDestination::Key ? keyItems : outputItems)[ColumnDestinations[j].second] = columns[j][i];
                }

                NUdf::TUnboxedValue *v;
                if (state->FullscanLimit > 0) {
                    auto [it, _] = request->emplace(key, NUdf::TUnboxedValue{});
                    v = &(it->second);
                } else if (auto it = request->find(key); it != request->end()) {
                    v = &(it->second);
                } else {
                    continue;
                }
                if (IsMultiMatches) {
                    *v = HolderFactory.CreateDirectListHolder((*v ? *NKikimr::NMiniKQL::GetDefaultListRepresentation(*v) : NKikimr::NMiniKQL::TDefaultListRepresentation{}).Append(std::move(output)));
                } else {
                    *v = std::move(output); // duplicates will be overwritten
                }
            }
#else
            Y_ENSURE(result.result_setsSize() == 1);
            NYdb::TResultSetParser parser(result.result_sets()[0]);
            ui32 columnsCount = SelectResultType->GetMembersCount();
            TVector<ui32> columnMap(columnsCount);
            for (ui32 c = 0; c != columnsCount; ++c) {
                auto index = parser.ColumnIndex(std::string(SelectResultType->GetMemberName(c)));
                Y_ENSURE(index >= 0);
                columnMap[c] = index;
            }

            while (parser.TryNextRow()) {
                NUdf::TUnboxedValue* keyItems;
                NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(KeyType->GetMembersCount(), keyItems);
                NUdf::TUnboxedValue* outputItems;
                NUdf::TUnboxedValue output = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), outputItems);

                for (ui32 j = 0; j != columnsCount; ++j) {
                    auto& v = (ColumnDestinations[j].first == EColumnDestination::Key ? keyItems : outputItems)[ColumnDestinations[j].second];
                    v = YdbValueToUnboxedValue(parser.ColumnParser(j), SelectResultType->GetMemberType(j));
                }

                NUdf::TUnboxedValue *v;
                if (state->FullscanLimit > 0) {
                    auto [it, _] = request->emplace(key, NUdf::TUnboxedValue{});
                    v = &(it->second);
                } else if (auto it = request->find(key); it != request->end()) {
                    v = &(it->second);
                } else {
                    continue;
                }
                if (IsMultiMatches) {
                    *v = HolderFactory.CreateDirectListHolder((*v ? *NKikimr::NMiniKQL::GetDefaultListRepresentation(*v) : NKikimr::NMiniKQL::TDefaultListRepresentation{}).Append(std::move(output)));
                } else {
                    *v = std::move(output); // duplicates will be overwritten
                }
            }
#endif
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) {
                CpuTime->Add(cputime);
            }
            Cerr << "ProcessReceivedData: " << cputime << Endl;
        }

        void FinalizeRequest(TLookupState::TPtr state) {
            if (LocalInFlight == 0) { // PassAway was called
                return;
            }
            --LocalInFlight;
            auto guard = Guard(*Alloc);
            //YQL_CLOG(DEBUG, ProviderGeneric) << "Sending lookup results with " << state->ResultRows << " rows";
            if (AnswerTime) {
                AnswerTime->Add((TInstant::Now() - state->SentTime).MicroSeconds());
                InFlight->Dec();
            }
            Cerr << "AnswerTime " << (TInstant::Now() - state->SentTime) << Endl;
            auto* ev = new IDqAsyncLookupSource::TEvLookupResult(std::move(state->Request), state->ResultRows, state->FullscanLimit);
            if (state->SessionId) {
                SessionIds.push_back(std::move(state->SessionId));
            }
            state.reset();
            TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(ParentId, SelfId(), ev));
        }

    private:
        enum class EColumnDestination {
            Key,
            Output
        };

        std::vector<std::pair<EColumnDestination, size_t>> CreateColumnDestination() {
            THashMap<TStringBuf, size_t> keyColumns;
            for (ui32 i = 0; i != KeyType->GetMembersCount(); ++i) {
                keyColumns[KeyType->GetMemberName(i)] = i;
            }
            THashMap<TStringBuf, size_t> outputColumns;
            for (ui32 i = 0; i != PayloadType->GetMembersCount(); ++i) {
                outputColumns[PayloadType->GetMemberName(i)] = i;
            }

            std::vector<std::pair<EColumnDestination, size_t>> result(SelectResultType->GetMembersCount());
            for (size_t i = 0; i != result.size(); ++i) {
                if (const auto* p = keyColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Key, *p};
                } else if (const auto* p = outputColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Output, *p};
                } else {
                    Y_ABORT();
                }
            }
            return result;
        }

        void MakeSelect(TStringBuilder& out) {
            out << "SELECT ";
            for (ui32 i = 0; i != SelectResultType->GetMembersCount(); ++i) {
                if (i) out << ',';
                out << '`' << EscapeC(SelectResultType->GetMemberName(i)) << '`';
            }
            out << "\n  FROM ";
            out << '`' << EscapeC(LookupSource.GetPath()) << '`';
        }

        void MakeSelectWithKeys(TStringBuilder& out) {
            auto columnsCount = KeyType->GetMembersCount();
            out << "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n";
            out << "DECLARE "<< KeyTupleListName << " AS List<Tuple<";
            for (ui32 c = 0; c != columnsCount; ++c) {
                if (c != 0) {
                    out << ",";
                }
                TStringBuilder output;
                NUdf::TTypePrinter p(*TypeInfoHelper, KeyType->GetMemberType(c));
                p.Out(out.Out);
            }
            out << ">>;\n";
            MakeSelect(out);
            out << "\n WHERE AsTuple(";
            for (ui32 c = 0; c != columnsCount; ++c) {
                if (c != 0) {
                    out << ",";
                }
                out << '`' << EscapeC(KeyType->GetMemberName(c)) << '`';
            }
            out << ") IN " << KeyTupleListName;
        }
        void MakeSelectWithLimit(TStringBuilder& out, ui64 limit, ui64 offset = 0) {
            MakeSelect(out);
            out << " LIMIT " << limit;
            if (offset) {
                out << " OFFSET " << offset;
            }
        }

        // must be called with bound Alloc
        Ydb::Table::ExecuteDataQueryRequest FillSelect(TLookupState::TPtr state) {
            Ydb::Table::ExecuteDataQueryRequest request;
            if (state->FullscanLimit > 0) {
                TStringBuilder out;
                MakeSelectWithLimit(out, state->FullscanLimit);
                request.mutable_query()->set_yql_text(std::move(out));
            } else {
                auto& keyTupleList = (*request.mutable_parameters())[KeyTupleListName];
                auto& keyTupleTypes = *keyTupleList.mutable_type()->mutable_list_type()->mutable_item()->mutable_tuple_type();
                auto keyColumnsCount = KeyType->GetMembersCount();
                for (ui32 c = 0; c != keyColumnsCount; ++c) {
                    ExportTypeToProto(KeyType->GetMemberType(c), *keyTupleTypes.add_elements());
                }
                auto& list = *keyTupleList.mutable_value();
                auto locked = state->Request.lock();
                if (!locked) {
                    throw yexception() << "Actor died";
                }
                for (const auto& [keys, _]: *locked) {
                    auto& row = *list.add_items();
                    for (ui32 c = 0; c != keyColumnsCount; ++c) {
                        auto& value = *row.add_items();
                        ExportValueToProto(KeyType->GetMemberType(c), keys.GetElement(c), value);
                    }
                }
                request.mutable_query()->set_yql_text(SelectWithKeys);
            }
            request.set_session_id(state->SessionId);
            {
                auto& tx_control = *request.mutable_tx_control();
                tx_control.mutable_begin_tx()->mutable_snapshot_read_only();
                tx_control.set_commit_tx(true);
            }
            request.mutable_query_cache_policy()->set_keep_in_cache(true);

            //Cerr << "Query: <<<" << request.DebugString() << ">>>" << Endl;
            return request;
        }

    private:
        const NActors::TActorId ParentId;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<TKeyTypeHelper> KeyTypeHelper;
        NKqpProto::TKikimrLookupSource LookupSource;
        const NKikimr::NMiniKQL::TStructType* const KeyType;
        const NKikimr::NMiniKQL::TStructType* const PayloadType;
        const NKikimr::NMiniKQL::TStructType* const SelectResultType; // columns from KeyType + PayloadType
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const std::vector<std::pair<EColumnDestination, size_t>> ColumnDestinations;
        const size_t MaxKeysInRequest;
        const bool IsMultiMatches;
        ui32 LocalInFlight = 0;
        static inline constexpr std::string_view KeyTupleListName = "$keyTupleList"sv;
        NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper = new NKikimr::NMiniKQL::TTypeInfoHelper();
        TString SelectWithKeys;
        TVector<TString> SessionIds;

        ::NMonitoring::TDynamicCounters::TCounterPtr Count;
        ::NMonitoring::TDynamicCounters::TCounterPtr Fullscans;
        ::NMonitoring::TDynamicCounters::TCounterPtr Keys;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultRows;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultChunks;
        ::NMonitoring::TDynamicCounters::TCounterPtr AnswerTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr CpuTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlight;
        static constexpr size_t MaxSupportedFullscanRequest = 5000; // todo: consider making tweakable
    };

    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateKikimrLookupActor(
        NActors::TActorId parentId,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        NKqpProto::TKikimrLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest,
        const bool isMultiMatches
    )
    {
        auto guard = Guard(*alloc);
        const auto actor = new TKikimrLookupActor(
            std::move(parentId),
            taskCounters,
            alloc,
            keyTypeHelper,
            std::move(lookupSource),
            keyType,
            payloadType,
            typeEnv,
            holderFactory,
            maxKeysInRequest,
            isMultiMatches);
        return {actor, actor};
    }

} // namespace NYql::NDq
