#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#include <src/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/protos/ydb_federation_discovery.pb.h>

#include <src/client/topic/common/callback_context.h>
#include <src/client/common_client/impl/client.h>
#include <ydb-cpp-sdk/client/federated_topic/federated_topic.h>

#include <util/system/spinlock.h>

#include <deque>
#include <memory>

namespace NYdb::inline V3::NFederatedTopic {

class TFederatedDbObserverImpl : public TClientImplCommon<TFederatedDbObserverImpl>,
                                 public NTopic::TEnableSelfContext<TFederatedDbObserverImpl> {
public:
    static constexpr TDuration REDISCOVER_DELAY = TDuration::Seconds(60);

public:
    TFederatedDbObserverImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TFederatedTopicClientSettings& settings);

    ~TFederatedDbObserverImpl();

    std::shared_ptr<TFederatedDbState> GetState();

    NThreading::TFuture<void> WaitForFirstState();

    void Start();
    void Stop();

    bool IsStale() const;

private:
    Ydb::FederationDiscovery::ListFederationDatabasesRequest ComposeRequest() const;
    void RunFederationDiscoveryImpl();
    void ScheduleFederationDiscoveryImpl(TDuration delay);
    void OnFederationDiscovery(TStatus&& status, Ydb::FederationDiscovery::ListFederationDatabasesResult&& result);

private:
    std::shared_ptr<TFederatedDbState> FederatedDbState;
    NThreading::TPromise<void> PromiseToInitState;
    TRpcRequestSettings RpcSettings;
    TSpinLock Lock;

    NTopic::IRetryPolicy::TPtr FederationDiscoveryRetryPolicy;
    NTopic::IRetryPolicy::IRetryState::TPtr FederationDiscoveryRetryState;
    NYdbGrpc::IQueueClientContextPtr FederationDiscoveryDelayContext;

    bool Stopping = false;
};

class TFederatedDbObserver : public NTopic::TContextOwner<TFederatedDbObserverImpl> {
public:
    inline TFederatedDbObserver(std::shared_ptr<TGRpcConnectionsImpl> connections,
                                const TFederatedTopicClientSettings& settings)
        : TContextOwner(connections, settings) {
    }

    inline std::shared_ptr<TFederatedDbState> GetState() {
        return TryGetImpl()->GetState();
    }

    inline NThreading::TFuture<void> WaitForFirstState() {
        return TryGetImpl()->WaitForFirstState();
    }

    inline void Start() {
        return TryGetImpl()->Start();
    }

    inline void Stop() {
        return TryGetImpl()->Stop();
    }

    inline bool IsStale() const {
        return TryGetImpl()->IsStale();
    }
};

} // namespace NYdb::V3::NFederatedTopic
