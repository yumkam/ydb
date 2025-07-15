#pragma once

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql::NDq {

// remove LEGACY* support after upgrade S3/Generic Sources to use modern format

enum TInputChannelFormat {
    FORMAT_UNKNOWN,
    SIMPLE_SCALAR,
    SIMPLE_WIDE,
    BLOCK_WIDE,
    LEGACY_CH,
    LEGACY_SIMPLE_BLOCK,
    LEGACY_TUPLED_BLOCK
};

template <class TDerived, class IInputInterface>
class TDqInputImpl : public IInputInterface {
public:
    TDqInputImpl(NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes)
        : InputType(inputType)
        , Width(inputType->IsMulti() ? static_cast<const NKikimr::NMiniKQL::TMultiType*>(inputType)->GetElementsCount() : TMaybe<ui32>{})
        , MaxBufferBytes(maxBufferBytes)
    {
    }

    i64 GetFreeSpace() const override {
        return (i64) MaxBufferBytes - StoredBytes;
    }

    ui64 GetStoredBytes() const override {
        return StoredBytes;
    }

    [[nodiscard]]
    bool Empty() const override {
        return BeforeBarrier.Batches == 0;
    }

    bool IsLegacySimpleBlock(NKikimr::NMiniKQL::TStructType* structType, ui32& blockLengthIndex) {
        auto index = structType->FindMemberIndex(BlockLengthColumnName);
        if (index) {
            for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                auto type = structType->GetMemberType(i);
                if (!type->IsBlock()) {
                    return false;
                }
            }
            blockLengthIndex = *index;
            return true;
        } else {
            return false;
        }
    }

    TInputChannelFormat GetFormat() {
        if (Width) {
            switch (InputType->GetKind()) {
                case NKikimr::NMiniKQL::TTypeBase::EKind::Struct: {
                    auto structType = static_cast<NKikimr::NMiniKQL::TStructType*>(InputType);
                    for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                        if (structType->GetMemberType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple: {
                    auto tupleType = static_cast<NKikimr::NMiniKQL::TTupleType*>(InputType);
                    for (ui32 i = 0; i < tupleType->GetElementsCount(); i++) {
                        if (tupleType->GetElementType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                case NKikimr::NMiniKQL::TTypeBase::EKind::Multi: {
                    auto multiType = static_cast<NKikimr::NMiniKQL::TMultiType*>(InputType);
                    for (ui32 i = 0; i < multiType->GetElementsCount(); i++) {
                        if (multiType->GetElementType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            return SIMPLE_WIDE;
        }

        if (InputType->IsStruct()) {
            return IsLegacySimpleBlock(static_cast<NKikimr::NMiniKQL::TStructType*>(InputType), LegacyBlockLengthIndex) ? LEGACY_SIMPLE_BLOCK : SIMPLE_SCALAR;
        } else if (InputType->IsResource()) {
            if (static_cast<NKikimr::NMiniKQL::TResourceType*>(InputType)->GetTag() == "ClickHouseClient.Block") {
                return LEGACY_CH;
            }
        } else if (InputType->IsTuple()) {
            auto tupleType= static_cast<NKikimr::NMiniKQL::TTupleType*>(InputType);
            if (tupleType->GetElementsCount() == 2) {
                auto type = tupleType->GetElementType(0);
                if (type->IsStruct()) {
                    return IsLegacySimpleBlock(static_cast<NKikimr::NMiniKQL::TStructType*>(type), LegacyBlockLengthIndex) ? LEGACY_TUPLED_BLOCK : SIMPLE_SCALAR;
                } else if (InputType->IsResource()) {
                    if (static_cast<NKikimr::NMiniKQL::TResourceType*>(InputType)->GetTag() == "ClickHouseClient.Block") {
                        return LEGACY_CH;
                    }
                }
            }
        }

        return SIMPLE_SCALAR;
    }

    ui64 GetRowsCount(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) {
        if (Y_UNLIKELY(Format == FORMAT_UNKNOWN)) {
            Format = GetFormat();
        }

        switch (Format) {
            case BLOCK_WIDE: {
                ui64 result = 0;
                batch.ForEachRowWide([&](NUdf::TUnboxedValue* values, ui32 width) {
                    const auto& blockLength = values[width - 1];
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_CH:
                // can't count rows inside CH UDF resource
                return 0;
            case LEGACY_SIMPLE_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    const auto& blockLength = value.GetElement(LegacyBlockLengthIndex);
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_TUPLED_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    auto value0 = value.GetElement(0);
                    const auto& blockLength = value0.GetElement(LegacyBlockLengthIndex);
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case SIMPLE_SCALAR:
            case SIMPLE_WIDE:
            default:
                return batch.RowCount();
        }
    }

    ui64 AddBatch(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());

        ui64 rows = GetRowsCount(batch);
        StoredBytes += space;
        StoredRows += rows;

        if (GetFreeSpace() < 0) {
            static_cast<TDerived*>(this)->PopStats.TryPause();
        }

        Batches.emplace_back(std::move(batch));
        auto& barrier = PendingBarriers.empty() ? BeforeBarrier : PendingBarriers.back();
        barrier.Batches ++;
        barrier.Bytes += space;
        barrier.Rows += rows;
        Cerr << "AddBatch " << Endl;
        DumpBarrier();

        return rows;
    }

    void SquashWatermarks() {
        while (!PendingBarriers.empty()) {
            auto& barrier = PendingBarriers.front();
            if (barrier.Barrier >= WatermarkBarrier) {
                break;
            }
            Cerr << "Squash " << barrier.Barrier << " into " << WatermarkBarrier << Endl; // FIXME logging?
            BeforeBarrier.Batches += barrier.Batches;
            BeforeBarrier.Rows += barrier.Rows;
            BeforeBarrier.Bytes += barrier.Bytes;
            PendingBarriers.pop_front();
        }
    }

    void PauseByWatermark(TInstant watermark) override {
        Y_ENSURE(WatermarkBarrier <= watermark);
        Cerr << "PauseByWatermark " << WatermarkBarrier << "->" << watermark << Endl;
        WatermarkBarrier = watermark;
        SquashWatermarks();
        DumpBarrier();
    }

    void DumpBarrier() {
        Cerr << "PendingBarriers";
        auto dump = [](auto& b) {
            Cerr << ' ' << b.Batches << 'B' << b.Rows << 'r' << b.Bytes << 'b';
            if (b.Barrier != TInstant::Max()) Cerr << b.Barrier << 'w';
        };
        for (auto &b : PendingBarriers) {
            dump(b);
        }
        Cerr << Endl;
        Cerr << "WatermarkBarrier: " << WatermarkBarrier << Endl;
        Cerr << "BeforeBarrier"; dump(BeforeBarrier);
        Cerr << Endl;
    }

    void AddBarrier(TInstant watermark) {
        Y_ENSURE (PendingBarriers.empty() || PendingBarriers.back().IsCheckpoint() || PendingBarriers.back().Barrier < watermark);
        //if (PendingBarriers.empty() || PendingBarriers.back().Barrier != watermark) 
        {
            PendingBarriers.emplace_back(TBarrier { .Barrier = watermark });
        }
        Cerr << "AddBarrier " << watermark << Endl;
        DumpBarrier();
    }

    void AddCheckpoint() override {
        AddBarrier(TInstant::Max());
    }

    void AddWatermark(TInstant watermark) override {
        if (watermark >= WatermarkBarrier) {
            AddBarrier(watermark);
        }
    }

    bool IsPausedByWatermark() const override {
        return !PendingBarriers.empty() && !PendingBarriers.front().IsCheckpoint();
    }

    bool IsPausedByCheckpoint() const override {
        return !PendingBarriers.empty() && PendingBarriers.front().IsCheckpoint();
    }

private:
    void Resume() {
        Cerr << "Resume" << Endl;
        DumpBarrier();
        Y_ENSURE(BeforeBarrier.Batches == 0);
        Y_ENSURE(BeforeBarrier.Rows == 0);
        Y_ENSURE(BeforeBarrier.Bytes == 0);
        BeforeBarrier = PendingBarriers.front();
        PendingBarriers.pop_front();
    }

public:
    void ResumeByWatermark([[maybe_unused]] TInstant watermark) override {
        Y_ENSURE(IsPausedByWatermark());
        Resume();
        //Y_ENSURE(WatermarkBarrier == watermark);
        if (WatermarkBarrier != watermark) { Cerr << "ResumeByWatermark " << WatermarkBarrier << " != " << watermark << Endl; }
        //if (WatermarkBarrier == watermark) {
            WatermarkBarrier = {};
        //}
    }

    void ResumeByCheckpoint() override {
        Y_ENSURE(IsPausedByCheckpoint());
        Resume();
        SquashWatermarks();
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());
        if (Empty()) {
            static_cast<TDerived*>(this)->PushStats.TryPause();
            return false;
        }

        batch.clear();

        auto& popStats = static_cast<TDerived*>(this)->PopStats;

        popStats.Resume(); //save timing before processing
        ui64 popBytes = 0;

        if (IsPaused()) {
            ui64 batchesCount = BeforeBarrier.Batches;
            Y_ABORT_UNLESS(batchesCount > 0);
            Y_ABORT_UNLESS(batchesCount <= Batches.size());

            if (batch.IsWide()) {
                while (batchesCount--) {
                    auto& part = Batches.front();
                    part.ForEachRowWide([&batch](NUdf::TUnboxedValue* values, ui32 width) {
                        batch.PushRow(values, width);
                    });
                    Batches.pop_front();
                }
            } else {
                while (batchesCount--) {
                    auto& part = Batches.front();
                    part.ForEachRow([&batch](NUdf::TUnboxedValue& value) {
                        batch.emplace_back(std::move(value));
                    });
                    Batches.pop_front();
                }
            }

            popBytes = BeforeBarrier.Bytes;

            StoredBytes -= BeforeBarrier.Bytes;
            StoredRows -= BeforeBarrier.Rows;
        } else {
            if (batch.IsWide()) {
                for (auto&& part : Batches) {
                    part.ForEachRowWide([&batch](NUdf::TUnboxedValue* values, ui32 width) {
                        batch.PushRow(values, width);
                    });
                }
            } else {
                for (auto&& part : Batches) {
                    part.ForEachRow([&batch](NUdf::TUnboxedValue& value) {
                        batch.emplace_back(std::move(value));
                    });
                }
            }

            popBytes = StoredBytes;

            StoredBytes = 0;
            StoredRows = 0;
            Batches.clear();
        }
        BeforeBarrier = {};

        if (popStats.CollectBasic()) {
            popStats.Bytes += popBytes;
            popStats.Rows += GetRowsCount(batch);
            popStats.Chunks++;
        }

        Y_ABORT_UNLESS(!batch.empty());
        return true;
    }

    void Finish() override {
        Finished = true;
    }

    bool IsFinished() const override {
        return Finished && (!IsPaused() || Batches.empty());
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return InputType;
    }

    bool IsPaused() const {
        return !PendingBarriers.empty();
    }

protected:

    TMaybe<ui32> GetWidth() const {
        return Width;
    }

protected:
    NKikimr::NMiniKQL::TType* const InputType = nullptr;
    const TMaybe<ui32> Width;
    const ui64 MaxBufferBytes = 0;
    TList<NKikimr::NMiniKQL::TUnboxedValueBatch, NKikimr::NMiniKQL::TMKQLAllocator<NKikimr::NMiniKQL::TUnboxedValueBatch>> Batches;
    ui64 StoredBytes = 0;
    ui64 StoredRows = 0;
    bool Finished = false;
    TInputChannelFormat Format = FORMAT_UNKNOWN;
    ui32 LegacyBlockLengthIndex = 0;
    struct TBarrier {
        ui64 Batches = 0;
        ui64 Bytes = 0;
        ui64 Rows = 0;
        TInstant Barrier = TInstant::Max();
        // watermark (!= TInstant::Max()) or checkpoint (TInstant::Max())
        bool IsCheckpoint() const {
            return Barrier == TInstant::Max();
        }
    };
    std::deque<TBarrier> PendingBarriers;
    TBarrier BeforeBarrier;
    TInstant WatermarkBarrier;
};

} // namespace NYql::NDq
