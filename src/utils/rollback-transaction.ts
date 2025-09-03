import {Effect, Option, Ref} from "effect";
import type {DeleteMutationFnParams, InsertMutationFnParams, UpdateMutationFnParams} from "@tanstack/react-db";
import {BaseCollectionConfig} from "../types/base-config";
import {Refs} from "../types/refs";
import {CollectionService} from "./collection-service";


interface RollbackTransactionOptions<TItem extends object, TRuntimeContext> {
    config: BaseCollectionConfig<TItem, TRuntimeContext>
    params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
    refs: Refs<TItem>
}

export const rollbackTransaction = <TItem extends object, TRuntimeContext>({ config, params, refs}: RollbackTransactionOptions<TItem, TRuntimeContext>) => Effect.gen(function* () {
    const optimisticKeys = new Set(params.transaction.mutations.map(mutation => mutation.key))
    const itemsToDelete = params.transaction.mutations.map(mutation => mutation.modified)
    yield* Effect.annotateCurrentSpan({collectionId: config.id, optimisticKeysCount: optimisticKeys.size, itemsToDeleteCount: itemsToDelete.length})

    const collectionService = yield* CollectionService
    yield* collectionService.begin

    // Remove optimistic items from collection
    yield* Effect.all(itemsToDelete.map(item =>
        collectionService.write({type: "delete", value: item})
    ))

    // Restore original data to refs and collection
    const originalData = yield* Ref.get(refs.originalData)
    if (Option.isNone(originalData)) {
        yield* collectionService.commit
        return yield* Effect.dieMessage("[rollbackTransaction]: nothing to restore")
    }

    yield* Ref.set(refs.currentData, originalData.value)

    // Re-insert original items that were optimistically modified
    const itemsToInsert = originalData.value.filter(item => optimisticKeys.has(config.getKey(item)))
    yield* Effect.all(itemsToInsert.map(item =>
        collectionService.write({type: "insert", value: item})
    ))

    yield* collectionService.commit
}).pipe(
    Effect.withSpan("collection.rollbackTransaction"),
    Effect.catchAll(error => Effect.logError(`[rollbackTransaction]: failed to rollback transaction`, {error}))
)
