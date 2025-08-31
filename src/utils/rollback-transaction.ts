import {Effect, ManagedRuntime, Option, Ref} from "effect";
import type {DeleteMutationFnParams, InsertMutationFnParams, UpdateMutationFnParams} from "@tanstack/react-db";
import {BaseCollectionConfig} from "../types/base-config";
import {Refs} from "../types/refs";
import {CollectionNotInitializedError, DeleteCollectionItemError, WriteCollectionItemError} from "../local/errors";


interface RollbackTransactionOptions<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>> {
    config: BaseCollectionConfig<TItem, TRuntime>
    params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
    mutationType: 'insert' | 'update' | 'delete'
    refs: Refs<TItem>
}

export const rollbackTransaction = <TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>({mutationType, config, params, refs}: RollbackTransactionOptions<TItem, TRuntime>) => Effect.gen(function* () {
    const optimisticKeys = new Set(params.transaction.mutations.map(mutation => mutation.key))
    const itemsToDelete = params.transaction.mutations.map(mutation => mutation.modified)
    yield* Effect.annotateCurrentSpan({optimisticKeys, itemsToDelete})
    const syncParams = yield* Ref.get(refs.syncParams)
    if (Option.isNone(syncParams)) {
        return yield* new CollectionNotInitializedError({
            collectionId: config.id,
            operation: mutationType
        })
    }
    yield* Effect.all(itemsToDelete.map(item => Effect.try({
        try: () => syncParams.value.write({type: "delete", value: item}),
        catch: (cause) => new DeleteCollectionItemError({cause, item, collectionId: config.id})
    })))
    const originalData = yield* Ref.get(refs.originalData)
    if (Option.isNone(originalData)) {
        return yield* Effect.dieMessage("[rollbackTransaction]: nothing to restore")
    }
    yield* Ref.set(refs.currentData, originalData.value)
    const itemsToInsert = originalData.value.filter(item => optimisticKeys.has(config.getKey(item)))
    yield* Effect.all(itemsToInsert.map(item => Effect.try({
        try: () => syncParams.value.write({type: "insert", value: item}),
        catch: (cause) => new WriteCollectionItemError({item, cause, collectionId: config.id})
    })))
    yield* Ref.set(refs.originalData, Option.none())
}).pipe(
    Effect.withSpan("collection.rollbackTransaction"),
    Effect.catchAll(error => Effect.logError(`[rollbackTransaction]: failed to rollback transaction`, {error}))
)
