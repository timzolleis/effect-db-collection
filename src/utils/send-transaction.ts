import {Effect, Option, Ref} from "effect";
import type {DeleteMutationFnParams, InsertMutationFnParams, UpdateMutationFnParams} from "@tanstack/react-db";
import {BaseCollectionConfig} from "../types/base-config";
import {Refs} from "../types/refs";
import {RefetchResponse} from "../types/refetch-response";
import {CollectionHandlerError} from "../local/errors";
import {CollectionService} from "./collection-service";
import {runInitialSync} from "./run-inital-sync";

interface SendTransactionOptions<TItem extends object, TRuntimeContext> {
    config: BaseCollectionConfig<TItem, TRuntimeContext>
    params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
    effect: (params: any) => Effect.Effect<Array<TItem> | RefetchResponse | void, unknown, TRuntimeContext>
    mutationType: 'insert' | 'update' | 'delete'
    refs: Refs<TItem>
}

const removeOptimisticKeys = <TItem extends object>(
    optimisticKeys: Set<string | number>,
    config: BaseCollectionConfig<TItem, any>,
    refs: Refs<TItem>
) => Effect.gen(function* () {
    const currentData = yield* refs.currentData.get
    yield* Ref.set(refs.originalData, Option.some(currentData))
    const filteredData = currentData.filter(item => !optimisticKeys.has(config.getKey(item)))
    yield* Ref.set(refs.currentData, filteredData)
})

export const sendTransaction = <TItem extends object, TRuntimeContext>({effect, params, config, refs, mutationType}: SendTransactionOptions<TItem, TRuntimeContext>) => Effect.gen(function* () {
    const optimisticKeys = new Set(params.transaction.mutations.map(mutation => mutation.key))
    yield* Effect.annotateCurrentSpan({collectionId: config.id, mutationType, optimisticKeysCount: optimisticKeys.size})

    yield* Effect.logDebug("Sending transaction", {mutationType, optimisticKeysCount: optimisticKeys.size})

    const serverResponse = yield* effect(params).pipe(
        Effect.mapError(error => new CollectionHandlerError({transactionType: mutationType, cause: error}))
    )

    const collectionService = yield* CollectionService
    yield* collectionService.begin

    // Remove deleted items from collection
    const deleteTransactions = params.transaction.mutations.filter(m => m.type === "delete")
    yield* Effect.all(deleteTransactions.map(transaction =>
        collectionService.write({value: transaction.modified, type: "delete"})
    ))

    if (!serverResponse) {
        yield* collectionService.commit
        yield* Effect.logDebug(`[sendTransaction]: No data returned from server handler`)
        return
    }

    if (Array.isArray(serverResponse)) {
        // Remove optimistic keys and add server data to refs
        yield* removeOptimisticKeys(optimisticKeys, config, refs)
        const currentData = yield* refs.currentData.get
        yield* Ref.set(refs.currentData, [...currentData, ...serverResponse])

        // Update collection with server data
        const itemsToRemove = params.transaction.mutations
            .filter(m => m.type !== "delete")
            .map(m => m.modified)

        yield* Effect.all(itemsToRemove.map(item =>
            collectionService.write({type: "delete", value: item})
        ))
        yield* Effect.all(serverResponse.map(item =>
            collectionService.write({type: "insert", value: item})
        ))

        yield* collectionService.commit
    } else {
        yield* collectionService.commit
        yield* removeOptimisticKeys(optimisticKeys, config, refs)

        const {refetch} = serverResponse
        if (!refetch) {
            yield* Effect.logDebug(`[sendTransaction]: Refetch disabled, optimistic keys removed`)
            return
        }

        yield* Effect.logDebug(`[sendTransaction]: Refetch enabled, running initial sync`)
        return yield* runInitialSync({config, refs})
            .pipe(Effect.catchAll(error =>
                Effect.logDebug(`[sendTransaction]: failed to run initial sync`, {error})
            ))
    }
}).pipe(
    Effect.withSpan("collection.sendTransaction")
)