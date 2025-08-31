import {Effect, ManagedRuntime, Option, Ref} from "effect";
import type {DeleteMutationFnParams, InsertMutationFnParams, UpdateMutationFnParams} from "@tanstack/react-db";
import {BaseCollectionConfig} from "../types/base-config";
import {RefetchResponse} from "../types/refetch-response";
import {Refs} from "../types/refs";
import {
    BeginSyncError,
    CollectionHandlerError,
    CollectionNotInitializedError, CommitSyncError,
    DeleteCollectionItemError, WriteCollectionItemError
} from "../local/errors";
import {runInitialSync} from "./run-inital-sync";


interface SendTransactionOptions<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>> {
    config: BaseCollectionConfig<TItem, TRuntime>
    params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
    effect: (params: any) => Effect.Effect<Array<TItem> | RefetchResponse | void, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>
    mutationType: 'insert' | 'update' | 'delete'
    refs: Refs<TItem>
}

export const sendTransaction = <TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>({effect, params, config, refs, mutationType}: SendTransactionOptions<TItem, TRuntime>) => Effect.gen(function* () {
    const optimisticKeys = new Set(params.transaction.mutations.map(mutation => mutation.key))
    yield* Effect.logInfo("Sending transaction")
    const serverResponse = yield* effect(params).pipe(
        Effect.mapError(error => new CollectionHandlerError({transactionType: mutationType, cause: error}))
    )
    const syncParams = yield* Ref.get(refs.syncParams)
    if (Option.isNone(syncParams)) {
        return yield* new CollectionNotInitializedError({collectionId: config.id, operation: "insert"})
    }
    if (!serverResponse) {
        yield* Effect.logDebug(`[sendTransaction]: No data returned from server handler`)
        return
    }
    yield* Effect.logInfo("Done with transaction query", {serverResponse})
    if (!Array.isArray(serverResponse)) {
        const {refetch} = serverResponse
        if (!refetch) {
            return yield* Effect.logInfo(`[sendTransaction]: Refetch disabled, returning`)
        }
        yield* Effect.logInfo(`[sendTransaction]: Refetch response returned from server effect, rerunning initial effect`)
        return yield* runInitialSync({
            config,
            refs
        }).pipe(Effect.catchAll(error => Effect.logDebug(`[sendTransaction]: failed to run initial sync to refetch`, {error})))
    }
    const currentData = yield* refs.currentData.get
    yield* Effect.logDebug(`[sendTransaction]: Storing current data in backup ref`)
    yield* Ref.set(refs.originalData, Option.some(currentData))
    const newData = currentData.filter(item => !optimisticKeys.has(config.getKey(item)))
    newData.push(...serverResponse)
    yield* Ref.set(refs.currentData, newData)
    yield* Effect.try({
        try: () => syncParams.value.begin(),
        catch: (cause) => new BeginSyncError({cause, collectionId: config.id})
    })
    const itemsToDelete = params.transaction.mutations.map(mutation => mutation.modified)
    yield* Effect.all(itemsToDelete.map(item => Effect.try({
        try: () => syncParams.value.write({type: "delete", value: item}),
        catch: (cause) => new DeleteCollectionItemError({item, cause, collectionId: config.id})
    })))
    yield* Effect.all(serverResponse.map(item => Effect.try({
        try: () => syncParams.value.write({type: "insert", value: item}),
        catch: (cause) => new WriteCollectionItemError({collectionId: config.id, item, cause})
    })))
    yield* Effect.try({
        try: () => syncParams.value.commit(),
        catch: (cause) => new CommitSyncError({collectionId: config.id, cause})
    })
}).pipe(
    Effect.catchAllDefect(def => Effect.logError("caught defect")),
    Effect.withSpan("collection.sendTransaction")
)