/**
 * Local Effect Collection - A TanStack React DB collection implementation using Effect-TS
 *
 * This module provides a collection that integrates TanStack React DB with Effect-TS,
 * enabling reactive data management with optimistic updates, proper error handling,
 * and transaction safety.
 *
 * Key features:
 * - Effect-based mutations with proper error handling
 * - Optimistic updates with automatic rollback on failure
 * - Transaction safety with begin/commit/rollback patterns
 * - Memory leak prevention with proper cleanup
 * - Comprehensive logging and observability
 */

import type {
    CollectionConfig,
    DeleteMutationFnParams,
    InsertMutationFnParams,
    SyncConfig,
    UpdateMutationFnParams,
    UtilsRecord
} from '@tanstack/react-db'
import {Effect, Fiber, ManagedRuntime, Ref} from 'effect'
import {CollectionHandlerError, CollectionNotInitializedError} from './errors'
import {
    BeginSyncError,
    CommitSyncError,
    DeleteCollectionItemError,
    SyncDataResponseError,
    WriteCollectionItemError
} from "./errors";

/**
 * Configuration for a LocalEffectCollection
 * @template TItem - The type of items stored in the collection
 * @template TRuntime - The Effect runtime type providing services
 */
interface LocalEffectCollectionConfig<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>> {
    /** Unique identifier for this collection */
    id: string

    /** Function to extract a unique key from each item */
    getKey: (item: TItem) => string | number

    /** Effect runtime that provides services and executes effects */
    runtime: TRuntime

    /** Effect that fetches the initial data for the collection */
    effect: Effect.Effect<Array<TItem>, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** Handler for insert mutations - should return the new state after insertion */
    onInsert?: (params: InsertMutationFnParams<TItem>) => Effect.Effect<Array<TItem>, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** Handler for update mutations - should return the new state after update */
    onUpdate?: (params: UpdateMutationFnParams<TItem>) => Effect.Effect<Array<TItem>, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** Handler for delete mutations - returns void as items are removed from state */
    onDelete?: (params: DeleteMutationFnParams<TItem>) => Effect.Effect<void, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** How row updates should be applied - 'partial' for merging, 'full' for replacement */
    rowUpdateMode?: 'partial' | 'full'
}

/**
 * Utility functions available on the collection instance
 * @template TItem - The type of items in the collection
 */
interface LocalEffectCollectionUtils<TItem> extends UtilsRecord {
    /** Manually trigger a refetch of the collection data */
    refetch: () => Promise<void>

    /** Clear all data from the collection */
    clearData: () => void
}


/**
 * Creates a TanStack React DB collection configuration using Effect-TS
 *
 * @template TItem - The type of items stored in the collection
 * @template TRuntime - The Effect runtime type providing services
 * @param config - Configuration object for the collection
 * @returns A CollectionConfig compatible with TanStack React DB
 */
export function localEffectCollectionOptions<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>(
    config: LocalEffectCollectionConfig<TItem, TRuntime>
): CollectionConfig<TItem> {

    // Internal state references
    /** Current collection data */
    const dataRef = Ref.unsafeMake<TItem[]>([])

    /** Original data snapshot for rollback purposes */
    const originalDataRef = Ref.unsafeMake<TItem[] | null>(null)

    /** Sync parameters for database operations */
    const syncRef = Ref.unsafeMake<Parameters<SyncConfig<TItem>["sync"]>[0] | null>(null)

    /**
     * Query effect that fetches data from the configured source
     * Includes error handling and observability annotations
     */
    const query = Effect.fn("collection.sync.query")(function* () {
        yield* Effect.logDebug(`[${config.id}] Starting data query`)

        const data = yield* config.effect.pipe(
            Effect.mapError(error => {
                return new SyncDataResponseError({
                    collectionId: config.id,
                    details: {
                        operation: "query",
                        error
                    }
                })
            }),
        )

        yield* Effect.annotateCurrentSpan({
            collectionId: config.id,
            items: data.length
        })

        yield* Effect.logInfo(`[${config.id}] Query completed`, { itemCount: data.length })
        return data
    })

    /**
     * Clears all current items from the collection
     * Used during initial sync to ensure clean state
     */
    const clear = Effect.fn("collection.sync.clear")(function* (writeFn: Parameters<SyncConfig<TItem>["sync"]>[0]["write"]) {
        const currentItems = yield* Ref.get(dataRef)

        yield* Effect.logDebug(`[${config.id}] Clearing ${currentItems.length} items`)

        yield* Effect.annotateCurrentSpan({
            currentItems: currentItems.length
        })

        return yield* Effect.all(currentItems.map(item => Effect.try({
            try: () => writeFn({type: "delete", value: item}),
            catch: (cause) => {
                return new DeleteCollectionItemError({
                    item,
                    cause,
                    collectionId: config.id
                })
            }
        })))
    })

    /**
     * Performs the initial synchronization of the collection
     * This includes fetching data, clearing existing items, and populating with fresh data
     */
    const initialSync = Effect.fn("collection.sync.initialSync")(function* (params: Parameters<SyncConfig<TItem>["sync"]>[0]) {
        yield* Effect.logInfo(`[${config.id}] Starting initial sync`)

        const {begin, write, commit} = params
        const data = yield* query()

        // Begin transaction
        yield* Effect.try({
            try: () => begin(),
            catch: (cause) => {
                return new BeginSyncError({cause, collectionId: config.id})
            }
        })

        // Clear existing data
        yield* Effect.logDebug(`[${config.id}] Clearing existing items`)
        yield* clear(write)

        // Insert fresh data
        yield* Effect.logDebug(`[${config.id}] Inserting ${data.length} new items`)
        yield* Effect.all(data.map(item => Effect.try({
            try: () => write({type: "insert", value: item}),
            catch: (cause) => {
                return new WriteCollectionItemError({
                    collectionId: config.id,
                    item,
                    cause
                })
            }
        })))

        // Commit transaction
        yield* Effect.try({
            try: () => commit(),
            catch: (error) => {
                return new CommitSyncError({
                    collectionId: config.id,
                    cause: error
                })
            }
        })

        // Update in-memory state
        yield* Effect.logDebug(`[${config.id}] Updating in-memory data`)
        yield* Ref.set(dataRef, data)

        yield* Effect.logInfo(`[${config.id}] Initial sync completed successfully`, { itemCount: data.length })
    })


    // Sync function implementation using Effect.gen with scoped context
    const sync: SyncConfig<TItem>['sync'] = (params) => {
        const {markReady} = params

        const initialSyncHandler = Ref.set(syncRef, params).pipe(
            Effect.flatMap(() => initialSync(params)),
            Effect.onExit(() => Effect.sync(() => markReady())),
            Effect.catchAll(error => Effect.logError("Failed to run initial sync", {error}))
        )
        const fiber = config.runtime.runFork(initialSyncHandler)

        return () => {
            return Effect.gen(function* () {
                yield* Effect.all([
                    Ref.set(dataRef, []),
                    Ref.set(originalDataRef, null),
                    Ref.set(syncRef, null)
                ])
                yield* Fiber.interrupt(fiber)

            }).pipe(config.runtime.runSync)
        }
    }

    // Transaction helper
    const sendEffectTransaction = async (
        params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
        effectHandler?: (params: any) => Effect.Effect<Array<TItem> | void, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>,
        mutationType: 'insert' | 'update' | 'delete' = 'insert'
    ): Promise<void> => {
        if (!effectHandler) return
        const transactionEffect = Effect.gen(function* () {
            const optimisticKeys = new Set(params.transaction.mutations.map(m => m.key))
            const serverResponse = yield* effectHandler(params).pipe(
                Effect.mapError(error => new CollectionHandlerError({
                    transactionType: mutationType,
                    cause: error
                }))
            )
            const syncParams = yield* Ref.get(syncRef)
            if (!syncParams) {
                return yield* new CollectionNotInitializedError({
                    collectionId: config.id,
                    operation: mutationType
                })
            }
            //Replace optimistic data if any was returned
            if (!serverResponse || serverResponse.length === 0) {
                return
            }
            const currentData = yield* Ref.get(dataRef)
            //Set the currently stored data in a ref to restore later
            yield* Ref.set(originalDataRef, currentData)
            const newData = currentData.filter(item => !optimisticKeys.has(config.getKey(item)))
            newData.push(...serverResponse)
            yield* Ref.set(dataRef, newData)
            yield* Effect.try({
                try: () => syncParams.begin(),
                catch: (cause) => new BeginSyncError({cause, collectionId: config.id})
            })
            const itemsToDelete = params.transaction.mutations.map(mutation => mutation.modified)
            yield* Effect.all(itemsToDelete.map(item => Effect.try({
                try: () => syncParams.write({type: "delete", value: item}),
                catch: (cause) => new DeleteCollectionItemError({item, cause, collectionId: config.id})
            })))
            yield* Effect.all(serverResponse.map(item => Effect.try({
                try: () => syncParams.write({type: "insert", value: item}),
                catch: (cause) => new WriteCollectionItemError({collectionId: config.id, item, cause})
            })))
            yield* Effect.try({
                try: () => syncParams.commit(),
                catch: (cause) => new CommitSyncError({collectionId: config.id, cause})
            })
        })

        const rollbackEffect = Effect.gen(function* () {
            const optimisticKeys = new Set(params.transaction.mutations.map(m => m.key))
            const itemsToDelete = params.transaction.mutations.map(mutation => mutation.modified)
            const syncParams = yield* Ref.get(syncRef)
            if (!syncParams) {
                return yield* new CollectionNotInitializedError({
                    collectionId: config.id,
                    operation: mutationType
                })
            }
            yield* Effect.all(itemsToDelete.map(item => Effect.try({
                try: () => syncParams.write({type: "delete", value: item}),
                catch: (cause) => new DeleteCollectionItemError({cause, item, collectionId: config.id})
            })))
            const originalData = yield* Ref.get(originalDataRef)
            if (!originalData) {
                return yield* Effect.dieMessage("[rollbackEffect]: noting to restore")
            }
            yield* Ref.set(dataRef, originalData)
            const itemsToInsert = originalData.filter(item => optimisticKeys.has(config.getKey(item)))
            yield* Effect.all(itemsToInsert.map(item => Effect.try({
                try: () => syncParams.write({type: "insert", value: item}),
                catch: (cause) => new WriteCollectionItemError({item, cause, collectionId: config.id})
            })))
            yield* Ref.set(originalDataRef, null)
        }).pipe(
            Effect.catchAll(err => Effect.logError(`Failed to perform rollback`, {
                cause: err.cause
            }))
        )
        return transactionEffect.pipe(
            Effect.onError(() => rollbackEffect),
            config.runtime.runPromise
        )


    }

    // Mutation handlers
    const onInsert = async (params: InsertMutationFnParams<TItem>) => {
        await sendEffectTransaction(params, config.onInsert, 'insert')
    }

    const onUpdate = async (params: UpdateMutationFnParams<TItem>) => {
        await sendEffectTransaction(params, config.onUpdate, 'update')
    }

    const onDelete = async (params: DeleteMutationFnParams<TItem>) => {
        await sendEffectTransaction(params, config.onDelete, 'delete')
    }


    return {
        id: config.id,
        getKey: config.getKey,
        sync: {sync},
        onInsert,
        onUpdate,
        onDelete,
    }
}