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
    UpdateMutationFnParams
} from '@tanstack/react-db'
import {Effect, Fiber, Layer, ManagedRuntime, Option, Ref} from 'effect'
import {runInitialSync} from "../utils/run-inital-sync";
import {sendTransaction} from "../utils/send-transaction";
import {rollbackTransaction} from "../utils/rollback-transaction";
import {RefetchResponse} from "../types/refetch-response";
import {CollectionService} from "../utils/collection-service";

/**
 * Configuration for a LocalEffectCollection
 * @template TItem - The type of items stored in the collection
 * @template TRuntime - The Effect runtime type providing services
 */
interface LocalEffectCollectionConfig<TItem extends object, TRuntimeContext> {
    /** Unique identifier for this collection */
    id: string

    /** Function to extract a unique key from each item */
    getKey: (item: TItem) => string | number

    /** Effect runtime that provides services and executes effects */
    runtime: ManagedRuntime.ManagedRuntime<TRuntimeContext, never>

    /** Effect that fetches the initial data for the collection */
    effect: Effect.Effect<Array<TItem>, unknown,TRuntimeContext>

    /** Handler for insert mutations - should return the new state after insertion */
    onInsert?: (params: InsertMutationFnParams<TItem>) => Effect.Effect<TItem[] | RefetchResponse, unknown, TRuntimeContext>

    /** Handler for update mutations - should return the new state after update */
    onUpdate?: (params: UpdateMutationFnParams<TItem>) => Effect.Effect<Array<TItem> | RefetchResponse, unknown, TRuntimeContext>

    /** Handler for delete mutations - returns void as items are removed from state */
    onDelete?: (params: DeleteMutationFnParams<TItem>) => Effect.Effect<void | RefetchResponse, unknown, TRuntimeContext>

    /** How row updates should be applied - 'partial' for merging, 'full' for replacement */
    rowUpdateMode?: 'partial' | 'full'
}

/**
 * Creates a TanStack React DB collection configuration using Effect-TS
 *
 * @template TItem - The type of items stored in the collection
 * @template TRuntime - The Effect runtime type providing services
 * @param config - Configuration object for the collection
 * @returns A CollectionConfig compatible with TanStack React DB
 */
export function localEffectCollectionOptions<TItem extends object, TRuntimeContext>(
    config: LocalEffectCollectionConfig<TItem, TRuntimeContext>
): CollectionConfig<TItem> {

    const currentDataRef = Ref.unsafeMake<TItem[]>([])
    const originalDataRef = Ref.unsafeMake<Option.Option<TItem[]>>(Option.none())
    const collectionServiceRef = Ref.unsafeMake<Option.Option<Layer.Layer<CollectionService>>>(Option.none())


    // Sync function implementation using Effect.gen with scoped context
    const sync: SyncConfig<TItem>['sync'] = (params) => {
        const {markReady} = params

        const captureServiceLayer = Ref.set(collectionServiceRef, Option.some(CollectionService.Default({collectionId: config.id, syncParams: params})))
        const initialSyncHandler = captureServiceLayer.pipe(
            Effect.flatMap(() => runInitialSync({
                refs: {originalData: originalDataRef, currentData: currentDataRef},
                config
            })),
            Effect.onExit(() => Effect.sync(() => markReady())),
            Effect.catchAll(error => Effect.logError("Failed to run initial sync", {error}))
        )
        const fiber = config.runtime.runFork(initialSyncHandler.pipe(Effect.provide(CollectionService.Default({
            syncParams: params,
            collectionId: config.id
        }))))

        return () => {
            return Effect.gen(function* () {
                yield* Effect.all([
                    Ref.set(currentDataRef, []),
                    Ref.set(originalDataRef, Option.none()),
                ])
                yield* Fiber.interrupt(fiber)

            }).pipe(config.runtime.runSync)
        }
    }

    // Transaction helper
    const sendEffectTransaction = async (
        params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
        effectHandler?: (params: any) => Effect.Effect<Array<TItem> | void | RefetchResponse, unknown, TRuntimeContext>,
        mutationType: 'insert' | 'update' | 'delete' = 'insert'
    ): Promise<void> => {
        if (!effectHandler) return
        return sendTransaction({
            refs: {currentData: currentDataRef, originalData: originalDataRef},
            params,
            mutationType,
            effect: effectHandler,
            config
        }).pipe(
            Effect.map(() => void 0),
            Effect.onError((err) => Effect.gen(function* () {
                yield* Effect.logError(err)
                return yield* rollbackTransaction({
                    config,
                    refs: {currentData: currentDataRef, originalData: originalDataRef},
                    params,
                })
            })),
            Effect.provide(Layer.unwrapEffect(Effect.gen(function* () {
                const collectionService = yield* Ref.get(collectionServiceRef)
                if (Option.isNone(collectionService)) {
                    return yield* Effect.dieMessage(`[${config.id}: Collection not initialized`)
                }
                return collectionService.value
            }))),
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