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
import {Cause, Effect, Fiber, ManagedRuntime, Option, Ref} from 'effect'
import {RefetchResponse} from "../types/refetch-response";
import {runInitialSync} from "../utils/run-inital-sync";
import {rollbackTransaction} from "../utils/rollback-transaction";
import {sendTransaction} from "../utils/send-transaction";

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
    onInsert?: (params: InsertMutationFnParams<TItem>) => Effect.Effect<TItem[] | RefetchResponse, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** Handler for update mutations - should return the new state after update */
    onUpdate?: (params: UpdateMutationFnParams<TItem>) => Effect.Effect<Array<TItem> | RefetchResponse, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

    /** Handler for delete mutations - returns void as items are removed from state */
    onDelete?: (params: DeleteMutationFnParams<TItem>) => Effect.Effect<void | RefetchResponse, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>

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
export function localEffectCollectionOptions<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>(
    config: LocalEffectCollectionConfig<TItem, TRuntime>
): CollectionConfig<TItem> {

    const currentDataRef = Ref.unsafeMake<TItem[]>([])
    const originalDataRef = Ref.unsafeMake<Option.Option<TItem[]>>(Option.none())
    const syncParamsRef = Ref.unsafeMake<Option.Option<Parameters<SyncConfig<TItem>["sync"]>[0]>>(Option.none())

    // Sync function implementation using Effect.gen with scoped context
    const sync: SyncConfig<TItem>['sync'] = (params) => {
        const {markReady} = params
        const initialSyncHandler = Ref.set(syncParamsRef, Option.some(params)).pipe(
            Effect.flatMap(() => runInitialSync({
                refs: {syncParams: syncParamsRef, originalData: originalDataRef, currentData: currentDataRef},
                config
            })),
            Effect.onExit(() => Effect.sync(() => markReady())),
            Effect.catchAll(error => Effect.logError("Failed to run initial sync", {error}))
        )
        const fiber = config.runtime.runFork(initialSyncHandler)

        return () => {
            return Effect.gen(function* () {
                yield* Effect.all([
                    Ref.set(currentDataRef, []),
                    Ref.set(originalDataRef, Option.none()),
                    Ref.set(syncParamsRef, Option.none())
                ])
                yield* Fiber.interrupt(fiber)

            }).pipe(config.runtime.runSync)
        }
    }

    // Transaction helper
    const sendEffectTransaction = async (
        params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>,
        effectHandler?: (params: any) => Effect.Effect<Array<TItem> | void | RefetchResponse, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>,
        mutationType: 'insert' | 'update' | 'delete' = 'insert'
    ): Promise<void> => {
        if (!effectHandler) return
        return sendTransaction({
            refs: {currentData: currentDataRef, originalData: originalDataRef, syncParams: syncParamsRef},
            params,
            mutationType,
            effect: effectHandler,
            config
        }).pipe(
            Effect.map(() => void 0),
            Effect.onError(() => rollbackTransaction({config, params, mutationType, refs: {currentData: currentDataRef, originalData: originalDataRef, syncParams: syncParamsRef} })),
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