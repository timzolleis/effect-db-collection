import {Effect, Ref} from "effect";
import {Refs} from "../types/refs";
import {BaseCollectionConfig} from "../types/base-config";
import {SyncDataResponseError} from "../local/errors";
import {CollectionService} from "./collection-service";
export interface RunInitialSyncOptions<TItem extends object, TRuntimeContext> {
    refs: Refs<TItem>
    config: BaseCollectionConfig<TItem, TRuntimeContext>
}

const runEffectQuery = <TItem extends object, TRuntimeContext>({effect, collectionId}: {
    effect: Effect.Effect<TItem[], unknown, TRuntimeContext>,
    collectionId: string
}) => Effect.gen(function* () {
    yield* Effect.logDebug(`[${collectionId}]: Starting data query`)
    const data = yield* effect.pipe(
        Effect.mapError(error => {
            return new SyncDataResponseError({
                collectionId,
                details: {
                    operation: "query",
                    error
                }
            })
        }),
    )
    yield* Effect.annotateCurrentSpan({
        collectionId,
        items: data.length
    })
    yield* Effect.logDebug(`[${collectionId}] Query completed`, {itemCount: data.length})
    return data
}).pipe(
    Effect.withSpan("collection.sync.query")
)


const clearCollection = <TItem extends object>({refs, collectionId}: {
    refs: Refs<TItem>,
    collectionId: string
}) => Effect.gen(function* () {
    const collectionService = yield* CollectionService
    const currentItems = yield* Ref.get(refs.currentData)
    yield* Effect.logDebug(`[${collectionId}] Clearing ${currentItems.length} items`)

    yield* Effect.annotateCurrentSpan({
        currentItems: currentItems.length
    })
    return yield* Effect.all(currentItems.map(item => collectionService.write({type: "delete", value: item})))
}).pipe(
    Effect.withSpan("collection.sync.clear")
)


export const runInitialSync = <TItem extends object, TRuntimeContext>({
                                                                          config,
                                                                          refs
                                                                      }: RunInitialSyncOptions<TItem, TRuntimeContext>) => Effect.gen(function* () {
    yield* Effect.logDebug(`[${config.id}] Starting initial sync`)
    const collectionService = yield* CollectionService

    const data = yield* runEffectQuery({effect: config.effect, collectionId: config.id})
    // Begin transaction
    yield* collectionService.begin
    yield* Effect.logDebug(`[${config.id}] Clearing existing items`)
    yield* clearCollection({refs, collectionId: config.id})
    // Insert fresh data
    yield* Effect.logDebug(`[${config.id}] Inserting ${data.length} new items`)
    yield* Effect.all(data.map(item => collectionService.write({type: "insert", value: item})))

    yield* collectionService.commit
    // Update in-memory state
    yield* Effect.logDebug(`[${config.id}] Updating in-memory data`)
    yield* Ref.set(refs.currentData, data)
    yield* Effect.logDebug(`[${config.id}] Initial sync completed successfully`, {itemCount: data.length})
}).pipe(
    Effect.withSpan("collection.sync.initialSync")
)