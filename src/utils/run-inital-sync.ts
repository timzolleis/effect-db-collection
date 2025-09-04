import { Effect } from 'effect'
import { BaseCollectionConfig } from '../types/base-config'
import { SyncDataResponseError } from '../local/errors'
import { CollectionService } from './collection-service'
import { Collection } from '@tanstack/react-db'

export interface RunInitialSyncOptions<TItem extends object, TRuntimeContext> {
  config: BaseCollectionConfig<TItem, TRuntimeContext>
  collection: Collection<TItem>
}

const runEffectQuery = <TItem extends object, TRuntimeContext>({
  effect,
  collectionId
}: {
  effect: Effect.Effect<TItem[], unknown, TRuntimeContext>
  collectionId: string
}) =>
  Effect.gen(function* () {
    yield* Effect.logDebug(`[${collectionId}]: Starting data query`)
    const data = yield* effect.pipe(
      Effect.mapError((error) => {
        return new SyncDataResponseError({
          collectionId,
          details: {
            operation: 'query',
            error
          }
        })
      })
    )
    yield* Effect.annotateCurrentSpan({
      collectionId,
      items: data.length
    })
    yield* Effect.logDebug(`[${collectionId}] Query completed`, { itemCount: data.length })
    return data
  }).pipe(Effect.withSpan('collection.sync.query'))

export const runInitialSync = <TItem extends object, TRuntimeContext>({
  config,
  collection
}: RunInitialSyncOptions<TItem, TRuntimeContext>) =>
  Effect.gen(function* () {
    yield* Effect.logDebug(`[${config.id}] Starting initial sync`)
    const collectionService = yield* CollectionService

    const data = yield* runEffectQuery({ effect: config.effect, collectionId: config.id })
    // Begin transaction
    yield* collectionService.begin
    yield* Effect.logDebug(`[${config.id}] Clearing existing items`)
    const items = Array.from(collection.entries()).map(([_, item]) => item)
    yield* Effect.all(
      items.map((item) => collectionService.write<TItem>({ type: 'delete', value: item })),
      { concurrency: 'unbounded' }
    )
    // Insert fresh data
    yield* Effect.logDebug(`[${config.id}] Inserting ${data.length} new items`)
    yield* Effect.all(data.map((item) => collectionService.write({ type: 'insert', value: item })))

    yield* collectionService.commit
    yield* Effect.logDebug(`[${config.id}] Initial sync completed successfully`, { itemCount: data.length })
  }).pipe(Effect.withSpan('collection.sync.initialSync'))
