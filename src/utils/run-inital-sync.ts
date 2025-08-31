import { Effect, ManagedRuntime, Option, Ref } from 'effect'
import { BaseCollectionConfig } from '../types/base-config'
import { Refs } from '../types/refs'
import {
  BeginSyncError,
  CollectionNotInitializedError,
  CommitSyncError,
  DeleteCollectionItemError,
  SyncDataResponseError,
  WriteCollectionItemError
} from '../local/errors'

export interface RunInitialSyncOptions<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>> {
  refs: Refs<TItem>
  config: BaseCollectionConfig<TItem, TRuntime>
}

const runEffectQuery = <TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>({
  effect,
  collectionId
}: {
  effect: Effect.Effect<TItem[], unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>
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
    yield* Effect.logInfo(`[${collectionId}] Query completed`, {
      itemCount: data.length
    })
    return data
  }).pipe(Effect.withSpan('collection.sync.query'))

const clearCollection = <TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>({
  refs,
  collectionId
}: {
  refs: Refs<TItem>
  collectionId: string
}) =>
  Effect.gen(function* () {
    const syncParams = yield* Ref.get(refs.syncParams)
    if (Option.isNone(syncParams)) {
      return yield* new CollectionNotInitializedError({
        operation: 'delete',
        collectionId
      })
    }
    const currentItems = yield* Ref.get(refs.currentData)
    yield* Effect.logDebug(`[${collectionId}] Clearing ${currentItems.length} items`)

    yield* Effect.annotateCurrentSpan({
      currentItems: currentItems.length
    })
    return yield* Effect.all(
      currentItems.map((item) =>
        Effect.try({
          try: () => syncParams.value.write({ type: 'delete', value: item }),
          catch: (cause) => {
            return new DeleteCollectionItemError({
              item,
              cause,
              collectionId
            })
          }
        })
      )
    )
  }).pipe(Effect.withSpan('collection.sync.clear'))

export const runInitialSync = <TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>>({
  config,
  refs
}: RunInitialSyncOptions<TItem, TRuntime>) =>
  Effect.gen(function* () {
    const syncParams = yield* Ref.get(refs.syncParams)
    if (Option.isNone(syncParams)) {
      return yield* new CollectionNotInitializedError({
        operation: 'delete',
        collectionId: config.id
      })
    }
    yield* Effect.logInfo(`[${config.id}] Starting initial sync`)

    const { begin, write, commit } = syncParams.value
    const data = yield* runEffectQuery({
      effect: config.effect,
      collectionId: config.id
    })
    // Begin transaction
    yield* Effect.try({
      try: () => begin(),
      catch: (cause) => {
        return new BeginSyncError({ cause, collectionId: config.id })
      }
    })
    yield* Effect.logDebug(`[${config.id}] Clearing existing items`)
    yield* clearCollection({ refs, collectionId: config.id })
    // Insert fresh data
    yield* Effect.logDebug(`[${config.id}] Inserting ${data.length} new items`)
    yield* Effect.all(
      data.map((item) =>
        Effect.try({
          try: () => write({ type: 'insert', value: item }),
          catch: (cause) => {
            return new WriteCollectionItemError({
              collectionId: config.id,
              item,
              cause
            })
          }
        })
      )
    )
    yield* Effect.try({
      try: () => commit(),
      catch: (cause) => new CommitSyncError({ collectionId: config.id, cause })
    })
    // Update in-memory state
    yield* Effect.logDebug(`[${config.id}] Updating in-memory data`)
    yield* Ref.set(refs.currentData, data)
    yield* Effect.logInfo(`[${config.id}] Initial sync completed successfully`, { itemCount: data.length })
  }).pipe(Effect.withSpan('collection.sync.initialSync'))
