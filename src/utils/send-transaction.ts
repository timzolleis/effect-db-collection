import { Effect } from 'effect'
import type { DeleteMutationFnParams, InsertMutationFnParams, UpdateMutationFnParams } from '@tanstack/react-db'
import { BaseCollectionConfig } from '../types/base-config'
import { RefetchResponse } from '../types/refetch-response'
import { CollectionHandlerError } from '../local/errors'
import { CollectionService } from './collection-service'
import { runInitialSync } from './run-inital-sync'

interface SendTransactionOptions<TItem extends object, TRuntimeContext> {
  config: BaseCollectionConfig<TItem, TRuntimeContext>
  params: InsertMutationFnParams<TItem> | UpdateMutationFnParams<TItem> | DeleteMutationFnParams<TItem>
  effect: (params: any) => Effect.Effect<Array<TItem> | RefetchResponse | void, unknown, TRuntimeContext>
  mutationType: 'insert' | 'update' | 'delete'
}

export const sendTransaction = <TItem extends object, TRuntimeContext>({
  effect,
  params,
  config,
  mutationType
}: SendTransactionOptions<TItem, TRuntimeContext>) =>
  Effect.gen(function* () {
    const serverResponse = yield* effect(params).pipe(
      Effect.mapError((error) => new CollectionHandlerError({ transactionType: mutationType, cause: error }))
    )

    const collectionService = yield* CollectionService

    const deleteTransactions = params.transaction.mutations.filter((mutation) => mutation.type === 'delete')
    const updateTransactions = params.transaction.mutations.filter((mutation) => mutation.type === 'update')
    const insertTransactions = params.transaction.mutations.filter((mutation) => mutation.type === 'insert')

    yield* collectionService.begin
    //Delete transactions need to be deleted, no matter what happens
    yield* Effect.all(
      deleteTransactions.map((transaction) => collectionService.write({ value: transaction.modified, type: 'delete' }))
    )
    if (!serverResponse) {
      yield* Effect.logDebug('[sendTransaction]: No response from handler')
      return yield* collectionService.commit
    }

    if (Array.isArray(serverResponse)) {
      //Handle updates
      Effect.all(
        [...updateTransactions, ...insertTransactions].map((item) =>
          collectionService.write({ type: 'delete', value: item.modified })
        )
      )
      yield* Effect.all(serverResponse.map((item) => collectionService.write({ type: 'insert', value: item })))

      yield* collectionService.commit
    } else {
      yield* collectionService.commit
      const { refetch } = serverResponse
      if (!refetch) {
        //TODO: Check if we should preserve optimistic keys in this case - Tim, 09/04/25
        yield* Effect.logDebug(`[sendTransaction]: Refetch disabled, optimistic keys removed`)
        return
      }

      yield* Effect.logDebug(`[sendTransaction]: Refetch enabled, running initial sync`)
      return yield* runInitialSync({ config, collection: params.collection }).pipe(
        Effect.tapError((error) => Effect.logError(`[sendTransaction]: failed to refetch collection`, { error: error }))
      )
    }
  }).pipe(Effect.withSpan('collection.sendTransaction'))
