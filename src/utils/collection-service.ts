import { Effect } from 'effect'
import type { ChangeMessage, SyncConfig } from '@tanstack/react-db'
import {
  BeginSyncError,
  CommitSyncError,
  CreateCollectionItemError,
  DeleteCollectionItemError,
  UpdateCollectionItemError
} from '../local/errors'

export class CollectionService extends Effect.Service<CollectionService>()('CollectionService', {
  effect: Effect.fnUntraced(function* ({
    syncParams,
    collectionId
  }: {
    syncParams: Parameters<SyncConfig<any>['sync']>[0]
    collectionId: string
  }) {
    const begin = Effect.try({
      try: () => syncParams.begin(),
      catch: (error) => new BeginSyncError({ collectionId, cause: error })
    })
    const write = <TItem extends object>(options: Omit<ChangeMessage<TItem>, `key`>) =>
      Effect.try({
        try: () => syncParams.write(options),
        catch: (cause) => {
          const item = options.value
          switch (options.type) {
            case 'delete': {
              return new DeleteCollectionItemError({ collectionId, cause, item })
            }
            case 'insert': {
              return new CreateCollectionItemError({ collectionId, cause, item })
            }
            case 'update': {
              return new UpdateCollectionItemError({ collectionId, cause, item })
            }
          }
        }
      })
    const commit = Effect.try({
      try: () => syncParams.commit(),
      catch: (cause) => new CommitSyncError({ cause, collectionId })
    })

    return { begin, write, commit } as const
  })
}) {}
