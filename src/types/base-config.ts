import { Effect } from 'effect'

export interface BaseCollectionConfig<TItem extends object, TRuntimeContext> {
  getKey: (item: TItem) => string | number
  id: string
  effect: Effect.Effect<Array<TItem>, unknown, TRuntimeContext>
}
