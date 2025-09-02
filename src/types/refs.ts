import type { SyncConfig } from '@tanstack/react-db'
import { Option, Ref } from 'effect'

export type Refs<TItem extends object> = {
  syncParams: Ref.Ref<Option.Option<Parameters<SyncConfig<TItem>['sync']>[0]>>
  currentData: Ref.Ref<TItem[]>
  originalData: Ref.Ref<Option.Option<TItem[]>>
}
