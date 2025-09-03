import {Option, Ref} from "effect";

export type Refs<TItem extends object> = {
    currentData:  Ref.Ref<TItem[]>
    originalData: Ref.Ref<Option.Option<TItem[]>>

}