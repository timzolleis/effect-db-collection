import {Effect, ManagedRuntime, Schema} from "effect";

export interface BaseCollectionConfig<TItem extends object, TRuntime extends ManagedRuntime.ManagedRuntime<any, any>> {
    getKey: (item: TItem) => string | number
    id: string
    effect: Effect.Effect<Array<TItem>, unknown, ManagedRuntime.ManagedRuntime.Context<TRuntime>>
}