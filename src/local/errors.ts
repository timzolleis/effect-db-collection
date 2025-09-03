import {Data} from "effect";

export class CollectionHandlerError extends Data.TaggedError("RunHandlerError")<{
    transactionType?: 'insert' | 'update' | 'delete'
    cause: unknown
}> {
}

export class CollectionNotInitializedError extends Data.TaggedError("CollectionNotInitializedError")<{
    readonly collectionId: string;
}> {
}


import type {ParseError} from "effect/ParseResult";

export class BeginSyncError extends Data.TaggedError("BeginSyncError")<{
    collectionId: string
    cause: unknown
}> {
}

export class SyncDataResponseError extends Data.TaggedError("SyncDataResponseError")<{
    collectionId: string
    readonly details: {
        operation: "query",
        error: unknown
    } | { operation: "parse", error: ParseError }
}> {
}

export class CommitSyncError extends Data.TaggedError("CommitSyncError")<{
    collectionId: string
    cause: unknown
}> {
}


export class DeleteCollectionItemError<TItem extends object> extends Data.TaggedError("DeleteCollectionItemError")<{
    item: TItem,
    cause: unknown
    collectionId: string
}> {
}

export class UpdateCollectionItemError<TItem extends object> extends Data.TaggedError("UpdateCollectionItemError")<{
    item: TItem,
    cause: unknown
    collectionId: string
}> {

}


export class CreateCollectionItemError<TItem extends object> extends Data.TaggedError("CreateCollectionError")<{
    collectionId: string
    cause: unknown
    item: TItem,
}> {}

