# Effect DB Collection

⚠️ **This project is highly experimental and not ready for production use.**

A collection library that integrates Effect-ts with @tanstack/react-db for functional reactive data management.

## Installation

```bash
npm install effect @tanstack/react-db
```

## Usage

```typescript
import {createCollection} from "@tanstack/react-db"
import {Effect} from "effect"
import {AppRuntime} from "./app-runtime"

export const clientsCollection = createCollection(
    localEffectCollectionOptions<ProxyClientResponse, typeof AppRuntime>({
        id: "clients",
        getKey: (item: ProxyClientResponse) => item.id,
        runtime: AppRuntime,
        effect: Effect.gen(function *() {
           const api = yield* EffectClient
            return yield* api.client.listClients()
        }),
        onInsert: ({transaction, ...rest}) => {
            const insertClient = (client: ProxyClientResponse) => Effect.gen(function* () {
                const api = yield* EffectClient
                return yield* api.client.createClient({
                    name: client.name,
                    enableDynamicDns: client.enableDynamicDns
                })
            })
            return Effect.all(transaction.mutations.map(mutation => insertClient(mutation.modified)), {
                concurrency: "unbounded",
            }).pipe(
                Effect.tapErrorTag("ClientCreateError", ({details}) => toastMessage({
                    type: "error",
                    title: "Failed to create client",
                    description: `Error during client creation: ${details}`
                })),
                Effect.tapErrorTag("BadRequestError", ({details}) => toastMessage({
                    type: "error",
                    title: "Invalid data",
                    description: `Invalid data: ${details}`
                }))
            )
        },
    })
)
```

## Features

- Integration between Effect-ts and TanStack React DB
- Functional reactive data management
- Built-in error handling with Effect's error management
- Support for CRUD operations with Effect generators
- Concurrent operations with Effect.all

## Manual Data Population

For scenarios where you need to manually populate data (e.g., from a sync engine), you can use the `insertManually(data)` method:

```typescript
// Manually insert data from external sources
const syncedData = await syncEngine.fetchData()
await collection.insertManually(syncedData)
```

This is useful when integrating with external data sources or synchronization engines that provide data outside the normal collection flow.

## Known Limitations

- **Data fetching is incomplete** - missing refetch option
- Error handling and tracing need improvement
- Persistence providers are not yet implemented

## Roadmap

Over the next few days, the following improvements are planned:
- Add persistence providers for different storage backends
- Enhance error handling and tracing capabilities
- Complete data fetching implementation with refetch functionality
- Stabilize APIs and improve documentation

## Status

This library is in early development. APIs may change without notice.