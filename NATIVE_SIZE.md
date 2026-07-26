# Native size and Rust implementation decisions

Research and measurement date: 2026-07-13

## Outcome

The measured arm64-v8a Release APK fell from 80.46 MiB to 42.90 MiB. Against
the same stock React Native Test App baseline (18.59 MiB), the SurrealDB package
increment fell from 61.88 MiB to 24.31 MiB, a 60.7% reduction.

The optimized APK contains:

| Library | Uncompressed bytes in APK | MiB |
| --- | ---: | ---: |
| `libsurrealdb_rn_core.so` | 25,052,376 | 23.89 |
| `libreact-native-surrealdb.so` | 402,800 | 0.38 |
| combined SurrealDB native code | 25,455,176 | 24.28 |

Run the paired measurement with:

```sh
pnpm --filter react-native-surrealdb run ubrn:android:size
pnpm --filter surrealdb-harness-rn86 run size:android:benchmark
```

The report records the stock baseline, exact native libraries, optimized
reference, commands, dates, and hard budgets in
`apps/harness-rn86/size-results/android/report.json`. Generated results are
ignored; the reference metadata remains in
`apps/harness-rn86/size-budget.json`.

## Changes retained

- Rust Release builds use `opt-level = "z"`, fat LTO, and one codegen unit.
- Android uses the shared-library path already supported by
  `uniffi-bindgen-react-native`; the Rust core remains a `cdylib` and the small
  React Native JSI adapter links to it.
- Rust artifacts retain symbols because the UniFFI generator reads the native
  library to generate bindings. Android strips the final app libraries.
- SurrealDB session state uses an async `RwLock`: queries briefly take the read
  side to clone the SDK handle, while sign-in, authentication, invalidation, and
  namespace/database changes take the write side across the mutation. This
  prevents concurrent session mutations from overwriting each other without
  blocking unrelated query clones on a synchronous mutex.

## Changes deliberately not retained

- `panic = "abort"` can reduce code size further, but it would turn a recoverable
  Rust panic at the FFI boundary into a process abort. That is the wrong default
  for a database SDK until crash behavior is measured and explicitly accepted.
- Dynamic iOS frameworks are not enabled yet. The current
  `uniffi-bindgen-react-native` iOS builder selects static `.a` files and creates
  an XCFramework with `xcodebuild -create-xcframework`; unlike its Android path,
  it has no shared-library option. Shipping a dynamic iOS framework therefore
  requires generator work plus rpath, code-signing, CocoaPods embedding, device,
  and simulator tests. The linked dylib guide is a useful prototype, but this
  should not be smuggled into the package as unmaintained custom glue.
- A blanket replacement of every `Mutex` with `RwLock` would be incorrect.
  Live-query resource teardown is write-only and remains a mutex; only the SDK
  session handle benefits from concurrent reads.

## Sources

- [Rust Noobie Best Practices](https://ospfranco.com/rust-tips-from-a-noob/)
- [Complete guide to Rust dylibs in iOS and Android](https://ospfranco.com/complete-guide-to-dylibs-in-ios-and-android/)
- [`uniffi-bindgen-react-native` configuration reference](https://github.com/jhugman/uniffi-bindgen-react-native)
- [SurrealDB 3.0 benchmark report](https://surrealdb.com/blog/surrealdb-3-0-benchmarks-a-new-foundation-for-performance)
- [SurrealDB crud-bench](https://github.com/surrealdb/crud-bench)
