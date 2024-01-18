
change the line 89 in ```crates/node/src/commands/run.rs``` in deoxys to
```rust
NetworkType::Main => "http://127.0.0.1:3000",
```
start the server