## Build

Building Ordo from source requires Zig 0.15.2 or newer.

Build the executable:

```sh
zig build
```

The executable is produced at:

```text
zig-out/bin/ordo
```

If you build from source and do not install it into `PATH`, run it as `./zig-out/bin/ordo`.

Run tests with:

```sh
zig build test
```

## TODO
- [x] Fancy command line output
- [ ] User mannual or documentation
- [ ] Official website
- [x] `repl list` shows the connecting status.
- [x] Allow `repl` to connect multiple devices simultiniously
- [ ] Allow `repl` to mount adapters, so that users can call `psu.set_voltage(1.0)` 
- [x] Add a `description` field to adapter command schema
- [x] Change `recipe.step` schema from 
  ```yaml
  call: set
          instrument: d1
  ```
  to
  ```yaml
  call: d1.set
  ```
- [ ] Allow `repl` to set instrument environment config (termination, etc.)