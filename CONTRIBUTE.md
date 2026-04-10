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
- [] Fantasy command line output