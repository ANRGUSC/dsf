# DSF Project Rules

## Code Organization

1. **Modular file design**: Keep separate concerns in separate files. Controllers, schedulers, profilers, template logic, etc. should each live in their own file — not bundled into a single monolithic source. Each file should have a clear, single responsibility.

   Examples:
   - `cmd/odag-controller/main.go` — controller entry point and pod lifecycle
   - `cmd/odag-controller/heft.go` — HEFT scheduling algorithm
   - `cmd/odag-controller/schedule.go` — schedule prediction utilities
   - `cmd/odag-controller/profiler.go` — profiler DB and EMA logic
   - `cmd/odag-controller/template.go` — ODAGTemplate watch and run creation
