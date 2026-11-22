# V2 integrated candidate provenance

The integrated candidate starts from `origin/v2-rust-plan` at `fa279f2f894cd9c86de708f943b36a7ef9f8fb1c`.
It consumes only the clean local shadow integration ref at `716ad4dffc547c9a531a3cc00ca02eb3e11fae2f`.
The integration merge retains the shadow ref as its second parent, so the complete source history remains reachable and the merge can be reverted with first-parent mainline semantics.
The already-landed Click reducer is shared history rather than a duplicate transplant.
Workspace manifests were merged semantically and `v2/Cargo.lock` was regenerated from the combined manifests.

## Source and rollback boundaries

| Slice | Exact published source | Shadow rollback boundary |
| --- | --- | --- |
| Slice 0 contracts | `bdff91628c9b4fe05db55c8f8902aa612ced6906` | `fcb4491a7da1835892f86dd6ee6cf829499374fa` |
| Slice 1 parser | `48bc144eb409346d4da9d1c65011e8a85f173710` | `d211933265bf64d53a7d2b5994ff9cb9d998f2f4` |
| Slice 2 identity | `160cbf22869da42e0bb8ba6a6697f2d2a473773e` | `04c1fb5d35bf26591b87acfd8594073b26948e4f` |
| Slice 3 event reducer | `902e70c60b5c8876cd9476c55a7327e53e59e903` | `eedafa130bccaf4da25a1cf186fd9f10fb4c2aaa` |
| Slice 4 Click reducer | `3bcf4478e557bb1ba83de2f00e9eaafb37aae8ac` | `a412b19e950e3a9616195845faa6eeb459ae7440` |
| Slice 5 sweep | `5590e9ea25c1999648eb0c0ad3f9393898c20311` | `a843dd5219e1f6f9d5da17eb51c589843061f541` |
| Slice 6 output | `df6647e4b5f2c7ef4e8b8faf09dfdc743cb8fe29` | `4c1e91ace43b61a97655c29fed80f22f14b53b8f` |
| Application and validation wiring | `7ca5c727ab35cbda722eb8b3e4cd99960b3f0911` | same commit |
| Multi-arena orchestration, telemetry, and physical schema completion | `716ad4dffc547c9a531a3cc00ca02eb3e11fae2f` | same commit |
| Harness observer optimization and equivalence tests | `afc6fce7b6965e8ac556b32f35561e39506f3b71` | same commit |

## Qualification caveats

The shadow qualification passed 75 Rust tests, eight Python tests, all 36 fixtures, 10 randomized differential seeds, exact practical 1M output, physical schema validation, deterministic repeat, timed-run correctness, telemetry validation, and a scorer-constrained `linux/amd64` debug build.
Its complete harness refused timing publication because measured observer overhead was `0.5788%`, above the strict `0.5000%` ceiling.
Its `linux/amd64` release-image build also exceeded its 1,200-second dependency-compilation timeout.

The integrated candidate's exact scorer-facing root Dockerfile subsequently produced a `linux/amd64` release image in 114 seconds.
That image passed all 36 fixtures, all 10 randomized differential seeds, exact practical 1M output, physical schema validation, deterministic repeat, timed-run correctness, and telemetry validation.
Before the observer optimization, the integrated harness also refused timing publication at `0.5177%` overhead.
After integrating `afc6fce7b6965e8ac556b32f35561e39506f3b71`, its equivalence tests passed and the exact root-image harness passed with `0.2776%` observer overhead, strictly below the unchanged `0.5000%` ceiling.
The historical shadow timeout and both earlier overhead refusals remain disclosed rather than retrospectively represented as green results.
