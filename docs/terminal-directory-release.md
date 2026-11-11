# Terminal launch-directory release preparation

Baseline: released 0.2.0, commit 6df4019f2af49d4e43eb0c23ef1365a6522e8a1e.
Candidate: 0.2.1. No publication or remote push is authorized by this preparation.

The optional JSON directory is parsed before workspace/layout mutation and passed
through TerminalOpenRequest. Root, target, owner, lease and runtime remain intact.
No C ABI or protocol changes. Existing launch receipts and uncertainty behavior
are unchanged. Consumers must not retry uncertain launches.

Regression command: cargo test --locked terminal_start_. The default-root case
passes on the baseline; explicit local/remote directory and malformed-directory
cases fail there. The remote fixture checks actual shell pwd in a nested Unicode
folder with spaces and verifies workspace/owner identity remains unchanged.

Run scripts/check.sh with the matching helper and ripgrep 15.1.0. Package with
cargo package --locked --offline; review the resulting crate before separately
authorizing publication. Live SSH/container and Windows acceptance are unverified.
