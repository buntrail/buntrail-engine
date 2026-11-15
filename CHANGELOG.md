# Changelog

## 0.4.2 — Lossless Git snapshots

Align first-party dependencies with the lossless Git snapshot correction; no API changes in this crate.

## 0.4.1 — Failed-push review

Add guarded acknowledgment of inspected, observed completed failed pushes without replay or changing the original receipt. Unknown outcomes remain pending.

## 0.4.0 — Git sidebar

Expose local/framed sidebar history, commit summaries, Index/Worktree diffs and mutually exclusive typed mutations through existing admission and durable inspection. Correct full-reference lookup for legacy checked-out branch network actions.

## 0.3.0

File-sidebar operations and compatible shared core contracts. C ABI remains version 1.

## 0.2.1 — Unreleased

Allow terminal.start to choose an absolute UTF-8 launch directory independently
of the verified workspace root. Reject malformed supplied directories before
changing terminal state; retain the existing default and ownership contract.

## 0.2.0 — Git Repository

Expose repository APIs and coordinate document admission, frozen repository identity, operation retention and local/framed mutation outcomes.

## 0.1.1

Keep terminal restoration state in memory, preserve catalog documents and verified ownership, and retain safe same-run remote reconnect context.

## Unreleased — initial 0.1.0 preparation

- Extracted self-contained crate source, resources, documentation and checks.
- Added MIT licensing and versioned Buntrail dependencies.
- Publication and platform acceptance are tracked separately from source preparation.
