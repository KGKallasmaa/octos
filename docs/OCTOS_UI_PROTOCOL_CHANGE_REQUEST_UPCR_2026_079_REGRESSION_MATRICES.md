# UPCR-2026-079 — Table-driven regression matrices without protocol changes

Status: proposed (implemented for review)
Task: T-0274 / T-0201 / T-0208 / T-0221 / T-0227 / T-0229 / T-0231 / T-0233
Capability: none (no protocol change)

## Problem

Seven independent regression groups duplicated the same harness control flow with different data. Maintaining separate functions made fixture updates harder to review without adding protocol coverage.

## Contract

No RPC method, notification, event payload, schema, capability, permission, lifecycle, or client-visible behavior changes. The changes only execute the original scenarios through named table-driven cases. WS Origin rejection reasons and every other error shape, command, message, and filesystem assertion remain intact; 20 scenarios remain while 20 harness functions become 7.

## Validation

The consolidated loader (51), plugin tool (123), prompt (7), budget (15), admin (29), router (37), and WS Origin (7) groups all pass. Agent and CLI all-target Clippy, formatting, typos, quality-ratchet, and diff checks pass. Each original slice also killed a targeted production mutation before that mutation was reverted.
