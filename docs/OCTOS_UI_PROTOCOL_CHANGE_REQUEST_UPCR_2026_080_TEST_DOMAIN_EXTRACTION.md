# UPCR-2026-080 — Mechanical UI test-domain extraction without protocol changes

Status: proposed (implemented for review)
Task: T-0274 / T-0195 / T-0212 / T-0271
Capability: none (no protocol change)

## Problem

Runtime-helper, workspace-browse, and session-CWD regression tests shared a large source file even though their domains could be moved unchanged into child modules.

## Contract

No RPC method, notification, event payload, schema, capability, permission, lifecycle, or client-visible behavior changes. The 63 source scenarios, assertions, fixtures, platform guards, and ignored count are unchanged. Only source locations and fully qualified test module paths change; shared support remains in the parent test module.

## Validation

The consolidated UI test inventory remains 958 names, with only the moved tests gaining their domain-module prefixes. The full transport group passes 956 tests with 2 ignored. CLI all-target Clippy, formatting, typos, quality-ratchet, and diff checks pass.
