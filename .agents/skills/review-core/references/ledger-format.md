# Review Ledger Format

This specification defines the machine-validated finding grammar for Vertebra review ledgers.

---

## Severity Scale

| Severity | Definition |
| --- | --- |
| `critical` | Undefined behavior, data race, deadlock, missed wakeup, memory corruption, use-after-release of SHM/FFI views, security vulnerability, data loss, wire-protocol framing violation, or GIL re-enabling under 3.14t. |
| `important` | Correctness flaw on an edge path, resource leak (ring slot, file descriptor, SHM segment, Python refcount), unbounded memory growth, measured performance regression, missing test for modified behavior, or interface drift. |
| `minor` | Code layout, symbol naming, documentation omission, or test hygiene issue with no operational correctness risk. |

---

## Grammar Specification

Each finding block is defined by an H3 heading followed by five bulleted fields:

```markdown
### <ID> · <severity> · <path>[:L<start>[-L<end>]]
- **Finding**: <Concise summary of the defect or non-conformance>
- **Evidence**: <Code citation plus reproducible test result or specification reference>
- **Fix**: <Minimal code modification that satisfies the checklist requirement>
- **Confidence**: <verified|likely>
- **Status**: <open|accepted|duplicate:<anchor>|fp:FP-<PREFIX>-NN|wontfix:<reason>|fixed:<sha>>
```

---

## Validation Rules

1. **Heading format**:
   - Must match `^###\s+([A-Z]+-\d{2}|[A-Z]+-NEW)\s+·\s+(critical|important|minor)\s+·\s+([^:\s]+)(?::L\d+(-L\d+)?)?$`.
   - `<path>` must exist in the repository relative to repo root.
   - If line bounds `L<start>` and `L<end>` are specified, they must be within the file's line count.

2. **Confidence**:
   - Must be either `verified` or `likely`.
   - When `Status: accepted` on `critical` or `important` findings, `Confidence: verified` is strictly required.

3. **Status**:
   - `open`: Newly reported candidate finding awaiting verification or triage.
   - `accepted`: Confirmed defect scheduled for remediation.
   - `duplicate:<anchor>`: Duplicate of an existing ledger entry.
   - `fp:FP-<PREFIX>-NN`: Suppressed under an entry in `false-positive-suppressions.md`.
   - `wontfix:<reason>`: Explicitly rejected finding with rationale.
   - `fixed:<sha>`: Remediated by the specified commit SHA.
