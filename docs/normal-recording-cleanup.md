# Normal session recording storage and cleanup

Normal capture sessions are stored under dated directories:

```text
<record-dir>/<YYYY-MM-DD>/<session-id>/
```

`CAPMESH_RECORD_MAX_NORMAL_TOTAL_SIZE` limits the combined size of these normal
session directories. It does not include packet continuous capture or workload
log capture, which keep their existing independent rolling retention behavior.
The default limit is `50GiB`; `0` disables the total-size limit and cleanup API.

Before creating a normal session, the server scans the normal recording
directories. When usage is greater than or equal to the configured limit,
`CreateSession` returns gRPC `ResourceExhausted` without creating a session,
packet pipeline, recorder, or agent command. Back up recordings that must be
kept, then inspect the cleanup plan:

```bash
capmesh-client get recordings

capmesh-client clean recordings --dry-run
```

Run cleanup after reviewing the preview:

```bash
capmesh-client clean recordings
```

The interactive command requires confirmation and warns that deletion is
permanent. Automation must opt in explicitly with `--yes`.

Cleanup deletes whole normal-session directories, oldest first, until usage is
at most 50% of the configured maximum. Directory-sized deletion can bring usage
below the exact target. A directory is eligible only when its recording has
finished, it is not active, and its recovery desired state is not `RUNNING`.
Continuous packet data, workload logs, active sessions, recoverable sessions,
unknown/corrupt metadata, symlinks, and paths outside the configured recording
root are never selected.

Viewer, admin, and shared tokens may list recordings. Only admin and shared
tokens may preview or execute cleanup.
