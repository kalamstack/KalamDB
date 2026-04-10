# Notes Audit

This directory replaces the old monolithic [Notes.md](../Notes.md) task dump.

Last audit: 2026-04-10

Files:
- [todo.md](todo.md): tasks that are still open, only partially addressed, or still relevant enough to keep on the backlog.
- [finished.md](finished.md): tasks that are completed, superseded, or no longer relevant, including the items reclassified during the 2026-04-10 repo audit.

Audit rules:
- A task moved to finished only when the current repo had direct evidence in code, tests, docs, or runtime wiring.
- A task marked superseded means the original request is covered by a newer surface or architecture, even if the exact old shape is gone.
- Anything still ambiguous stayed in todo.
- Task numbers are historical and are not globally unique. Use the section plus the task number together when referencing an item.