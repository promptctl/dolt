# This is a patch-holding fork of Dolt

[links-issue-tracker](https://github.com/promptctl/links-issue-tracker) — the
`lit` CLI — embeds Dolt as its storage engine and needs a small number of
patches on top of it. This fork is where those patches live. It is not a
competing project and not a rewrite: everything here except the patches is
[dolthub/dolt](https://github.com/dolthub/dolt)'s work, under Dolt's own
Apache-2.0 license.

**Branch `lit` is the branch `lit` builds against.** `main` tracks upstream and
carries no patches.

**The patch ledger and the rebase procedure live in the `lit` repo**, next to
the `go.mod` `replace` directive that consumes them:
**[FORKS.md](https://github.com/promptctl/links-issue-tracker/blob/master/FORKS.md)**.
That file is authoritative. It records what each patch changes, why it exists,
what would let it be dropped, how to rebase this fork onto a newer upstream, and
the check that proves a rebase did not quietly reintroduce a copyleft
dependency. Deliberately none of that is repeated here — one copy cannot go
stale against another.

## License and modification notices

`LICENSE` is upstream's, unmodified. Apache-2.0 section 4(b) requires modified
files to carry prominent notice that they changed, so every file this fork
patches has a `NOTICE` comment directly under its upstream copyright header,
naming the modification and pointing at the ledger. Upstream ships no `NOTICE`
file, so there is none to carry forward.
