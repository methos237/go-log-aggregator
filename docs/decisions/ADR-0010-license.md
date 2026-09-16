# ADR-0010: AGPL-3.0 license

- **Status:** Accepted
- **Date:** 2026-09-16 (backfilled; the LICENSE file dates from the first commit)
- **Supersedes:** none

## Context

The repository needs a license a reviewer can read in one line and a contributor
can rely on. The code is network server software: its normal use is to run it as a
service that other people's programs and browsers talk to.

## Decision

The project is licensed under the GNU Affero General Public License, version 3.0.

Anyone may run, study, modify and redistribute it. Anyone who runs a modified
version as a network service must offer its users the corresponding source. That
network clause is the reason for choosing AGPL over GPL: for server software, the
ordinary GPL trigger (distribution of a binary) rarely fires, and a hosted fork could
diverge privately.

Rejected:

- **MIT or Apache-2.0.** The default for Go libraries and the easiest for a company
  to adopt. This is not a library; it is a service, and a permissive license lets a
  hosted fork take the work without returning changes. Adoption by companies with
  AGPL policies is not a goal of a portfolio project.
- **GPL-3.0.** Copyleft, but its trigger is distribution. A service that is only ever
  run, never shipped, is never obliged to share.
- **A source-available license (BSL, SSPL).** Not open source by the OSI definition,
  and the restrictions they add protect a commercial product this project is not.

## Consequences

- Contributions are accepted under the same license. No contributor license
  agreement, since the project has one author.
- Every dependency is MIT, BSD, Apache-2.0 or MPL-2.0 licensed (the MPL ones are
  `hashicorp/memberlist` and its helpers), all of which AGPL-3.0 may include. A future
  dependency under an incompatible license would need replacing.
- The README states the license and the one-sentence reason next to it, so nobody
  has to open the 34 KB LICENSE file to learn what applies.
