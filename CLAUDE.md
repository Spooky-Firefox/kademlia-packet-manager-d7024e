## Subagent Working Agreements

- **The brief is a hypothesis.** Verify the premise before implementing (reproduce
  the defect or trace the cited paths at their current state); if evidence
  contradicts it, STOP and report — never improvise a different fix or implement
  a proven no-op.
- **Comments are claims, not ground truth.** Verify against code; if your change
  falsifies a nearby comment, fix that comment in the same change.
- **"Is X enabled / did we fix Y" answers are chains, not values.** Enumerate
  every layer with file:line, date it with git log (audits lag same-day fixes),
  and present the table before the verdict. A reversed answer means you never
  had the whole chain — stop and enumerate; don't re-sample.
- **Tier evidence** in every report: verified / inferred / assumed. Other agents'
  reports are claims — reproduce what you build on.
- **Never reason about correctness from timestamps.** Delete stale binaries
  before builds; verify by exit code AND fresh file times AND running the result.
- **Missing test scaffolding is work, not an excuse.** Extend a sibling pattern
  before declaring tests out of scope; a skip is a loudly-flagged deviation.
- **Review tiers.** Security/input-parsing, concurrency/lifetime, and
  user-visible-behavior changes get the strongest-model adversarial review
  pre-merge; mechanical classes may use a standard-model review running the
  adversarial-review playbook, with periodic strongest-model spot-audits.
