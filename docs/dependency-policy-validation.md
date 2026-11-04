# Staged Python adaptation validation

Task c3b2f03f-c0c2-4fee-8ade-1ce6a6705a9c; branch codex/policy-a-c3b2f03f.
Base `618fc175da36c63dd78c5a6b05a5aa2d74613f06`.
No commit, push, PR, host setting, merge, deployment or board update performed.
Seed evidence is preserved separately in `dependency-policy-seed-validation.md`;
its full npm build and independent rounds 1-3 are not Python validation claims.
Aggregate round 4 completed. Its accepted fixes are staged here; final aggregate
round 5 belongs to the parent and remains pending.

## Focused evidence

- 113 policy/runner tests passed. Shared npm regression tests remain scoped to
  seed fixtures. Python AUTO uses this repository's complete actual manifest and
  lock with a synthetic test-tool patch replay; it is fixture evidence, not a
  claim that a historical PR qualified or that new artifact hashes were fetched.
- An isolated replay of the authentic pytest-mock minor delta, with actual
  distribution hashes and signed trailer, also qualifies for AUTO. Lock-only
  updates are tested within the real constraint. The unmodified historical PR
  does not qualify; its complete data remains a negative regression.
- Authentic GitHub signed profiler PRs #1 and #69 preserve complete immutable
  before/after TOML and commit metadata. Both are correctly HUMAN: #1 also
  changes hashes on unrelated native distributions; #69 includes a major and
  transitive changes. The adapter does not ignore that lock churn to obtain AUTO.
- Actual runner tests cover Python blob parsing, authenticated metadata, AUTO,
  HUMAN approval and CI/hold checks, API revocation, duplicate publication and S1
  ordinary PR success without bot-context writes, stale head, wrong branch/author, closed
  state and API error. Neither automation nor bot approval is granted to humans.
- Official Mergify Draft 2020-12 schema passes. Parsed YAML confirms queue entry,
  final merge and PR-rule eligibility are identical; old locations/schema keys
  and batch merging were replaced without removing legacy CI conditions.
- actionlint 1.7.12 passes the two new trusted policy/signal workflows. Including
  modified ci-cd.yml reports its pre-existing obsolete actions/setup-python@v4.
  That unrelated action upgrade is not bundled here. CI token restriction and
  policy suite wiring are tested. Existing CodeQL v2 remains unchanged.
- Python adapter focused E/F/I lint uses each repository's locked Ruff version,
  isolated configuration and Python 3.11 target; Black uses locked 23.9.1.
  Its one conditional-expression formatting change is semantic-neutral.
  App sources, pyproject.toml, poetry.lock and application dependencies unchanged.

## Boundaries

No existing virtualenv was available in these prepared worktrees. Full app
Poetry/native/scientific installs and Docker builds were not duplicated locally;
required repository build/test/security CI must run after parent-authorized PRs.
No assertions, timeouts, error handling or existing check requirements were
weakened. Tests added the repository-specific branch/check-count boundary.
Live Mergify service semantics, policy/app protection binding, queue vetoes and
remote CI remain unverified; activation remains blocked. `Build` and `Build, check, test, push` are required by existing branch protection. The legacy Mergify `CodeQL` condition is also retained; CodeQL is not currently a required protection context.

Artifacts: `/tmp/c3b2f03f-python-r4-fix-iaesymnv`
(fresh task-owned tools and final focused run logs).
Final staged tree/base/path manifest is recorded there after final validation.

## S2 regression

Bot-only `Dependency policy Dependabot` readiness is required in the common
Mergify eligibility anchor. The actual API runner reproduces a signed Dependabot
head with retained stale pnupu approval, then an ordinary fork PR using that same
SHA. Fresh active-bot ownership keeps general and bot readiness failed. Further tests
revoke cached bot readiness after initial GET, CI API, readiness-write and
general-write failures. An ordinary API failure cannot touch bot-only contexts.
S1 assertions now require no ordinary writes to either bot-only context; this
strengthens isolation instead of publishing an ordinary AUTO failure.

## Accepted aggregate round-4 fixes

- Removed the workflow job's repository-variable gate. Workflow-level assertions
  prohibit that gate and job-level conditional execution, while activation.json
  remains false. Ordinary/adoption status reporting no longer depends on a
  repository variable or a deadlocking activation order.
- Fresh authenticated open-PR inventory and matching bot GETs assign shared-head
  general status to an active bot PR. The manual path cannot alternate it. A
  closed/moved/retargeted bot releases ownership, regardless of historical status.
- Actual runner regression completes 500 shared-SHA scans in alternating PR
  orders, both disabled and enabled, with a 1000-write-per-SHA/context mock cap:
  three initial writes, no
  repeated writes, and five total writes after current-head pnupu approval
  recovers general and bot readiness. Added lookup-error and release regressions.
- The parent verified the actual organization's Enterprise plan. The hypothetical
  standard-plan hourly budget finding was declined; no schedule/infrastructure
  change is included. The independent status-count cap is covered above.
- No existing assertion, timeout or error handling was weakened. The former S2
  shared-head assertion now requires no manual write when the fresh inventory
  confirms the active bot owner, strengthening the accepted isolation contract.

The first round-4 final-check run exposed formatter-version differences and one
transient parser timeout at the existing 15-second deadline. The timeout failed
closed as designed. No deadline or assertion was changed; the fresh final rerun
records the result. Focused lint uses isolated configuration to avoid importing
unrelated application-wide rules into the trusted Python 3.11 adapter.
