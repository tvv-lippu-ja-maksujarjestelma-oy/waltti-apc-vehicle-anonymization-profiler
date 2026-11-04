# Dependency approval policy: Python Option A

Approver: **pnupu**. Repository default branch: **main**.
Source: http-pulsar-poller commit `f1422645c6fa930e615613227c4bb2e25b696423`.
Common approval, metadata and trusted evaluation semantics are imported through
Git. This repository's active classifier is `python-policy.cjs` plus
`poetry_policy.py`. The npm classifier and fixtures remain isolated regression
coverage for the shared source; npm catalogs never authorize Python updates.

## Disabled adoption

`activation.json` remains false. The trusted workflow always runs, including
ordinary and adoption PRs when no repository variable exists. The activation
file gates only dependency AUTO/readiness authorization. No host setting changes.
`Build` and `Build, check, test, push` are required by existing branch protection. The legacy Mergify `CodeQL` condition is also retained; CodeQL is not currently a required protection context.

Before activation, obtain separate authorization to require `Dependency policy`,
bound to the github-actions app, and preserve all stronger existing rules and
checks. Remove PR-executing status/check write privileges and bypass routes;
these jobs explicitly restrict tokens. Verify a live queued hold, dismissal,
new-head and failed-CI veto before enabling `activation.json`. For an unprotected
branch, protection and the policy/app binding must be adopted first; do not
interpret local eligibility or auto_merge=false as branch protection.
The exact retained queue checks and protected subset are in `repository.json`.
The evaluator uses its configured branch and exact check count; there is no
hard-coded main/two-check adoption assumption.

## Python AUTO boundary

Only `pytest` and `pytest-mock`, directly declared in
`tool.poetry.group.dev.dependencies`, qualify for patch/minor AUTO. Their role
is test execution/mocking, with pure Python wheel and sdist distributions.
Both Dockerfiles exclude dev dependencies from the production builder/runtime.
They neither generate shipped code nor supply runtime dependencies. No
production library is currently in the Python audited catalog. `black`, `ruff`,
build tools, Actions, Python/base images, tool-dependencies requirements, native,
privacy/security, scientific and runtime libraries remain HUMAN. In particular
pulsar-client, jax/jaxlib, numpy, pandas and psycopg2 are never automatic.

The trusted runner reads immutable API blobs. It never checks out, installs,
imports or executes PR code. A fixed trusted Python program parses TOML with
stdlib `tomllib`; Python >=3.11 is required on the trusted Ubuntu runner. A
missing interpreter/parser, timeout, invalid input or unknown format fails closed.
No Python dependency installation or new service is needed for evaluation.

The adapter audits Poetry 1.x lock format **2.0** and its exact Poetry 1.6.1
content-hash algorithm. Poetry 2.1 locks, PEP 621, legacy dev groups, complex
constraints and other pip manifests stay HUMAN pending a separate adapter audit.
This is narrower than the current Poetry installer version, intentionally.
Simple caret/tilde/exact constraints are checked against locked versions.
Every changed package, hash/file list, dependency edge and lock metadata is
compared. Additions/removals, changed transitives, unknown metadata, optional or
non-registry packages and native distributions require HUMAN. Unchanged
multi-version records are preserved in full and all their dependency edges
participate in reachability; changed duplicate records require HUMAN.
All production and non-dev group edges, including optional/marked edges, are
traversed conservatively. A test tool reachable through those edges is HUMAN.

Complete GitHub-authenticated single-head Dependabot metadata is required:
Dependabot author, web-flow committer and verified signature. The restricted
shared trailer grammar supports grouped entries. Actual pip trailers may omit
`dependency-version`; the adapter then verifies old/new versions from the locks,
and requires matching signed name/type/update class for every changed package.
A supplied version must match. Missing, duplicate, extra or mixed metadata fails
closed. No label, title or unsigned PR body authorizes AUTO. All majors,
pre-1.0 minors, unknown versions, mixed risk and security-sensitive updates are
HUMAN. Security urgency never bypasses the boundary.

## Approval and merge

HUMAN requires non-dismissed APPROVED review by User pnupu on the exact head.
Other humans and bots cannot substitute. A new head or dismissal invalidates
approval; any outstanding changes request blocks both dependency lanes.
Mergify is the sole configured automatic executor. Identical queue entry, final
merge and manual-queue conditions require open, conflict-free, non-draft state,
no help wanted/help-wanted/hold label, green retained CI, publisher-qualified
policy status, and either AUTO classification or live pnupu approval. The
single-PR queue cannot use label-only authorization or generic bot approval.
GitHub required checks remain additional constraints. No approval is created.

S1: ordinary non-Dependabot PRs receive the required general policy success only
after two fresh GitHub PR reads agree on author/head/base and confirm an open PR
targeting the configured branch. They never write AUTO or Dependabot readiness statuses. They never enter
the Dependabot queue or gain bot approval. Existing CI and Ilkka's explicit
manual merge authorization still apply. Invalid states and API errors fail
closed. This allows human adoption PRs without a required-status deadlock.

Evaluation uses trusted default-branch code, review signals without checkout or
write rights, and five-minute reconciliation. API failures revoke known-head
success. Head/base changes during evaluation block success. Live Mergify review
and hold vetoes complement asynchronous policy statuses; activation requires
real verification and cannot rely on optional statuses alone.

## Validation

Run `node --test .github/dependency-policy/*.cases.cjs` from the repository root.
The suite exercises the actual API runner, raw TOML decoding, current full
repository lock shapes, strict delta accounting, current-head approvals,
mutable-state vetoes, publisher isolation, disabled activation and S1.
See `docs/dependency-policy-validation.md` for evidence and explicit gaps.

References: [Poetry 1.6.1 content hash](https://github.com/python-poetry/poetry/blob/1.6.1/src/poetry/packages/locker.py),
[verified Dependabot commits](https://github.com/dependabot/fetch-metadata/blob/main/src/dependabot/verified_commits.ts),
[Mergify queues](https://docs.mergify.com/merge-queue/rules/).

## S2: SHA-scoped status collision isolation

GitHub statuses are SHA-scoped, not PR-scoped. An ordinary fork PR can reuse a
signed Dependabot SHA. The general required `Dependency policy` status therefore
cannot alone authorize a Dependabot merge. Mergify additionally requires
`@github-actions/Dependency policy Dependabot` at queue entry and final merge.
Only the GitHub-confirmed Dependabot path writes this readiness status, using
all existing eligibility checks and exact current-head pnupu approval. It also
revokes readiness on API or publication errors, including failure of the first
fresh GET on a GitHub webhook/list-confirmed bot head. Ordinary paths write only
the general status and never grant or revoke either bot-only authorization
context. A retained old pnupu review plus same-SHA manual general success cannot
replace bot readiness. The activation file and all CI/live vetoes remain.

## Active shared-head ownership and status capacity

The general status belongs to an active Dependabot PR when its current head is
also used by an ordinary/fork PR. Before an ordinary publication, the evaluator
paginates the authenticated open PR inventory for the configured default branch
and refetches matching bot PRs. An open bot PR still targeting that branch with
that exact head owns the status; the ordinary evaluation writes nothing.
Ownership is not inferred from cached statuses. Closing the bot PR, moving its
head or retargeting it releases ordinary status publication. The fresh manual
head/state checks and bot-only readiness separation remain unchanged.
This prevents stable same-head scans from alternating failure/success until
GitHub's 1000-status-per-SHA/context cap blocks later legitimate approval.
The actual runner regression performs 500 scans in both PR orders under that
cap, then verifies that pnupu current-head approval recovers the same SHA.
The existing five-minute schedule and prompt review/live vetoes are preserved.
