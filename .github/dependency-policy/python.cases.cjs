const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const { classify } = require("./python-policy.cjs");
const { eligible } = require("./policy.cjs");
const { repositoryFixture } = require("./python-fixtures.cjs");
const repository = require("./repository.json");

test("Python dev patch uses complete current repository TOML and Poetry lock", () => {
  const f = repositoryFixture();
  assert.equal(classify(f).lane, "AUTO");
  assert.equal(f.before[0], fs.readFileSync("pyproject.toml", "utf8"));
  assert.equal(f.before[1], fs.readFileSync("poetry.lock", "utf8"));
});
for (const n of [1, 69])
  test(`authentic signed Python PR ${n}: uncovered native hashes or major/transitive changes remain HUMAN`, () => {
    const f = require(`./python-pr-${n}.fixture.json`);
    assert.equal(f.commits[0].commit.verification.verified, true);
    assert.equal(classify(f).lane, "HUMAN");
  });
for (const [name, change] of [
  ["unsigned", (f) => (f.commits[0].commit.verification.verified = false)],
  ["foreign author", (f) => (f.commits[0].author.login = "human")],
  ["foreign committer", (f) => (f.commits[0].committer.login = "human")],
  ["new head", (f) => (f.head = "new")],
  ["extra commit", (f) => f.commits.push(f.commits[0])],
  [
    "unsigned body only",
    (f) => (f.commits[0].commit.message = "Bump pytest-mock"),
  ],
  [
    "inconsistent version",
    (f) =>
      (f.commits[0].commit.message = f.commits[0].commit.message.replace(
        "  dependency-type",
        "  dependency-version: 90.0.0\n  dependency-type",
      )),
  ],
  [
    "inconsistent update class",
    (f) =>
      (f.commits[0].commit.message = f.commits[0].commit.message.replace(
        "semver-patch",
        "semver-minor",
      )),
  ],
  [
    "production metadata",
    (f) =>
      (f.commits[0].commit.message = f.commits[0].commit.message.replace(
        "direct:development",
        "direct:production",
      )),
  ],
  [
    "extra metadata",
    (f) =>
      (f.commits[0].commit.message = f.commits[0].commit.message.replace(
        "...",
        "- dependency-name: unknown\n  dependency-group: other\n...",
      )),
  ],
  [
    "Actions",
    (f) =>
      f.files.push({
        filename: ".github/workflows/ci-cd.yml",
        status: "modified",
      }),
  ],
  [
    "pip requirements",
    (f) => f.files.push({ filename: "requirements.txt", status: "modified" }),
  ],
  ["deleted lock", (f) => (f.files[1].status = "removed")],
  ["malformed TOML", (f) => (f.after[0] += "\n[")],
  [
    "lock format 2.1",
    (f) =>
      (f.after[1] = f.after[1].replace(
        'lock-version = "2.0"',
        'lock-version = "2.1"',
      )),
  ],
  [
    "Python runtime delta",
    (f) =>
      (f.after[1] = f.after[1].replace(
        'python-versions = "^3.10"',
        'python-versions = "^3.11"',
      )),
  ],
  [
    "lock content hash",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /content-hash = "[a-f0-9]+"/,
        'content-hash = "bad"',
      )),
  ],
  [
    "manifest build backend",
    (f) =>
      (f.after[0] = f.after[0].replace("poetry.core.masonry.api", "evil.api")),
  ],
  [
    "manifest scripts",
    (f) => (f.after[0] += '\n[tool.poetry.scripts]\nnew = "evil:run"\n'),
  ],
  [
    "native transitive hash-only",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /sha256:[a-f0-9]{64}/,
        "sha256:" + "a".repeat(64),
      )),
  ],
  ["unknown lock metadata", (f) => (f.after[1] += '\nunknown = "new"\n')],
  [
    "dev package source",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /(name = "pytest-mock"\n)/,
        '$1source = {type="url",url="https://evil.test"}\n',
      )),
  ],
  [
    "package metadata",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /(name = "pytest-mock"\n)/,
        "$1unknown = true\n",
      )),
  ],
  [
    "dev group membership",
    (f) =>
      (f.after[0] = f.after[0].replace(
        "group.dev.dependencies",
        "group.production.dependencies",
      )),
  ],
  [
    "uncovered dev dependency",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /(name = "ruff"\nversion = ")[^"]+/,
        "$19.0.0",
      )),
  ],
  [
    "duplicate TOML key",
    (f) => (f.after[1] += '\ncontent-hash = "duplicate"\n'),
  ],
  [
    "package dependency edge",
    (f) =>
      (f.after[1] = f.after[1].replace(
        '[package.dependencies]\npytest = ">=5.0"',
        '[package.dependencies]\npytest = ">=6.0"',
      )),
  ],
])
  test(`Python ${name} fails closed`, () => {
    const f = repositoryFixture();
    assert.equal(classify(f).lane, "AUTO", "positive control");
    change(f);
    assert.equal(classify(f).lane, "HUMAN");
  });

test("default branch and CI count are repository-specific, including a single required check", () => {
  const r = {
    activated: true,
    classification: { lane: "AUTO" },
    reviews: [],
    checks: ["success"],
    baseBranch: repository.baseBranch,
    requiredCheckCount: 1,
    pr: {
      state: "open",
      draft: false,
      mergeable: true,
      base: { ref: repository.baseBranch },
      user: { login: "dependabot[bot]" },
      labels: [],
      head: { sha: "head" },
    },
  };
  assert.equal(eligible(r), true);
  r.checks = [];
  assert.equal(eligible(r), false);
  r.checks = ["success"];
  r.pr.base.ref = "wrong";
  assert.equal(eligible(r), false);
  const yaml = fs.readFileSync(".github/mergify.yml", "utf8");
  assert.ok(yaml.includes(`base=${repository.baseBranch}`));
  for (const check of repository.checks)
    assert.ok(yaml.includes(`check-success=${check}`));
  for (const check of repository.protectedChecks)
    assert.ok(repository.checks.includes(check));
});

function isolatedHistoricalReplay() {
  const f = structuredClone(require("./python-pr-1.fixture.json"));
  // Historical PR includes unrelated native hash churn: restore unchanged
  // packages to isolate the authentic pytest-mock delta, including its hashes.
  // This is a replay fixture, never a claim the original PR qualifies.
  const blocks = (text) =>
    text.match(/\[\[package\]\][\s\S]*?(?=\n\[\[package\]\]|\n\[metadata\])/g);
  const old = new Map(
    blocks(f.before[1]).map((block) => [
      block.match(/name = "([^"]+)"/)[1],
      [],
    ]),
  );
  for (const block of blocks(f.before[1]))
    old.get(block.match(/name = "([^"]+)"/)[1]).push(block);
  const positions = new Map();
  for (const block of blocks(f.after[1])) {
    const name = block.match(/name = "([^"]+)"/)[1];
    const pos = positions.get(name) || 0;
    positions.set(name, pos + 1);
    if (name !== "pytest-mock")
      f.after[1] = f.after[1].replace(block, old.get(name)[pos]);
  }
  return f;
}
test("isolated real pytest-mock minor delta replay uses authentic distribution hashes and trailer", () => {
  assert.equal(classify(isolatedHistoricalReplay()).lane, "AUTO");
});
test("lock-only audited dev minor stays inside the real manifest constraint", () => {
  const f = isolatedHistoricalReplay();
  f.after[0] = f.before[0];
  f.after[1] = f.after[1].replace(
    /content-hash = "[a-f0-9]+"/,
    f.before[1].match(/content-hash = "[a-f0-9]+"/)[0],
  );
  f.files = [{ filename: "poetry.lock", status: "modified" }];
  assert.equal(classify(f).lane, "AUTO");
});
for (const [name, change] of [
  [
    "native wheel",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /pytest_mock-([^" ]+)-py3-none-any.whl/,
        "pytest_mock-$1-cp311-cp311-linux_x86_64.whl",
      )),
  ],
  [
    "missing hash",
    (f) =>
      (f.after[1] = f.after[1].replace(
        /(pytest_mock-[^" ]+"\s*, hash = ")sha256:[a-f0-9]+/,
        "$1invalid",
      )),
  ],
  [
    "production reachability",
    (f) => {
      for (const key of ["before", "after"])
        f[key][1] = f[key][1].replace(
          "[package.dependencies]",
          '[package.dependencies]\npytest-mock = "*"',
        );
    },
  ],
])
  test(`isolated real replay ${name} is HUMAN`, () => {
    const f = isolatedHistoricalReplay();
    assert.equal(classify(f).lane, "AUTO");
    change(f);
    assert.equal(classify(f).lane, "HUMAN");
  });
test("Python version boundaries reject majors, 0.x minors, PEP440 prereleases and downgrades", () => {
  const { spawnSync } = require("node:child_process");
  const { join } = require("node:path");
  const result = spawnSync(
    "python3",
    [
      "-I",
      "-c",
      `import sys,runpy
m=runpy.run_path(sys.argv[1]); bump=m['bump']
assert bump('1.0.0','2.0.0') == 'major'
assert bump('0.1.0','0.2.0') == 'breaking'
assert bump('0.1.0','0.1.1') == 'patch'
for a,b in [('1.0.0','1.0.0'),('1.0.0','0.9.0'),('1.0.0','1.0.1rc1')]:
 try: bump(a,b)
 except ValueError: pass
 else: raise AssertionError((a,b))`,
      join(__dirname, "poetry_policy.py"),
    ],
    { encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr);
});
