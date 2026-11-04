const fs = require("node:fs");
const { spawnSync } = require("node:child_process");
const { join } = require("node:path");
// Replay a narrow dev-tool patch on THIS repository's complete real lock shape.
// No unrelated entries or hashes are projected away from current-repo coverage.
function repositoryFixture() {
  const f = structuredClone(require("./python-pr-1.fixture.json"));
  f.head = "head";
  f.commits[0].sha = "head";
  f.files = ["pyproject.toml", "poetry.lock"].map((filename) => ({
    filename,
    status: "modified",
  }));
  f.before = ["pyproject.toml", "poetry.lock"].map((path) =>
    fs.readFileSync(path, "utf8"),
  );
  f.after = [...f.before];
  const entry = f.before[1].match(
    /\[\[package\]\]\nname = "pytest-mock"\n[\s\S]*?(?=\n\[\[package\]\]|\n\[metadata\])/,
  )[0];
  const old = entry.match(/version = "([^"]+)"/)[1];
  const next = old.replace(/\d+$/, (n) => String(+n + 1));
  f.after[0] = f.after[0].replace(
    new RegExp(`(pytest-mock = "\\^)${old.replaceAll(".", "\\.")}("\\n)`),
    `$1${next}$2`,
  );
  f.after[1] = f.after[1].replace(entry, entry.replaceAll(old, next));
  const hash = spawnSync(
    "python3",
    [
      "-I",
      "-c",
      `import sys,runpy,tomllib; m=runpy.run_path(sys.argv[1]); print(m['content_hash'](tomllib.loads(sys.stdin.read())))`,
      join(__dirname, "poetry_policy.py"),
    ],
    { input: f.after[0], encoding: "utf8" },
  );
  if (hash.status !== 0) throw Error(hash.stderr);
  f.after[1] = f.after[1].replace(
    /content-hash = "[a-f0-9]+"/,
    `content-hash = "${hash.stdout.trim()}"`,
  );
  f.commits[0].commit.message = `---\nupdated-dependencies:\n- dependency-name: pytest-mock\n  dependency-type: direct:development\n  update-type: version-update:semver-patch\n  dependency-group: pytest\n...\n`;
  return f;
}
module.exports = { repositoryFixture };
