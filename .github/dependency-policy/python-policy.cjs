/* Python is a data parser only: no PR content is imported or executed. */
const { spawnSync } = require("node:child_process");
const { join } = require("node:path");
const { metadata } = require("./policy.cjs");
function classify({ files, before, after, commits, head }) {
  const human = (reason) => ({ lane: "HUMAN", reason });
  if (
    !files.length ||
    files.some(
      (f) =>
        !["pyproject.toml", "poetry.lock"].includes(f.filename) ||
        f.status !== "modified",
    )
  )
    return human("Uncovered file or ecosystem");
  if (
    commits.length !== 1 ||
    commits[0].sha !== head ||
    commits[0].author?.login !== "dependabot[bot]" ||
    commits[0].committer?.login !== "web-flow" ||
    !commits[0].commit?.verification?.verified
  )
    return human("Missing authenticated, current Dependabot metadata");
  const meta = metadata(commits[0].commit.message);
  if (!meta.length || !before || !after)
    return human("Missing metadata or manifests");
  // Fixed trusted program and bounded JSON stdin. Isolated Python ignores user
  // site and environment imports. Missing Python/tomllib fails closed.
  const result = spawnSync(
    "python3",
    ["-I", join(__dirname, "poetry_policy.py")],
    {
      input: JSON.stringify({ before, after, meta }),
      encoding: "utf8",
      timeout: 15000,
      maxBuffer: 8 * 1024 * 1024,
    },
  );
  if (result.error || result.status !== 0)
    return human(
      "Python >=3.11 with stdlib tomllib unavailable, or parser failed",
    );
  try {
    const answer = JSON.parse(result.stdout);
    return ["AUTO", "HUMAN"].includes(answer.lane) &&
      typeof answer.reason === "string"
      ? answer
      : human("Invalid parser response");
  } catch {
    return human("Invalid parser response");
  }
}
module.exports = { classify };
