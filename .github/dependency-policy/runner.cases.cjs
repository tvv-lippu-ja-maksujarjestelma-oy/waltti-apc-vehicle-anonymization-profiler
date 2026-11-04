/* CommonJS is required by the trusted github-script loader. */
/* eslint-disable @typescript-eslint/no-require-imports */
const test = require("node:test");
const assert = require("node:assert/strict");
const run = require("./run.cjs");
const activation = require("./activation.json");
const { repositoryFixture } = require("./python-fixtures.cjs");
const repository = require("./repository.json");

function mock({
  held = false,
  state = "APPROVED",
  head = "head",
  ci = "success",
  missing = false,
  changed = false,
  fixture,
  encoding = "base64",
  cached = [],
  active = [],
} = {}) {
  const pr = {
    number: 1,
    state: "open",
    user: { login: "dependabot[bot]" },
    base: { ref: repository.baseBranch, sha: "base" },
    head: { sha: head },
    labels: held ? [{ name: "hold" }] : [],
    draft: false,
    mergeable: true,
    changed_files: fixture ? 2 : 1,
  };
  const written = [];
  const methods = {
    get: async () => ({
      data: changed ? { ...pr, head: { sha: Math.random().toString() } } : pr,
    }),
    list: () => {},
    listFiles: () => {},
    listCommits: () => {},
    listReviews: () => {},
    listForRef: () => {},
    listCommitStatusesForRef: () => {},
    getContent: async ({ ref, path }) => ({
      data: {
        encoding,
        content: Buffer.from(
          (ref === "base" ? fixture.before : fixture.after)[
            path === "pyproject.toml" ? 0 : 1
          ],
        ).toString("base64"),
      },
    }),
    getCombinedStatusForRef: async () => ({ data: { statuses: [] } }),
    createCommitStatus: async (s) => {
      written.push(s);
      cached.unshift({
        ...s,
        creator: { login: "github-actions[bot]" },
      });
      return { data: cached[0] };
    },
  };
  const github = {
    rest: { pulls: methods, repos: methods, checks: methods },
    paginate: async (fn) => {
      if (fn === methods.listCommitStatusesForRef) return cached.slice();
      if (fn === methods.list) return active;
      if (fn === methods.listFiles && fixture) return fixture.files;
      if (fn === methods.listFiles)
        return [{ filename: "Dockerfile", status: "modified" }];
      if (fn === methods.listCommits) return fixture?.commits || [];
      if (fn === methods.listReviews)
        return [
          { user: { login: "pnupu", type: "User" }, state, commit_id: "head" },
        ];
      if (fn === methods.listForRef)
        return missing
          ? []
          : repository.checks.map((name) => ({
              name,
              status: "completed",
              conclusion: ci,
            }));
      throw Error("Unexpected pagination");
    },
  };
  return {
    github,
    context: {
      repo: { owner: "owner", repo: "repo" },
      payload: { pull_request: pr },
    },
    core: { info: () => {}, warning: () => {}, setFailed: () => {} },
    written,
  };
}
const final = (m) =>
  m.written.filter((s) => s.context === "Dependency policy").at(-1).state;
test("actual runner fails closed while activation is disabled", async () => {
  const m = mock();
  await run(m);
  assert.equal(final(m), "failure");
  assert.match(m.written.at(-1).description, /Activation blocked/);
});
test("actual HUMAN runner checks current pnupu approval, CI and mutable state", async () => {
  activation.enabled = true;
  try {
    const good = mock();
    await run(good);
    assert.equal(final(good), "success");
    for (const options of [
      { held: true },
      { state: "DISMISSED" },
      { head: "new" },
      { ci: "failure" },
      { ci: "pending" },
      { missing: true },
      { changed: true },
    ]) {
      const m = mock(options);
      await run(m);
      assert.equal(final(m), "failure", JSON.stringify(options));
    }
  } finally {
    activation.enabled = false;
  }
});
test("API errors revoke cached success on the known candidate head", async () => {
  const cached = [
    {
      context: "Dependency policy",
      state: "success",
      creator: { login: "github-actions[bot]" },
    },
  ];
  const m = mock({ cached });
  m.github.rest.pulls.get = async () => {
    throw Error("unavailable");
  };
  await run(m);
  assert.equal(final(m), "failure");
  assert.equal(
    cached.find((s) => s.context === "Dependency policy").state,
    "failure",
  );
});
test("actual AUTO runner decodes both repository-shaped Python manifests/locks", async () => {
  activation.enabled = true;
  try {
    const m = mock({ fixture: repositoryFixture() });
    await run(m);
    assert.equal(final(m), "success");
    assert.equal(
      m.written.find((s) => s.context === "Dependency policy AUTO").state,
      "success",
    );
    const oversized = mock({ fixture: repositoryFixture(), encoding: "none" });
    await run(oversized);
    assert.equal(final(oversized), "failure");
  } finally {
    activation.enabled = false;
  }
});
test("unchanged reconciliation publishes no duplicate statuses", async () => {
  const m = mock();
  await run(m);
  const count = m.written.length;
  await run(m);
  assert.equal(m.written.length, count);
});
test("Python AUTO payload traverses runner blob and metadata checks", async () => {
  activation.enabled = true;
  try {
    const fixture = repositoryFixture();
    const m = mock({ fixture, head: fixture.head });
    await run(m);
    assert.equal(final(m), "success");
    assert.equal(
      m.written.find((s) => s.context === "Dependency policy AUTO").state,
      "success",
    );
  } finally {
    activation.enabled = false;
  }
});
test("foreign publisher cannot suppress authoritative AUTO failure", async () => {
  const cached = [
    {
      context: "Dependency policy AUTO",
      state: "success",
      description: "Immutable diff classification; never authorizes HUMAN",
      creator: { login: "foreign-app[bot]" },
    },
  ];
  const m = mock({ cached });
  await run(m);
  assert.equal(
    m.written.find((s) => s.context === "Dependency policy AUTO").state,
    "failure",
  );
});
test("failure on one PR does not stop reconciliation of later PRs", async () => {
  const m = mock();
  const first = m.context.payload.pull_request;
  const second = { ...first, number: 2 };
  m.context.payload = {};
  const paginate = m.github.paginate;
  m.github.paginate = async (fn, args) =>
    fn === m.github.rest.pulls.list ? [first, second] : paginate(fn, args);
  m.github.rest.pulls.get = async ({ pull_number }) => {
    if (pull_number === 1) throw Error("first unavailable");
    return { data: second };
  };
  await run(m);
  assert.ok(m.written.some((s) => s.description.startsWith("Fail closed")));
  assert.ok(
    m.written.some((s) => s.description.startsWith("Activation blocked")),
  );
});
test("failed policy publication revokes a newly written AUTO result", async () => {
  activation.enabled = true;
  try {
    const cached = [
      {
        context: "Dependency policy AUTO",
        state: "failure",
        description: "Fail closed: write failed",
        creator: { login: "github-actions[bot]" },
      },
    ];
    const m = mock({ fixture: repositoryFixture(), cached });
    const write = m.github.rest.repos.createCommitStatus;
    m.github.rest.repos.createCommitStatus = async (s) => {
      if (s.context === "Dependency policy" && s.state === "success")
        throw Error("write failed");
      return write(s);
    };
    await run(m);
    assert.equal(
      cached.find((s) => s.context === "Dependency policy AUTO").state,
      "failure",
    );
  } finally {
    activation.enabled = false;
  }
});
/* eslint-enable @typescript-eslint/no-require-imports */

for (const option of [
  "fresh",
  "stale",
  "error",
  "closed",
  "target",
  "author",
]) {
  test(`S1 ordinary PR required status: ${option}`, async () => {
    const m = mock();
    m.context.payload.pull_request.user = { login: "human" };
    const get = m.github.rest.pulls.get;
    let calls = 0;
    m.github.rest.pulls.get = async (...args) => {
      if (option === "error") throw Error("API unavailable");
      const { data } = await get(...args);
      const fresh = structuredClone(data);
      if (++calls > 1) {
        if (option === "stale") fresh.head.sha = "new";
        if (option === "closed") fresh.state = "closed";
        if (option === "target") fresh.base.ref = "wrong";
        if (option === "author") fresh.user.login = "dependabot[bot]";
      }
      return { data: fresh };
    };
    // Any attempt to classify, read PR code or retrieve dependency CI is a failure.
    m.github.paginate = async (fn, args) => {
      if (fn === m.github.rest.pulls.list) {
        assert.equal(args.state, "open");
        assert.equal(args.base, repository.baseBranch);
        assert.equal(args.per_page, 100);
        return [];
      }
      assert.equal(fn, m.github.rest.repos.listCommitStatusesForRef);
      return [];
    };
    await run(m);
    assert.equal(final(m), option === "fresh" ? "success" : "failure");
    assert.equal(
      m.written.some((s) =>
        ["Dependency policy AUTO", "Dependency policy Dependabot"].includes(
          s.context,
        ),
      ),
      false,
      "ordinary PRs must not write SHA-scoped bot authorization contexts",
    );
  });
}

const latest = (cached, name) =>
  cached.find(
    (s) => s.context === name && s.creator?.login === "github-actions[bot]",
  )?.state;
test("S2 same-SHA ordinary fork cannot override stale-review Dependabot readiness", async () => {
  activation.enabled = true;
  try {
    const cached = [];
    const fixture = structuredClone(require("./python-pr-69.fixture.json"));
    assert.equal(fixture.commits[0].commit.verification.verified, true);
    const bot = mock({ fixture, head: fixture.head, cached });
    // GitHub still retains pnupu APPROVED on the old head. Live Mergify's
    // approved-reviews-by would match it, but the trusted exact-head gate must not.
    await run(bot);
    assert.equal(latest(cached, "Dependency policy"), "failure");
    assert.equal(latest(cached, "Dependency policy AUTO"), "failure");
    assert.equal(latest(cached, "Dependency policy Dependabot"), "failure");
    const manual = mock({
      head: fixture.head,
      cached,
      active: [bot.context.payload.pull_request],
    });
    const manualGet = manual.github.rest.pulls.get;
    manual.github.rest.pulls.get = async (args) =>
      args.pull_number === 1
        ? { data: bot.context.payload.pull_request }
        : manualGet(args);
    manual.context.payload.pull_request.number = 2;
    manual.context.payload.pull_request.user = { login: "fork-attacker" };
    manual.context.payload.pull_request.head.repo = {
      fork: true,
      full_name: "fork-attacker/repo",
    };
    await run(manual);
    assert.equal(
      latest(cached, "Dependency policy"),
      "failure",
      "active same-head bot owns general status; ordinary path does not overwrite",
    );
    assert.equal(latest(cached, "Dependency policy AUTO"), "failure");
    assert.equal(
      latest(cached, "Dependency policy Dependabot"),
      "failure",
      "bot merge remains vetoed despite general success and retained old review",
    );
    assert.deepEqual(
      manual.written.map((s) => s.context),
      [],
    );
    assert.equal(
      bot.context.payload.pull_request.head.sha,
      manual.context.payload.pull_request.head.sha,
    );
  } finally {
    activation.enabled = false;
  }
});
for (const stage of [
  "initial GET",
  "CI API",
  "readiness publication",
  "general publication",
]) {
  test(`S2 bot-only cached success revoked after ${stage} failure`, async () => {
    activation.enabled = true;
    try {
      const cached = [
        "Dependency policy",
        "Dependency policy AUTO",
        "Dependency policy Dependabot",
      ].map((context) => ({
        context,
        state: "success",
        creator: { login: "github-actions[bot]" },
      }));
      const m = mock({ fixture: repositoryFixture(), cached });
      if (stage === "initial GET")
        m.github.rest.pulls.get = async () => {
          throw Error("unavailable");
        };
      if (stage === "CI API")
        m.github.rest.repos.getCombinedStatusForRef = async () => {
          throw Error("unavailable");
        };
      if (stage.endsWith("publication")) {
        const write = m.github.rest.repos.createCommitStatus;
        const context =
          stage === "readiness publication"
            ? "Dependency policy Dependabot"
            : "Dependency policy";
        m.github.rest.repos.createCommitStatus = async (s) => {
          if (s.context === context && s.state === "success")
            throw Error("write failed");
          return write(s);
        };
      }
      await run(m);
      for (const context of [
        "Dependency policy",
        "Dependency policy AUTO",
        "Dependency policy Dependabot",
      ])
        assert.equal(latest(cached, context), "failure", context);
    } finally {
      activation.enabled = false;
    }
  });
}
test("S2 ordinary API failure cannot revoke another PR's bot-only readiness", async () => {
  const cached = ["Dependency policy AUTO", "Dependency policy Dependabot"].map(
    (context) => ({
      context,
      state: "success",
      creator: { login: "github-actions[bot]" },
    }),
  );
  const m = mock({ cached });
  m.context.payload.pull_request.user = { login: "human" };
  m.github.rest.pulls.get = async () => {
    throw Error("unavailable");
  };
  await run(m);
  assert.equal(final(m), "failure");
  assert.equal(latest(cached, "Dependency policy AUTO"), "success");
  assert.equal(latest(cached, "Dependency policy Dependabot"), "success");
  assert.deepEqual(
    m.written.map((s) => s.context),
    ["Dependency policy"],
  );
});
test("S2 Mergify requires publisher-qualified bot readiness at entry and final merge", () => {
  const yaml = require("node:fs").readFileSync(".github/mergify.yml", "utf8");
  assert.match(
    yaml,
    /check-success=@github-actions\/Dependency policy Dependabot/,
  );
  assert.match(yaml, /queue_conditions: &eligibility/);
  assert.match(yaml, /merge_conditions: \*eligibility/);
  assert.match(yaml, /conditions: \*eligibility/);
});

for (const initiallyEnabled of [false, true])
  test(`round 4: 500 same-SHA scans with activation=${initiallyEnabled} stay below the status cap and recover on current-head approval`, async (t) => {
    activation.enabled = initiallyEnabled;
    try {
      const cached = [];
      const m = mock({ head: "shared-sha", cached });
      const bot = m.context.payload.pull_request;
      const manual = structuredClone(bot);
      manual.number = 2;
      manual.user = { login: "fork-attacker" };
      manual.head.repo = { fork: true, full_name: "fork-attacker/repo" };
      m.context.payload = {};
      let scan = 0,
        approved = false;
      const failures = [],
        counts = new Map();
      m.core.setFailed = (message) => failures.push(message);
      const paginate = m.github.paginate;
      m.github.paginate = async (fn, args) => {
        if (fn === m.github.rest.pulls.list) {
          assert.equal(args.state, "open");
          assert.equal(args.base, repository.baseBranch);
          return scan % 2 ? [manual, bot] : [bot, manual];
        }
        if (fn === m.github.rest.pulls.listReviews)
          return [
            {
              user: { login: "pnupu", type: "User" },
              state: "APPROVED",
              commit_id: approved ? bot.head.sha : "previous-head",
            },
          ];
        return paginate(fn, args);
      };
      m.github.rest.pulls.get = async ({ pull_number }) => ({
        data: structuredClone(pull_number === 1 ? bot : manual),
      });
      const write = m.github.rest.repos.createCommitStatus;
      m.github.rest.repos.createCommitStatus = async (status) => {
        const key = `${status.sha}/${status.context}`;
        const count = counts.get(key) || 0;
        if (count >= 1000)
          throw Error("422: 1000 statuses per SHA/context limit");
        const response = await write(status);
        counts.set(key, count + 1);
        return response;
      };
      for (; scan < 500; scan++) await run(m);
      assert.equal(
        m.written.length,
        3,
        "stable reconciliation must not alternate statuses",
      );
      assert.equal(latest(cached, "Dependency policy"), "failure");
      assert.equal(latest(cached, "Dependency policy Dependabot"), "failure");
      approved = true;
      activation.enabled = true;
      await run(m);
      assert.equal(latest(cached, "Dependency policy"), "success");
      assert.equal(latest(cached, "Dependency policy Dependabot"), "success");
      assert.equal(
        latest(cached, "Dependency policy AUTO"),
        "failure",
        "HUMAN never gains AUTO",
      );
      assert.equal(
        m.written.length,
        5,
        "approval changes only readiness and general status",
      );
      for (let i = 0; i < 5; i++) await run(m);
      assert.equal(
        m.written.length,
        5,
        "approved same-head reconciliation is also stable",
      );
      assert.deepEqual(failures, []);
      assert.ok([...counts.values()].every((n) => n < 1000));
      t.diagnostic(
        JSON.stringify({
          initiallyEnabled,
          scans: 500,
          writesBeforeApproval: 3,
          totalWritesAfterRecovery: 5,
          perSHAContext: Object.fromEntries(counts),
        }),
      );
    } finally {
      activation.enabled = false;
    }
  });
for (const change of ["closed", "new head", "new target", "different author"]) {
  test(`round 4: fresh ${change} releases stale bot ownership for ordinary status`, async () => {
    const bot = mock().context.payload.pull_request;
    const advertised = structuredClone(bot);
    const cached = [
      "Dependency policy",
      "Dependency policy Dependabot",
      "Dependency policy AUTO",
    ].map((context) => ({
      context,
      state: "failure",
      creator: { login: "github-actions[bot]" },
    }));
    const m = mock({ active: [advertised], cached });
    m.context.payload.pull_request.number = 2;
    m.context.payload.pull_request.user = { login: "human" };
    const get = m.github.rest.pulls.get;
    m.github.rest.pulls.get = async (args) =>
      args.pull_number === 1 ? { data: structuredClone(bot) } : get(args);
    await run(m);
    assert.deepEqual(m.written, [], "active bot owns shared head");
    if (change === "closed") bot.state = "closed";
    if (change === "new head") bot.head.sha = "new-head";
    if (change === "new target") bot.base.ref = "other-target";
    if (change === "different author") bot.user.login = "human";
    await run(m);
    assert.equal(
      latest(cached, "Dependency policy"),
      "success",
      "historical bot status must not deadlock manual PR",
    );
    assert.deepEqual(
      m.written.map((s) => s.context),
      ["Dependency policy"],
    );
    assert.equal(latest(cached, "Dependency policy Dependabot"), "failure");
    assert.equal(latest(cached, "Dependency policy AUTO"), "failure");
  });
}
for (const stage of ["inventory", "owner GET"]) {
  test(`round 4: ${stage} API error fails closed without touching bot-only statuses`, async () => {
    const bot = mock().context.payload.pull_request;
    const cached = [
      "Dependency policy AUTO",
      "Dependency policy Dependabot",
    ].map((context) => ({
      context,
      state: "success",
      creator: { login: "github-actions[bot]" },
    }));
    const m = mock({ active: [bot], cached });
    m.context.payload.pull_request.number = 2;
    m.context.payload.pull_request.user = { login: "human" };
    const get = m.github.rest.pulls.get,
      paginate = m.github.paginate;
    if (stage === "inventory")
      m.github.paginate = async (fn, args) => {
        if (fn === m.github.rest.pulls.list)
          throw Error("inventory unavailable");
        return paginate(fn, args);
      };
    else
      m.github.rest.pulls.get = async (args) => {
        if (args.pull_number === 1) throw Error("owner unavailable");
        return get(args);
      };
    await run(m);
    assert.equal(final(m), "failure");
    assert.equal(latest(cached, "Dependency policy AUTO"), "success");
    assert.equal(latest(cached, "Dependency policy Dependabot"), "success");
    assert.deepEqual(
      m.written.map((s) => s.context),
      ["Dependency policy"],
    );
  });
}
