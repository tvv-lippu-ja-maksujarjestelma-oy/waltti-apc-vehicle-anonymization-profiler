/* CommonJS is required by the trusted github-script loader. */
/* eslint-disable @typescript-eslint/no-require-imports */
const { eligible, CONTEXT } = require("./policy.cjs");
const { classify } = require("./python-policy.cjs");
const repository = require("./repository.json");
const activation = require("./activation.json");
const PUBLISHER = "github-actions[bot]";
const DEPENDABOT_CONTEXT = `${CONTEXT} Dependabot`;

module.exports = async ({ github, context, core }) => {
  const repo = context.repo;
  const configured = activation.enabled;
  const pulls = context.payload.pull_request
    ? [context.payload.pull_request]
    : await github.paginate(github.rest.pulls.list, {
        ...repo,
        state: "open",
        base: repository.baseBranch,
        per_page: 100,
      });
  for (const candidate of pulls) {
    const number = candidate.number;
    let sha = candidate.head.sha;
    let cached;
    // GitHub webhook/list author identifies the known bot head for revocation
    // if the first fresh GET fails. A confirmed ordinary PR never owns bot statuses.
    let dependency = candidate.user.login === "dependabot[bot]";
    const publish = async (name, state, description) => {
      description = description.slice(0, 140);
      const previous = cached?.find(
        (s) => s.context === name && s.creator?.login === PUBLISHER,
      );
      if (previous?.state === state && previous.description === description)
        return;
      const { data: written } = await github.rest.repos.createCommitStatus({
        ...repo,
        sha,
        context: name,
        state,
        description,
      });
      cached ||= [];
      cached.unshift(written);
    };
    try {
      const { data: pr } = await github.rest.pulls.get({
        ...repo,
        pull_number: number,
      });
      sha = pr.head.sha;
      dependency = pr.user.login === "dependabot[bot]";
      cached = await github.paginate(
        github.rest.repos.listCommitStatusesForRef,
        { ...repo, ref: sha, per_page: 100 },
      );
      // Required policy must not deadlock ordinary human PRs (including adoption).
      // This status never grants AUTO or causes an enqueue/approval/merge.
      if (!dependency) {
        // SHA-scoped general status has one active owner. Do not let a fork
        // alternate manual success with the same-head bot's eligibility result.
        // Fresh API inventory + GET releases ownership when the bot closes,
        // changes head or changes target; cached historical markers cannot own it.
        const active = await github.paginate(github.rest.pulls.list, {
          ...repo,
          state: "open",
          base: repository.baseBranch,
          per_page: 100,
        });
        let botOwnsHead = false;
        for (const associated of active) {
          if (
            associated.user.login !== "dependabot[bot]" ||
            associated.head.sha !== sha
          )
            continue;
          const { data: owner } = await github.rest.pulls.get({
            ...repo,
            pull_number: associated.number,
          });
          if (
            owner.user.login === "dependabot[bot]" &&
            owner.state === "open" &&
            owner.base.ref === repository.baseBranch &&
            owner.head.sha === sha
          ) {
            botOwnsHead = true;
            break;
          }
        }
        if (botOwnsHead) {
          core.info(
            `PR ${number}: active same-head Dependabot PR owns general status`,
          );
          continue;
        }
        const { data: fresh } = await github.rest.pulls.get({
          ...repo,
          pull_number: number,
        });
        const ordinary =
          fresh.user.login === pr.user.login &&
          fresh.user.login !== "dependabot[bot]" &&
          fresh.state === "open" &&
          fresh.base.ref === repository.baseBranch &&
          pr.base.ref === repository.baseBranch &&
          fresh.head.sha === sha &&
          fresh.base.sha === pr.base.sha;
        // Statuses are SHA-scoped. A fork may reuse a Dependabot SHA, so
        // ordinary paths must never write either bot-only context.
        await publish(
          CONTEXT,
          ordinary ? "success" : "failure",
          ordinary
            ? "Ordinary PR; existing CI and explicit human merge approval still apply"
            : "Invalid or changed ordinary PR",
        );
        continue;
      }
      if (pr.state !== "open" || pr.base.ref !== repository.baseBranch)
        throw new Error("Invalid dependency PR state or target");
      const files = await github.paginate(github.rest.pulls.listFiles, {
        ...repo,
        pull_number: number,
        per_page: 100,
      });
      if (files.length !== pr.changed_files)
        throw new Error("Incomplete file inventory");
      const commits = await github.paginate(github.rest.pulls.listCommits, {
        ...repo,
        pull_number: number,
        per_page: 100,
      });
      const read = async (ref, path) => {
        const { data } = await github.rest.repos.getContent({
          ...repo,
          ref,
          path,
        });
        if (data.encoding !== "base64" || !data.content)
          throw new Error("Unsupported file content");
        return Buffer.from(data.content, "base64").toString("utf8");
      };
      let before, after;
      if (
        files.every((f) =>
          ["pyproject.toml", "poetry.lock"].includes(f.filename),
        )
      ) {
        // Compare to current base: base drift fails conservatively to HUMAN.
        [before, after] = await Promise.all(
          [pr.base.sha, pr.head.sha].map((ref) =>
            Promise.all(
              ["pyproject.toml", "poetry.lock"].map((path) => read(ref, path)),
            ),
          ),
        );
      }
      const classification = classify({
        files,
        before,
        after,
        commits,
        head: sha,
      });
      const reviews = await github.paginate(github.rest.pulls.listReviews, {
        ...repo,
        pull_number: number,
        per_page: 100,
      });
      const runs = await github.paginate(github.rest.checks.listForRef, {
        ...repo,
        ref: sha,
        per_page: 100,
      });
      const { data: combined } =
        await github.rest.repos.getCombinedStatusForRef({ ...repo, ref: sha });
      const checks = repository.checks.map((name) => {
        const run = runs.find((c) => c.name === name);
        const legacy = combined.statuses.find((c) => c.context === name);
        return run
          ? run.status === "completed"
            ? run.conclusion
            : "pending"
          : legacy?.state;
      });
      const { data: fresh } = await github.rest.pulls.get({
        ...repo,
        pull_number: number,
      });
      if (fresh.head.sha !== sha || fresh.base.sha !== pr.base.sha)
        throw new Error("Head/base changed during evaluation");
      const allowed = eligible({
        pr: fresh,
        classification,
        reviews,
        checks,
        activated: configured,
        baseBranch: repository.baseBranch,
        requiredCheckCount: repository.checks.length,
      });
      core.info(
        JSON.stringify({
          number,
          head: sha,
          ...classification,
          activated: configured,
          allowed,
        }),
      );
      await publish(
        `${CONTEXT} AUTO`,
        configured && classification.lane === "AUTO" ? "success" : "failure",
        "Immutable diff classification; never authorizes HUMAN",
      );
      await publish(
        DEPENDABOT_CONTEXT,
        allowed ? "success" : "failure",
        !configured
          ? "Activation blocked: complete required-status and token-permission adoption"
          : `${classification.lane}: ${classification.reason}`,
      );
      await publish(
        CONTEXT,
        allowed ? "success" : "failure",
        !configured
          ? "Activation blocked: complete required-status and token-permission adoption"
          : `${classification.lane}: ${classification.reason}`,
      );
    } catch (error) {
      // Revoke cached success on the known head even when the first GET fails.
      // A rejected write cannot be made fail-closed by repository code alone.
      core.warning(`PR ${number}: ${error.message}`);
      const revoke = dependency
        ? [`${CONTEXT} AUTO`, DEPENDABOT_CONTEXT, CONTEXT]
        : [CONTEXT];
      for (const name of revoke) {
        try {
          await publish(name, "failure", `Fail closed: ${error.message}`);
        } catch (writeError) {
          core.setFailed(
            `Cannot revoke PR ${number} ${name}: ${writeError.message}`,
          );
        }
      }
    }
  }
};
/* eslint-enable @typescript-eslint/no-require-imports */
