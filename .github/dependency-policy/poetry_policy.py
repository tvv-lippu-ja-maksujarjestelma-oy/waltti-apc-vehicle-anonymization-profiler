"""Parse immutable TOML data with Python >=3.11, never load project code.

Deliberately supports Poetry 1.x lock 2.0 only. Poetry 2.1/PEP 621 and
non-Poetry pip manifests fail closed until separately audited.
"""

import copy
import hashlib
import json
import re
import sys
import tomllib

DEVELOPMENT = {"pytest", "pytest-mock"}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def canonical(name):
    require(isinstance(name, str), "Invalid package name")
    return re.sub(r"[-_.]+", "-", name).lower()


def indexed(deps):
    result = {}
    for name, value in deps.items():
        key = canonical(name)
        require(key not in result, "Ambiguous normalized dependency")
        result[key] = value
    return result


def content_hash(manifest):
    # Exact Poetry 1.6.1 Locker._get_content_hash algorithm, including null
    # legacy keys. This is not a hash of the TOML serialization.
    poetry = manifest["tool"]["poetry"]
    keys = ["dependencies", "source", "extras", "dev-dependencies"]
    content = {key: poetry.get(key) for key in keys}
    if "group" in poetry:
        content["group"] = poetry["group"]
    return hashlib.sha256(
        json.dumps(content, sort_keys=True).encode()
    ).hexdigest()


def version(value):
    require(
        isinstance(value, str)
        and re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", value),
        "Unsupported PEP 440 version",
    )
    return tuple(map(int, value.split(".")))


def bump(old, new):
    a, b = version(old), version(new)
    require(b > a, "Non-increasing version")
    return (
        "major"
        if a[0] != b[0]
        else (
            "breaking"
            if a[0] == 0 and a[1] != b[1]
            else "minor"
            if a[1] != b[1]
            else "patch"
        )
    )


def range_version(value):
    require(isinstance(value, str), "Complex dependency constraint")
    match = re.fullmatch(r"(\^|~|==)?(\d+\.\d+\.\d+)", value)
    require(match is not None, "Unsupported dependency constraint")
    version(match[2])
    return match[1], match[2]


def strip(obj, keys):
    return {key: value for key, value in obj.items() if key not in keys}


def packages(lock):
    result = {}
    for package in lock["package"]:
        name = canonical(package["name"])
        result.setdefault(name, []).append(package)
    return result


def pure_files(package):
    name, value = package["name"], package["version"]
    stem = canonical(name).replace("-", "_")
    expected = {
        f"{stem}-{value}-py3-none-any.whl",
        f"{stem}-{value}.tar.gz",
        f"{canonical(name)}-{value}.tar.gz",
    }
    files = package["files"]
    require(isinstance(files, list) and files, "Missing distribution hashes")
    seen = set()
    for entry in files:
        require(
            set(entry) == {"file", "hash"}, "Unknown distribution metadata"
        )
        require(
            entry["file"] in expected and entry["file"] not in seen,
            "Native/unknown or duplicate distribution",
        )
        require(
            re.fullmatch(r"sha256:[0-9a-f]{64}", entry["hash"]) is not None,
            "Missing/invalid distribution hash",
        )
        seen.add(entry["file"])


def classify(data):
    before = [tomllib.loads(text) for text in data["before"]]
    after = [tomllib.loads(text) for text in data["after"]]
    require(len(before) == len(after) == 2, "Missing manifest/lock")
    old, old_lock = before
    new, new_lock = after
    for manifest, lock in (before, after):
        require("project" not in manifest, "PEP 621 not audited")
        require(
            lock["metadata"]["lock-version"] == "2.0",
            "Unsupported lock format",
        )
        require(
            lock["metadata"]["content-hash"] == content_hash(manifest),
            "Manifest/lock content hash mismatch",
        )
    require(
        strip(old_lock, ["package", "metadata"])
        == strip(new_lock, ["package", "metadata"]),
        "Uncovered lock metadata",
    )
    require(
        strip(old_lock["metadata"], ["content-hash"])
        == strip(new_lock["metadata"], ["content-hash"]),
        "Uncovered lock metadata",
    )
    # Only dev dependency values can change. Source/group membership, scripts,
    # build-system, Python version and all other manifest data must be
    # identical.
    manifests = [copy.deepcopy(old), copy.deepcopy(new)]
    deps = []
    for manifest in manifests:
        poetry = manifest["tool"]["poetry"]
        require(
            "dev-dependencies" not in poetry, "Legacy dev group not audited"
        )
        deps.append(indexed(poetry["group"]["dev"].pop("dependencies")))
    require(manifests[0] == manifests[1], "Non-dev-version manifest delta")
    a, b = deps
    require(a.keys() == b.keys(), "Added/removed direct dependency")
    old_packages, new_packages = packages(old_lock), packages(new_lock)
    require(
        old_packages.keys() == new_packages.keys(),
        "Added/removed locked dependency",
    )
    changed = {name for name in a if a[name] != b[name]}
    locked = {
        name
        for name in old_packages
        if old_packages[name] != new_packages[name]
    }
    # Lock-only updates are meaningful when the exact same simple constraint
    # permits both versions; every changed lock still needs signed direct
    # metadata.
    covered = changed | locked
    require(
        covered and covered <= DEVELOPMENT,
        "Unknown/production/transitive dependency",
    )
    require(
        covered <= a.keys() and locked == covered,
        "Uncovered manifest or transitive delta",
    )
    meta = data["meta"]
    names = [canonical(item["dependency-name"]) for item in meta]
    require(
        len(names) == len(set(names)) and set(names) == covered,
        "Missing/extra/duplicate metadata",
    )
    groups = {item.get("dependency-group") for item in meta}
    require(len(groups) == 1, "Inconsistent dependency groups")
    # Poetry 2.0 locks have no group membership. Compute production
    # reachability
    # conservatively from ALL dependency edges (including optional/marked
    # ones).
    for manifest, table in ((old, old_packages), (new, new_packages)):
        poetry = manifest["tool"]["poetry"]
        roots = set(indexed(poetry["dependencies"])) - {"python"}
        for group, body in poetry.get("group", {}).items():
            if group != "dev":
                roots.update(indexed(body.get("dependencies", {})))
        seen = set()
        while roots:
            name = roots.pop()
            if name in seen:
                continue
            seen.add(name)
            require(name in table, "Incomplete dependency graph")
            for package in table[name]:
                roots.update(indexed(package.get("dependencies", {})))
        require(
            not covered & seen,
            "Dev dependency reachable from production/other group",
        )
    for name in covered:
        require(
            len(old_packages[name]) == len(new_packages[name]) == 1,
            "Changed duplicate locked dependency",
        )
        p, q = old_packages[name][0], new_packages[name][0]
        require(
            strip(p, ["version", "files"]) == strip(q, ["version", "files"]),
            "Non-version package metadata delta",
        )
        require(
            "source" not in p
            and "source" not in q
            and p.get("optional") is False
            and q.get("optional") is False,
            "Source/optional package not audited",
        )
        pure_files(p)
        pure_files(q)
        level = bump(p["version"], q["version"])
        require(level in {"patch", "minor"}, "Breaking/major dependency")
        old_prefix, old_min = range_version(a[name])
        new_prefix, new_min = range_version(b[name])
        require(old_prefix == new_prefix, "Changed constraint type")
        if name in changed:
            require(
                old_min == p["version"] and new_min == q["version"],
                "Manifest/lock version mismatch",
            )
        else:
            v = version(old_min)
            # Small audited subset of Poetry's range semantics.
            for value in (p["version"], q["version"]):
                target = version(value)
                require(
                    target >= v
                    and (
                        old_prefix == "^"
                        and v[0] > 0
                        and target[0] == v[0]
                        or old_prefix == "~"
                        and target[:2] == v[:2]
                        or old_prefix in {None, "=="}
                        and target == v
                    ),
                    "Locked version outside audited constraint",
                )
        item = meta[names.index(name)]
        require(
            item.get("dependency-type") == "direct:development"
            and item.get("update-type") == f"version-update:semver-{level}"
            and (
                "dependency-version" not in item
                or item["dependency-version"] == q["version"]
            ),
            "Inconsistent signed metadata",
        )
    return {
        "lane": "AUTO",
        "reason": "Audited Python dev patch/minor; complete lock "
        "delta covered",
    }


if __name__ == "__main__":
    try:
        result = classify(json.load(sys.stdin))
    except Exception as error:
        result = {"lane": "HUMAN", "reason": f"Poetry fail closed: {error}"}
    print(json.dumps(result))
