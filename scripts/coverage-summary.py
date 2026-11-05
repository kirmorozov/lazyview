"""Production line coverage from an lcov file (used by scripts/coverage.sh): src/ files only, minus inline
#[cfg(test)] modules and test-only files (#![cfg(test)]).
Usage: coverage-summary.py <cov.lcov> <repo root> <sha> <servers> <suite summary file>; writes summary.json beside the lcov."""
import json, os, re, sys

lcov, root, sha, flavors, suites = sys.argv[1:6]


def test_ranges(path):
    """Line ranges (1-based, inclusive) of #[cfg(test)] items, found by brace matching."""
    try:
        lines = open(path, encoding="utf-8").read().split("\n")
    except OSError:
        return [], False
    if any(l.strip().startswith("#![cfg(test)]") for l in lines[:40]):
        return [], True
    out, i = [], 0
    while i < len(lines):
        if lines[i].strip().startswith("#[cfg(test)]"):
            j, depth, opened = i, 0, False
            while j < len(lines):
                code = re.sub(r'"(\\.|[^"\\])*"|//.*', "", lines[j])
                depth += code.count("{") - code.count("}")
                opened = opened or "{" in code
                if opened and depth <= 0:
                    break
                if not opened and code.rstrip().endswith(";") and j > i:
                    break
                j += 1
            out.append((i + 1, j + 1))
            i = j + 1
        else:
            i += 1
    return out, False


files, cur = {}, None
for line in open(lcov):
    if line.startswith("SF:"):
        cur = line[3:].strip(); files.setdefault(cur, {})
    elif line.startswith("DA:") and cur:
        n, c = line[3:].split(",")[:2]
        n, c = int(n), int(c)
        files[cur][n] = max(files[cur].get(n, 0), c)

rows, tot_l, tot_c = [], 0, 0
for path, da in files.items():
    rel = os.path.relpath(path, root) if path.startswith(root) else path
    if not rel.startswith("src/"):
        continue
    ranges, test_only = test_ranges(os.path.join(root, rel))
    if test_only:
        continue
    keep = {n: c for n, c in da.items() if not any(a <= n <= b for a, b in ranges)}
    l, c = len(keep), sum(1 for v in keep.values() if v > 0)
    if l == 0:
        continue
    rows.append((rel, l, c)); tot_l += l; tot_c += c

pct = 100.0 * tot_c / tot_l if tot_l else 0.0
json.dump({"sha": sha, "flavors": flavors, "lines": tot_l, "covered": tot_c, "percent": round(pct, 1),
           "files": {r: {"lines": l, "covered": c} for r, l, c in rows}},
          open(os.path.join(os.path.dirname(lcov), "summary.json"), "w"), indent=1)
print(f"# lazyview code coverage at {sha[:7]}\n")
print(f"**Production line coverage: {pct:.1f} %** ({tot_c} of {tot_l} instrumented lines in src/, "
      f"inline test modules and test-only files excluded). Tests: lib tests + every integration suite on {flavors}.\n")
print("Suite runs:\n```")
try:
    print(open(suites).read().strip())
except OSError:
    print("(none)")
print("```\n\n| file | lines | covered | % |\n|---|---:|---:|---:|")
for rel, l, c in sorted(rows, key=lambda r: r[2] / r[1]):
    print(f"| {rel} | {l} | {c} | {100.0 * c / l:.1f} |")
