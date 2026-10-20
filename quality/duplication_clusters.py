"""Check per-cluster duplication files against the clones a native scan reports."""

from dataclasses import dataclass
from pathlib import Path

from quality.duplication_inventory import nonnegative
from quality.report_data import array, record, text

STATUSES = ("real", "incidental", "resolved")
LIVE_STATUSES = ("real", "incidental")
SEPARATOR = " <-> "


@dataclass(frozen=True)
class Clone:
    tokens: int
    lines: int
    first: str
    first_start: int
    second: str
    second_start: int


@dataclass(frozen=True)
class Cluster:
    number: int
    status: str
    files: frozenset[str]
    watch: frozenset[frozenset[str]]
    name: str


def front_matter(content: str) -> dict[str, str]:
    """Read the flat ``key: value`` header that opens every cluster file."""
    lines = content.splitlines()
    if not lines or lines[0] != "---" or "---" not in lines[1:]:
        raise ValueError("cluster file must open with a front-matter block")
    pairs = (line.partition(": ") for line in lines[1 : lines.index("---", 1)])
    return {key: value.strip() for key, _, value in pairs}


def listed(value: str | None) -> list[str]:
    """Lists are comma separated; absent keys are empty."""
    return [part.strip() for part in (value or "").split(",") if part.strip()]


def watched(value: str | None) -> frozenset[frozenset[str]]:
    """A watched pair names two files whose clone must stay gone."""
    return frozenset(frozenset(entry.split(SEPARATOR)) for entry in listed(value))


def parse_cluster(name: str, content: str) -> Cluster:
    """Validate one cluster header against the closed vocabulary."""
    header = front_matter(content)
    status = header.get("status", "")
    if status not in STATUSES:
        raise ValueError(f"{name}: status must be one of {', '.join(STATUSES)}")
    number = int(header.get("cluster", ""))
    if not name.startswith(f"{number:02d}-"):
        raise ValueError(f"{name}: file name does not carry cluster {number:02d}")
    files = frozenset(listed(header.get("files")))
    if status in LIVE_STATUSES and not files:
        raise ValueError(f"{name}: a {status} cluster must list the files it owns")
    return Cluster(number, status, files, watched(header.get("watch")), name)


def load_clusters(directory: Path) -> list[Cluster]:
    """Cluster numbers are unique across the directory."""
    clusters = [
        parse_cluster(path.name, path.read_text(encoding="utf-8"))
        for path in sorted(directory.glob("*.md"))
    ]
    if len({cluster.number for cluster in clusters}) != len(clusters):
        raise ValueError("duplication cluster numbers must be unique")
    return clusters


def clone_from_report(value: object, stage: str) -> Clone:
    """Map one native duplicate back to repository-relative paths."""
    clone = record(value)
    first, second = record(clone["firstFile"]), record(clone["secondFile"])
    return Clone(
        nonnegative(clone["tokens"]),
        nonnegative(clone["lines"]),
        text(first["name"]).removeprefix(stage),
        nonnegative(first["start"]),
        text(second["name"]).removeprefix(stage),
        nonnegative(second["start"]),
    )


def clones_from_report(report: object, stage: str) -> list[Clone]:
    """Largest first, ties broken by position, so output is deterministic."""
    clones = [clone_from_report(item, stage) for item in array(record(report)["duplicates"])]
    return sorted(clones, key=lambda c: (-c.tokens, c.first, c.first_start, c.second))


def owner(clusters: list[Cluster], clone: Clone) -> Cluster | None:
    """A live cluster owns a clone when both sides sit in files it lists."""
    for cluster in clusters:
        if {clone.first, clone.second} <= cluster.files:
            return cluster
    return None


def group(clusters: list[Cluster], clones: list[Clone]) -> dict[int, list[Clone]]:
    """Cluster number 0 collects clones no cluster file claims yet."""
    grouped: dict[int, list[Clone]] = {}
    for clone in clones:
        found = owner(clusters, clone)
        grouped.setdefault(found.number if found else 0, []).append(clone)
    return grouped


def stale_watch(cluster: Cluster, clones: list[Clone]) -> list[str]:
    """A resolved cluster must not see a clone between files it watches."""
    return [
        f"cluster {cluster.number:02d} is resolved but clones remain: "
        f"{clone.first}:{clone.first_start}{SEPARATOR}{clone.second}:{clone.second_start}"
        for clone in clones
        if frozenset((clone.first, clone.second)) in cluster.watch
    ]


def emptied(clusters: list[Cluster], clones: list[Clone]) -> list[str]:
    """A live cluster whose clones are all gone should say so."""
    grouped = group([c for c in clusters if c.status in LIVE_STATUSES], clones)
    return [
        f"cluster {c.number:02d} is {c.status} but no clone remains; mark it resolved"
        for c in clusters
        if c.status in LIVE_STATUSES and c.number not in grouped
    ]


def problems(clusters: list[Cluster], clones: list[Clone]) -> list[str]:
    """Every disagreement between cluster files and the scan, one line each."""
    found = [problem for c in clusters for problem in stale_watch(c, clones)]
    return found + emptied(clusters, clones)
