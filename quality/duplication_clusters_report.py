"""Render the generated duplication totals and per-clone list."""

from quality.duplication_clusters import Clone, Cluster, group

HEADER = "| tok | ln | A | B |\n| --- | --- | --- | --- |"


def clone_row(clone: Clone) -> str:
    return (
        f"| {clone.tokens} | {clone.lines} "
        f"| {clone.first}:{clone.first_start} | {clone.second}:{clone.second_start} |"
    )


def cluster_row(cluster: Cluster | None, clones: list[Clone]) -> str:
    label = f"{cluster.number:02d} ({cluster.status})" if cluster else "unassigned"
    return f"| {label} | {len(clones)} | {sum(clone.lines for clone in clones)} |"


def render(clusters: list[Cluster], clones: list[Clone], statistics: dict[str, object]) -> str:
    """Totals come from the native statistics; nothing here is hand-edited."""
    grouped = group([c for c in clusters if c.status != "resolved"], clones)
    by_number = {cluster.number: cluster for cluster in clusters}
    rows = [cluster_row(by_number.get(n), grouped[n]) for n in sorted(grouped)]
    return "\n".join(
        [
            "# Generated clone report",
            "",
            f"**{statistics['clones']} clone groups, {statistics['duplicatedLines']} "
            f"duplicated lines, {float(str(statistics['percentage'])):.2f}%** over "
            f"{statistics['sources']} tokenized sources.",
            "",
            "| cluster | clones | duplicated lines |",
            "| --- | --- | --- |",
            *rows,
            "",
            HEADER,
            *(clone_row(clone) for clone in clones),
            "",
        ]
    )
