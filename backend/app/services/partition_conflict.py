"""Partition ordering + overlap/adjacency conflict detection (pure functions).

Two admitted jobs may only run concurrently when their partition sets neither
overlap nor touch: partitions are ordered by value inside the table's
partition universe, and any pair whose positions differ by exactly 1 is
"adjacent" and must be intercepted (queued) rather than executed.
"""


def ordered_universe(*partition_groups) -> list[str]:
    """Sorted union of every known partition value for a table."""
    values: set[str] = set()
    for group in partition_groups:
        if not group:
            continue
        values.update(str(value) for value in group)
    return sorted(values)


def partition_index(universe: list[str]) -> dict[str, int]:
    return {value: position for position, value in enumerate(universe)}


def describe_conflicts(
    requested: list[str],
    occupied: dict[str, int],
    universe: list[str] | None = None,
) -> dict:
    """Return every overlap/adjacency conflict between ``requested`` and the
    currently leased partitions.

    ``occupied`` maps partition_value -> owning job_id. The result carries a
    deduplicated conflict list plus the sorted blocking job ids.
    """
    requested_values = sorted({str(value) for value in requested})
    occupied_map = {str(key): int(value) for key, value in occupied.items()}
    if universe is None:
        universe = ordered_universe(requested_values, occupied_map.keys())
    index = partition_index(universe)

    conflicts: list[dict] = []
    seen: set[tuple] = set()
    for value in requested_values:
        owner = occupied_map.get(value)
        if owner is not None:
            key = (value, value, owner, "overlap")
            if key not in seen:
                conflicts.append(
                    {"requested": value, "conflicting": value, "job_id": owner, "reason": "overlap"}
                )
                seen.add(key)
        for other, other_owner in occupied_map.items():
            if other == value:
                continue
            if abs(index[value] - index[other]) == 1:
                key = (value, other, other_owner, "adjacent")
                if key not in seen:
                    conflicts.append(
                        {
                            "requested": value,
                            "conflicting": other,
                            "job_id": other_owner,
                            "reason": "adjacent",
                        }
                    )
                    seen.add(key)
    job_ids = sorted({item["job_id"] for item in conflicts})
    return {"conflicts": conflicts, "job_ids": job_ids}
