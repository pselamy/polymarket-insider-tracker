"""Run historically effective cheap witnesses first without filtering tests."""

CLEANUP_PROBE = (
    "tests/test_process_cleanup.py::" "test_native_timeout_terminates_descendant_before_it_writes"
)
NATIVE_RECEIPTS = frozenset(
    (
        "tests/test_pytest_integrity.py",
        "tests/test_owned_process_native.py",
        "tests/test_owned_caller_native.py",
    )
)
FAST_FIRST = (
    "tests/test_annotation_contract.py",
    "tests/test_security_runner.py",
    "tests/test_package_consumer.py",
    "tests/test_component_inventory.py",
    "tests/test_mutation_services.py",
    "tests/test_package_build.py",
    "tests/test_build_constraints.py",
    "tests/test_package_archive.py",
    "tests/test_owned_commands.py",
    "tests/test_workflow_paths.py",
    "tests/test_owned_process.py",
    "tests/test_duplication.py",
    "tests/test_mutation_report.py",
    "tests/test_mutation_transport.py",
    "tests/test_mutation_runner.py",
    "tests/test_deadline.py",
    "tests/test_mutation_pool.py",
    "tests/test_pipeline.py",
    "tests/test_portable_paths.py",
    "tests/test_cli.py",
    "tests/test_owned_receipt.py",
    "tests/test_runtime_support.py",
    "tests/test_package_boundaries.py",
    "tests/test_intervals.py",
    "tests/test_mutation_journal.py",
    "tests/test_early_ordering.py",
    "tests/test_document_links.py",
    "tests/test_stability.py",
    "tests/test_incomplete.py",
    "tests/test_repository_hygiene.py",
)
FAST_RANK = {name: index for index, name in enumerate(FAST_FIRST)}


def priority(node: str) -> tuple[bool, int, str]:
    """Order the full collection by reviewed first-kill evidence and node ID."""
    file = node.partition("::")[0]
    late = file in NATIVE_RECEIPTS or node == CLEANUP_PROBE
    return late, FAST_RANK.get(file, len(FAST_FIRST)), node
