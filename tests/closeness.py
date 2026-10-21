"""Float comparison with the tolerances ``pytest.approx`` applies by default."""

import math


def is_close(
    actual: float, expected: float, *, rel_tol: float = 1e-6, abs_tol: float = 1e-12
) -> bool:
    """Whether ``actual`` equals ``expected`` within the tolerances."""
    return math.isclose(actual, expected, rel_tol=rel_tol, abs_tol=abs_tol)
