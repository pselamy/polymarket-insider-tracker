from typing import Self

from numpy import float64, intp
from numpy.typing import NDArray

class DBSCAN:
    labels_: NDArray[intp]

    def __init__(
        self,
        eps: float = 0.5,
        *,
        min_samples: int = 5,
        metric: str = "euclidean",
    ) -> None: ...
    def fit(
        self,
        X: NDArray[float64],
    ) -> Self: ...
