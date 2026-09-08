from typing import Self

from numpy import intp
from numpy.typing import ArrayLike, NDArray

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
        X: ArrayLike,
        y: object | None = None,
        sample_weight: ArrayLike | None = None,
    ) -> Self: ...
