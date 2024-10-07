from enum import Enum
from typing import NamedTuple, Dict, Any, Optional

class OperationType(Enum):
    ADD = 1
    UPDATE = 2
    DELETE = 3

class Operation(NamedTuple):
    type: OperationType
    collection_id: int
    doc_id: Optional[int]
    data: Optional[Dict[str, Any]]
