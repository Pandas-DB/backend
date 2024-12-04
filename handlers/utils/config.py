from dataclasses import dataclass
from typing import Set


@dataclass
class StorageConfig:
    chunk_size: int = 1000000
    reserved_words: Set[str] = None
    default_storage_method: str = 'concat'

    def __post_init__(self):
        if self.reserved_words is None:
            self.reserved_words = {'log', 'default'}
