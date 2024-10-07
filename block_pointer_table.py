from typing import List, Tuple, Optional
import asyncio

class BlockPointerTable:
    def __init__(self):
        self.entries: List[List[Tuple[int, int]]] = []
        self.indirect_entries: List[List[int]] = []
        self.lock = asyncio.Lock()

    async def create_entry(self) -> int:
        async with self.lock:
            self.entries.append([])
            self.indirect_entries.append([])
            return len(self.entries) - 1

    async def add_pointer(self, entry_id: int, block_store_id: int, record_location: int):
        async with self.lock:
            if entry_id >= len(self.entries):
                raise ValueError(f"Invalid entry_id: {entry_id}")
            
            entry = self.entries[entry_id]
            if len(entry) < 16:
                entry.append((block_store_id, record_location))
            else:
                indirect_entry = self.indirect_entries[entry_id]
                if not indirect_entry:
                    indirect_entry.append(await self.create_entry())
                last_indirect = self.entries[indirect_entry[-1]]
                if len(last_indirect) == 16:
                    new_indirect = await self.create_entry()
                    indirect_entry.append(new_indirect)
                    last_indirect = self.entries[new_indirect]
                last_indirect.append((block_store_id, record_location))

    async def get_pointers(self, entry_id: int) -> List[Tuple[int, int]]:
        async with self.lock:
            if entry_id >= len(self.entries):
                raise ValueError(f"Invalid entry_id: {entry_id}")
            
            result = self.entries[entry_id].copy()
            for indirect_id in self.indirect_entries[entry_id]:
                result.extend(self.entries[indirect_id])
            return result

    async def get_indirect_pointers(self, entry_id: int) -> Optional[List[int]]:
        async with self.lock:
            if entry_id >= len(self.indirect_entries):
                raise ValueError(f"Invalid entry_id: {entry_id}")
            
            return self.indirect_entries[entry_id] if self.indirect_entries[entry_id] else None
