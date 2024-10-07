import os
import asyncio
from typing import List
from logger import Logger
from filestore_exceptions import *

class BlockStore:
    def __init__(self, file_path: str, block_size: int):
        self.file_path = file_path
        self.block_size = block_size
        self.free_blocks: List[int] = []
        self.logger = Logger.get_logger()
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        try:
            if not os.path.exists(self.file_path):
                with open(self.file_path, 'wb') as f:
                    pass  # Create an empty file
            self.logger.info(f"Ensured file exists: {self.file_path}")
        except IOError as e:
            raise StorageException(f"Error ensuring file exists {self.file_path}: {str(e)}")
        except Exception as e:
            raise StorageException(f"Unexpected error ensuring file exists {self.file_path}: {str(e)}")

    async def allocate_block(self) -> int:
        try:
            return await asyncio.to_thread(self._allocate_block_sync)
        except Exception as e:
            raise BlockAllocationError(f"Error allocating block: {str(e)}")

    def _allocate_block_sync(self) -> int:
        if self.free_blocks:
            block_id = self.free_blocks.pop()
            self.logger.info(f"Allocated existing block: {block_id}")
            return block_id
        else:
            try:
                with open(self.file_path, 'ab') as f:
                    f.seek(0, 2)  # Move to the end of the file
                    block_id = f.tell() // self.block_size
                    f.write(b'\0' * self.block_size)
                self.logger.info(f"Allocated new block: {block_id}")
                return block_id
            except IOError as e:
                raise BlockAllocationError(f"IO error allocating new block: {str(e)}")

    async def write_block(self, block_id: int, data: bytes):
        try:
            await asyncio.to_thread(self._write_block_sync, block_id, data)
        except Exception as e:
            raise BlockWriteError(f"Error writing block {block_id}: {str(e)}")

    def _write_block_sync(self, block_id: int, data: bytes):
        try:
            with open(self.file_path, 'r+b') as f:
                f.seek(block_id * self.block_size)
                f.write(data.ljust(self.block_size, b'\0'))
            self.logger.info(f"Written block {block_id}")
        except IOError as e:
            raise BlockWriteError(f"IO error writing block {block_id}: {str(e)}")

    async def read_block(self, block_id: int) -> bytes:
        try:
            return await asyncio.to_thread(self._read_block_sync, block_id)
        except Exception as e:
            raise BlockReadError(f"Error reading block {block_id}: {str(e)}")

    def _read_block_sync(self, block_id: int) -> bytes:
        try:
            with open(self.file_path, 'rb') as f:
                f.seek(block_id * self.block_size)
                return f.read(self.block_size)
        except IOError as e:
            raise BlockReadError(f"IO error reading block {block_id}: {str(e)}")

    async def free_block(self, block_id: int):
        try:
            await asyncio.to_thread(self._free_block_sync, block_id)
        except Exception as e:
            raise StorageException(f"Error freeing block {block_id}: {str(e)}")

    def _free_block_sync(self, block_id: int):
        self.free_blocks.append(block_id)
        self.logger.info(f"Freed block {block_id}")

class SmallBlockStore(BlockStore):
    def __init__(self, file_path: str):
        super().__init__(file_path, 4 * 1024)  # 4 KB

class MediumBlockStore(BlockStore):
    def __init__(self, file_path: str):
        super().__init__(file_path, 64 * 1024)  # 64 KB

class LargeBlockStore(BlockStore):
    def __init__(self, file_path: str):
        super().__init__(file_path, 1024 * 1024)  # 1 MB
