import json
import os
import zlib
import asyncio
import time
from typing import Dict, Any, Optional, List
from index_manager import IndexManager
from reader_writer_lock import ReaderWriterLock, reader_lock, writer_lock
from lru_cache import LRUCache
from logger import Logger
from filestore_exceptions import *

class JSONFilestore:
    def __init__(self, filename: str, cache_capacity: int = 100):
        self.filename = filename
        self.data: Dict[int, Dict[str, Any]] = {}
        self.index_manager = IndexManager()
        self.compression_queue = asyncio.Queue()
        self.lock = ReaderWriterLock()
        self.locks = {}
        self.cache = LRUCache(cache_capacity)
        self.next_id = 1
        self.logger = Logger.get_logger()
        self.load_data()
        asyncio.create_task(self.process_compression_queue())

    def load_data(self):
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    self.data = json.load(f)
                self.next_id = max(self.data.keys()) + 1 if self.data else 1
                self.logger.info(f"Data loaded from {self.filename}")
            else:
                self.logger.warning(f"File {self.filename} does not exist. Starting with empty data.")
        except json.JSONDecodeError as e:
            raise StorageException(f"Error decoding JSON from {self.filename}: {str(e)}")
        except Exception as e:
            raise StorageException(f"Unexpected error loading data from {self.filename}: {str(e)}")

    @writer_lock
    async def create(self, document: Dict[str, Any]) -> int:
        doc_id = self.next_id
        self.next_id += 1
        return await self._create_with_id(doc_id, document)

    @writer_lock
    async def create_with_id(self, doc_id: int, document: Dict[str, Any]) -> int:
        if doc_id in self.data:
            raise DocumentAlreadyExistsError(f"Document with id {doc_id} already exists")
        if doc_id >= self.next_id:
            self.next_id = doc_id + 1
        return await self._create_with_id(doc_id, document)

    async def _create_with_id(self, doc_id: int, document: Dict[str, Any]) -> int:
        try:
            document['id'] = doc_id
            document['created_at'] = int(time.time())
            document['updated_at'] = int(time.time())
            self.data[doc_id] = document
            self.cache.put(doc_id, document)
            if len(json.dumps(document)) > 4 * 1024:  # 4 KB
                await self.compression_queue.put((doc_id, document))
            self.logger.info(f"Document created with id {doc_id}")
            return doc_id
        except Exception as e:
            raise StorageException(f"Error creating document with id {doc_id}: {str(e)}")

    @reader_lock
    async def read(self, doc_id: int) -> Dict[str, Any]:
        try:
            document = self.cache.get(doc_id)
            if document is not None:
                return document

            document = self.data.get(doc_id)
            if document is None:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")

            if document.get('compressed'):
                document['data'] = await self._decompress_data(document['data'])
                document['compressed'] = False

            self.cache.put(doc_id, document)
            return document
        except DocumentNotFoundError:
            raise
        except Exception as e:
            raise StorageException(f"Error reading document {doc_id}: {str(e)}")

    @writer_lock
    async def update(self, doc_id: int, updates: Dict[str, Any]) -> bool:
        try:
            if doc_id not in self.data:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")

            document = self.data[doc_id]
            document.update(updates)
            document['updated_at'] = int(time.time())
            self.cache.put(doc_id, document)

            if len(json.dumps(document)) > 4 * 1024:  # 4 KB
                await self.compression_queue.put((doc_id, document))

            self.logger.info(f"Document {doc_id} updated")
            return True
        except DocumentNotFoundError:
            raise
        except Exception as e:
            raise StorageException(f"Error updating document {doc_id}: {str(e)}")

    @writer_lock
    async def delete(self, doc_id: int) -> bool:
        try:
            if doc_id not in self.data:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")

            del self.data[doc_id]
            self.cache.invalidate(doc_id)
            self.logger.info(f"Document {doc_id} deleted")
            return True
        except DocumentNotFoundError:
            raise
        except Exception as e:
            raise StorageException(f"Error deleting document {doc_id}: {str(e)}")

    async def _compress_data(self, data: str) -> bytes:
        try:
            return zlib.compress(data.encode('utf-8'))
        except Exception as e:
            raise StorageException(f"Error compressing data: {str(e)}")

    async def _decompress_data(self, data: bytes) -> str:
        try:
            return zlib.decompress(data).decode('utf-8')
        except Exception as e:
            raise StorageException(f"Error decompressing data: {str(e)}")

    async def process_compression_queue(self):
        while True:
            doc_id, document = await self.compression_queue.get()
            try:
                compressed_data = await self._compress_data(json.dumps(document['data']))
                document['data'] = compressed_data
                document['compressed'] = True
                self.data[doc_id] = document
                self.logger.info(f"Compressed document {doc_id}")
            except Exception as e:
                self.logger.error(f"Error compressing document {doc_id}: {str(e)}")
            finally:
                self.compression_queue.task_done()

    @reader_lock
    async def query_by_collection_id(self, collection_id: int) -> List[Dict[str, Any]]:
        try:
            return [doc for doc in self.data.values() if doc.get('collection_id') == collection_id]
        except Exception as e:
            raise QueryException(f"Error querying documents by collection_id {collection_id}: {str(e)}")
