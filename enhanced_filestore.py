import json
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Set
from json_filestore import JSONFilestore
from collection_metadata_filestore import CollectionMetadataFilestore
from block_store import SmallBlockStore, MediumBlockStore, LargeBlockStore
from block_pointer_table import BlockPointerTable
from index_manager import IndexManager, IndexManagerConfig
from journal import Journal
from logger import Logger
from filestore_maintenance import FileStoreMaintenance
from transaction import TransactionManager
from filestore_exceptions import *
from transaction_types import OperationType, Operation

class EnhancedFilestore:
    def __init__(self, base_path: str, index_config: Dict[str, Any], doc_cache_capacity: int = 1000, collection_cache_capacity: int = 100):
        try:
            self.base_path = base_path
            self.document_store = JSONFilestore(f"{base_path}/documents.json", doc_cache_capacity)
            self.collection_store = CollectionMetadataFilestore(f"{base_path}/collections.json", collection_cache_capacity)
            self.small_block_store = SmallBlockStore(f"{base_path}/small_blocks.bin")
            self.medium_block_store = MediumBlockStore(f"{base_path}/medium_blocks.bin")
            self.large_block_store = LargeBlockStore(f"{base_path}/large_blocks.bin")
            self.block_pointer_table = BlockPointerTable()
            self.index_manager = IndexManager(IndexManagerConfig(index_config))
            self.journal = Journal(f"{base_path}/journal.log")
            self.logger = Logger.get_logger()
            self.maintenance = FileStoreMaintenance(self)
            self.transaction_manager = TransactionManager()
            self.lock = asyncio.Lock()
            self.DEFAULT_POPULATE_DEPTH = 3
        except Exception as e:
            raise ConfigurationException(f"Error initializing EnhancedFilestore: {str(e)}")

    async def create_collection(self, name: str, schema_definition: Optional[Dict[str, Dict[str, Any]]] = None, 
                                foreign_keys: Optional[Dict[str, Tuple[str, str]]] = None,
                                enforce_schema: bool = True) -> int:
        try:
            collection_id = await self.collection_store.create_collection(name, schema_definition, foreign_keys, enforce_schema)
            if schema_definition and enforce_schema:
                for field in schema_definition:
                    await self.index_manager.create_index(f"{name}_{field}")
            self.logger.info(f"Created collection: {name} (Schema enforced: {enforce_schema})")
            return collection_id
        except CollectionAlreadyExistsError:
            raise
        except SchemaException as e:
            raise SchemaException(f"Error in schema definition for collection {name}: {str(e)}")
        except Exception as e:
            raise CollectionException(f"Error creating collection {name}: {str(e)}")

    async def add_document(self, collection_id: int, data: Dict[str, Any]) -> int:
        transaction = await self.transaction_manager.start_transaction()
        try:
            collection = await self.collection_store.get_collection(collection_id)
            if collection["enforce_schema"]:
                if not await self.collection_store.validate_document(collection_id, data, self):
                    raise DocumentValidationError("Document does not match collection schema")
            
            doc_id = await self._add_document(transaction, collection_id, data)
            await self.transaction_manager.run_transaction(transaction)
            self.logger.info(f"Added document {doc_id} to collection {collection_id}")
            return doc_id
        except CollectionNotFoundError:
            raise
        except DocumentValidationError:
            raise
        except TransactionAbortedError:
            raise
        except Exception as e:
            raise DocumentException(f"Error adding document to collection {collection_id}: {str(e)}")

    async def get_document(self, doc_id: int) -> Dict[str, Any]:
        try:
            doc_metadata = await self.document_store.read(doc_id)
            if not doc_metadata:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")

            block_store, _ = await self._select_block_store(doc_metadata["size"])
            pointers = await self.block_pointer_table.get_pointers(doc_metadata["block_pointer"])

            data = b""
            for _, block in pointers:
                data += await self._read_block(block_store, block)

            return json.loads(data.decode('utf-8'))
        except DocumentNotFoundError:
            raise
        except json.JSONDecodeError:
            raise StorageException(f"Error decoding document {doc_id}: Corrupted data")
        except Exception as e:
            raise StorageException(f"Error retrieving document {doc_id}: {str(e)}")

    async def update_document(self, doc_id: int, data: Dict[str, Any]) -> bool:
        transaction = await self.transaction_manager.start_transaction()
        try:
            old_doc = await self.get_document(doc_id)
            if not old_doc:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")
            
            collection_id = old_doc["collection_id"]
            collection = await self.collection_store.get_collection(collection_id)
            if collection["enforce_schema"]:
                if not await self.collection_store.validate_document(collection_id, data, self):
                    raise DocumentValidationError("Updated document does not match collection schema")
            
            success = await self._update_document(transaction, doc_id, data)
            await self.transaction_manager.run_transaction(transaction)
            self.logger.info(f"Updated document {doc_id}")
            return success
        except DocumentNotFoundError:
            raise
        except DocumentValidationError:
            raise
        except TransactionAbortedError:
            raise
        except Exception as e:
            raise DocumentException(f"Error updating document {doc_id}: {str(e)}")

    async def delete_document(self, doc_id: int) -> bool:
        transaction = await self.transaction_manager.start_transaction()
        try:
            doc_metadata = await self.document_store.read(doc_id)
            if not doc_metadata:
                raise DocumentNotFoundError(f"Document with id {doc_id} not found")

            success = await self._delete_document(transaction, doc_id)
            await self.transaction_manager.run_transaction(transaction)
            self.logger.info(f"Deleted document {doc_id}")
            return success
        except DocumentNotFoundError:
            raise
        except TransactionAbortedError:
            raise
        except Exception as e:
            raise DocumentException(f"Error deleting document {doc_id}: {str(e)}")

    async def query_documents(self, collection_id: int, filter_func: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
        try:
            documents = await self.document_store.query_by_collection_id(collection_id)
            return [doc for doc in documents if filter_func(doc)]
        except CollectionNotFoundError:
            raise
        except Exception as e:
            raise QueryException(f"Error querying documents in collection {collection_id}: {str(e)}")

    async def atomic_transaction_execute(self, operations: List[Operation]) -> List[Tuple[bool, Optional[int]]]:
        transaction = await self.transaction_manager.start_transaction()
        results = []

        try:
            for operation in operations:
                if operation.type == OperationType.ADD:
                    doc_id = await self._add_document(transaction, operation.collection_id, operation.data)
                    results.append((True, doc_id))
                elif operation.type == OperationType.UPDATE:
                    success = await self._update_document(transaction, operation.doc_id, operation.data)
                    results.append((success, operation.doc_id))
                elif operation.type == OperationType.DELETE:
                    success = await self._delete_document(transaction, operation.doc_id)
                    results.append((success, operation.doc_id))
                else:
                    raise ValueError(f"Unknown operation type: {operation.type}")

            await self.transaction_manager.run_transaction(transaction)
            self.logger.info(f"Atomic transaction executed successfully with {len(operations)} operations")
            return results

        except TransactionAbortedError:
            self.logger.error("Atomic transaction aborted")
            raise
        except Exception as e:
            self.logger.error(f"Error executing atomic transaction: {str(e)}")
            raise TransactionException(f"Error executing atomic transaction: {str(e)}")

    # ... (other methods with similar error handling)

    async def _select_block_store(self, size: int) -> Tuple[BlockStore, int]:
        if size <= 4 * 1024:  # 4 KB
            return self.small_block_store, 0
        elif size <= 64 * 1024:  # 64 KB
            return self.medium_block_store, 1
        else:
            return self.large_block_store, 2

    async def _write_to_blocks(self, block_store, data: bytes) -> List[int]:
        blocks = []
        try:
            for i in range(0, len(data), block_store.block_size):
                block = await self._allocate_block(block_store)
                await self._write_block(block_store, block, data[i:i+block_store.block_size])
                blocks.append(block)
            return blocks
        except BlockAllocationError:
            raise
        except BlockWriteError:
            raise
        except Exception as e:
            raise StorageException(f"Error writing data to blocks: {str(e)}")

    @staticmethod
    async def _allocate_block(block_store):
        try:
            return await asyncio.to_thread(block_store.allocate_block)
        except Exception as e:
            raise BlockAllocationError(f"Error allocating block: {str(e)}")

    @staticmethod
    async def _write_block(block_store, block, data):
        try:
            await asyncio.to_thread(block_store.write_block, block, data)
        except Exception as e:
            raise BlockWriteError(f"Error writing to block: {str(e)}")

    @staticmethod
    async def _read_block(block_store, block):
        try:
            return await asyncio.to_thread(block_store.read_block, block)
        except Exception as e:
            raise BlockReadError(f"Error reading from block: {str(e)}")

    @staticmethod
    async def _free_block(block_store, block):
        try:
            await asyncio.to_thread(block_store.free_block, block)
        except Exception as e:
            raise StorageException(f"Error freeing block: {str(e)}")

    # ... (other helper methods with appropriate error handling)

