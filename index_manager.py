import asyncio
from typing import Dict, Any, List, Tuple, Optional, Callable
from b_plus_tree import BPlusTree
from index_manager_config import IndexManagerConfig
import re
from collections import defaultdict
from filestore_exceptions import *

class Index:
    def __init__(self, tree: BPlusTree, is_compound: bool = False, filter_condition: Optional[Callable] = None):
        self.tree = tree
        self.is_compound = is_compound
        self.filter_condition = filter_condition
        self.access_count = 0
        self.total_query_time = 0

class IndexManager:
    def __init__(self, config: IndexManagerConfig):
        self.config = config
        self.indexes: Dict[str, Index] = {}
        self.ref_indexes: Dict[str, Index] = {}
        self.text_indexes: Dict[str, Dict[str, List[int]]] = {}
        self.async_update_queue = asyncio.Queue(maxsize=config.async_update_queue_size) if config.enable_async_updates else None
        if config.enable_async_updates:
            asyncio.create_task(self._process_async_updates())

    async def create_index(self, name: str, is_compound: bool = False, filter_condition: Optional[Callable] = None):
        try:
            if is_compound and not self.config.enable_compound_indexes:
                raise ConfigurationException("Compound indexes are not enabled in the configuration")
            if filter_condition and not self.config.enable_partial_indexes:
                raise ConfigurationException("Partial indexes are not enabled in the configuration")
            
            if name in self.indexes:
                raise IndexAlreadyExistsError(f"Index {name} already exists")
            
            self.indexes[name] = Index(BPlusTree(order=10), is_compound, filter_condition)
        except Exception as e:
            raise IndexException(f"Failed to create index {name}: {str(e)}")

    async def create_ref_index(self, name: str):
        try:
            if not self.config.enable_ref_indexing:
                raise ConfigurationException("REF indexing is not enabled in the configuration")
            
            if name in self.ref_indexes:
                raise IndexAlreadyExistsError(f"REF index {name} already exists")
            
            self.ref_indexes[name] = Index(BPlusTree(order=10))
        except Exception as e:
            raise IndexException(f"Failed to create REF index {name}: {str(e)}")

    async def create_text_index(self, name: str):
        try:
            if not self.config.enable_text_search:
                raise ConfigurationException("Text search is not enabled in the configuration")
            
            if name in self.text_indexes:
                raise IndexAlreadyExistsError(f"Text index {name} already exists")
            
            self.text_indexes[name] = {}
        except Exception as e:
            raise IndexException(f"Failed to create text index {name}: {str(e)}")

    async def insert(self, index_name: str, key: Any, value: int):
        try:
            if index_name in self.indexes:
                index = self.indexes[index_name]
                if not index.filter_condition or index.filter_condition(key):
                    await self._insert_or_queue(index.tree.insert, key, value)
            elif index_name in self.ref_indexes:
                await self._insert_or_queue(self.ref_indexes[index_name].tree.insert, key, value)
            elif index_name in self.text_indexes:
                await self._insert_text_index(index_name, key, value)
            else:
                raise IndexNotFoundError(f"Index {index_name} not found")
        except Exception as e:
            raise IndexException(f"Failed to insert into index {index_name}: {str(e)}")

    async def _insert_or_queue(self, insert_func, key, value):
        try:
            if self.config.enable_async_updates:
                await self.async_update_queue.put((insert_func, key, value))
            else:
                await insert_func(key, value)
        except asyncio.QueueFull:
            raise ConcurrencyException("Async update queue is full")
        except Exception as e:
            raise IndexException(f"Failed to insert or queue update: {str(e)}")

    async def _insert_text_index(self, index_name: str, text: str, doc_id: int):
        try:
            words = self._tokenize(text)
            for word in words:
                if word not in self.text_indexes[index_name]:
                    self.text_indexes[index_name][word] = []
                self.text_indexes[index_name][word].append(doc_id)
        except Exception as e:
            raise IndexException(f"Failed to insert into text index {index_name}: {str(e)}")

    def _tokenize(self, text: str) -> List[str]:
        # Simple tokenization, can be improved with proper NLP libraries
        return re.findall(r'\w+', text.lower())

    async def search(self, index_name: str, key: Any) -> Optional[Any]:
        try:
            if self.config.enable_usage_statistics:
                return await self._search_with_stats(index_name, key)
            
            if index_name in self.indexes:
                return await self.indexes[index_name].tree.search(key)
            elif index_name in self.ref_indexes:
                return await self.ref_indexes[index_name].tree.search(key)
            else:
                raise IndexNotFoundError(f"Index {index_name} not found")
        except Exception as e:
            raise QueryException(f"Failed to search in index {index_name}: {str(e)}")

    async def _search_with_stats(self, index_name: str, key: Any) -> Optional[Any]:
        start_time = asyncio.get_event_loop().time()
        try:
            result = await self.search(index_name, key)
            query_time = asyncio.get_event_loop().time() - start_time
            
            if index_name in self.indexes:
                self.indexes[index_name].access_count += 1
                self.indexes[index_name].total_query_time += query_time
            elif index_name in self.ref_indexes:
                self.ref_indexes[index_name].access_count += 1
                self.ref_indexes[index_name].total_query_time += query_time
            
            return result
        except Exception as e:
            raise QueryException(f"Failed to search with stats in index {index_name}: {str(e)}")

    async def text_search(self, index_name: str, query: str) -> List[int]:
        try:
            if not self.config.enable_text_search:
                raise ConfigurationException("Text search is not enabled in the configuration")
            
            words = self._tokenize(query)
            results = [set(self.text_indexes[index_name].get(word, [])) for word in words]
            return list(set.intersection(*results) if results else set())
        except KeyError:
            raise IndexNotFoundError(f"Text index {index_name} not found")
        except Exception as e:
            raise QueryException(f"Failed to perform text search in index {index_name}: {str(e)}")

    async def range_query(self, index_name: str, start_key: Any, end_key: Any) -> List[Any]:
        try:
            if index_name in self.indexes:
                return await self.indexes[index_name].tree.range_query(start_key, end_key)
            elif index_name in self.ref_indexes:
                return await self.ref_indexes[index_name].tree.range_query(start_key, end_key)
            else:
                raise IndexNotFoundError(f"Index {index_name} not found")
        except Exception as e:
            raise QueryException(f"Failed to perform range query in index {index_name}: {str(e)}")

    async def delete(self, index_name: str, key: Any):
        try:
            if index_name in self.indexes:
                await self._delete_or_queue(self.indexes[index_name].tree.delete, key)
            elif index_name in self.ref_indexes:
                await self._delete_or_queue(self.ref_indexes[index_name].tree.delete, key)
            elif index_name in self.text_indexes:
                # Implement text index deletion logic
                pass
            else:
                raise IndexNotFoundError(f"Index {index_name} not found")
        except Exception as e:
            raise IndexException(f"Failed to delete from index {index_name}: {str(e)}")

    async def _delete_or_queue(self, delete_func, key):
        try:
            if self.config.enable_async_updates:
                await self.async_update_queue.put((delete_func, key))
            else:
                await delete_func(key)
        except asyncio.QueueFull:
            raise ConcurrencyException("Async update queue is full")
        except Exception as e:
            raise IndexException(f"Failed to delete or queue deletion: {str(e)}")

    async def _process_async_updates(self):
        while True:
            try:
                func, *args = await self.async_update_queue.get()
                await func(*args)
                self.async_update_queue.task_done()
            except Exception as e:
                self.logger.error(f"Error processing async update: {str(e)}")

    def get_usage_statistics(self) -> Dict[str, Dict[str, Any]]:
        try:
            if not self.config.enable_usage_statistics:
                raise ConfigurationException("Usage statistics are not enabled in the configuration")
            
            stats = {}
            for name, index in {**self.indexes, **self.ref_indexes}.items():
                stats[name] = {
                    "access_count": index.access_count,
                    "avg_query_time": index.total_query_time / index.access_count if index.access_count > 0 else 0
                }
            return stats
        except Exception as e:
            raise IndexException(f"Failed to get usage statistics: {str(e)}")
