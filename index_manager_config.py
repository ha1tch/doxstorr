from typing import Dict, Any

class IndexManagerConfig:
    def __init__(self, config: Dict[str, Any]):
        self.enable_ref_indexing = config.get('enable_ref_indexing', False)
        self.enable_compound_indexes = config.get('enable_compound_indexes', False)
        self.enable_partial_indexes = config.get('enable_partial_indexes', False)
        self.enable_text_search = config.get('enable_text_search', False)
        self.enable_async_updates = config.get('enable_async_updates', False)
        self.enable_usage_statistics = config.get('enable_usage_statistics', False)
        self.async_update_queue_size = config.get('async_update_queue_size', 1000)
        self.text_search_language = config.get('text_search_language', 'english')
