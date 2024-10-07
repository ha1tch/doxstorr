import time
from typing import Dict, Any, Optional, Tuple
from json_filestore import JSONFilestore
from schema import Schema, create_schema
from filestore_exceptions import *

class CollectionMetadataFilestore(JSONFilestore):
    async def create_collection(self, name: str, schema_definition: Optional[Dict[str, Dict[str, Any]]] = None, 
                                foreign_keys: Optional[Dict[str, Tuple[str, str]]] = None, 
                                enforce_schema: bool = True) -> int:
        try:
            schema = create_schema(schema_definition, foreign_keys) if schema_definition else None
            collection = {
                "name": name,
                "schema": schema_definition,
                "foreign_keys": foreign_keys,
                "enforce_schema": enforce_schema,
                "document_count": 0,
                "created_at": int(time.time()),
                "updated_at": int(time.time())
            }
            collection_id = await self.create(collection)
            self.logger.info(f"Created collection: {name} (ID: {collection_id})")
            return collection_id
        except DocumentAlreadyExistsError:
            raise CollectionAlreadyExistsError(f"Collection {name} already exists")
        except SchemaException as e:
            raise SchemaException(f"Invalid schema for collection {name}: {str(e)}")
        except Exception as e:
            raise CollectionException(f"Failed to create collection {name}: {str(e)}")

    async def get_collection(self, collection_id: int) -> Optional[Dict[str, Any]]:
        try:
            collection = await self.read(collection_id)
            if not collection:
                raise CollectionNotFoundError(f"Collection with id {collection_id} not found")
            return collection
        except DocumentNotFoundError:
            raise CollectionNotFoundError(f"Collection with id {collection_id} not found")
        except Exception as e:
            raise CollectionException(f"Error retrieving collection {collection_id}: {str(e)}")

    async def update_collection(self, collection_id: int, updates: Dict[str, Any]) -> bool:
        try:
            collection = await self.get_collection(collection_id)
            collection.update(updates)
            collection["updated_at"] = int(time.time())
            success = await self.update(collection_id, collection)
            if success:
                self.logger.info(f"Updated collection: {collection_id}")
            return success
        except CollectionNotFoundError:
            raise
        except Exception as e:
            raise CollectionException(f"Failed to update collection {collection_id}: {str(e)}")

    async def delete_collection(self, collection_id: int) -> bool:
        try:
            success = await self.delete(collection_id)
            if success:
                self.logger.info(f"Deleted collection: {collection_id}")
            return success
        except DocumentNotFoundError:
            raise CollectionNotFoundError(f"Collection with id {collection_id} not found")
        except Exception as e:
            raise CollectionException(f"Failed to delete collection {collection_id}: {str(e)}")

    async def validate_document(self, collection_id: int, document: Dict[str, Any], filestore) -> bool:
        try:
            collection = await self.get_collection(collection_id)
            if not collection["enforce_schema"]:
                return True  # Schema-less mode, no validation needed
            
            if not collection["schema"]:
                return True  # No schema defined, considered valid
            
            schema = create_schema(collection["schema"], collection.get("foreign_keys"))
            if not await schema.validate(document, filestore):
                raise DocumentValidationError("Document does not match collection schema")
            return True
        except CollectionNotFoundError:
            raise
        except DocumentValidationError:
            raise
        except Exception as e:
            raise SchemaException(f"Error validating document for collection {collection_id}: {str(e)}")
