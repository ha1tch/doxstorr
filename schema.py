from typing import Dict, Any, Optional, List, Tuple, Type

class FieldDefinition:
    def __init__(self, field_type: str, required: bool = False, default: Any = None, ref: Optional[str] = None):
        self.field_type: str = field_type
        self.required: bool = required
        self.default: Any = default
        self.ref: Optional[str] = ref

class Schema:
    def __init__(self, fields: Dict[str, FieldDefinition], foreign_keys: Optional[Dict[str, Tuple[str, str]]] = None):
        self.fields: Dict[str, FieldDefinition] = fields
        self.foreign_keys: Dict[str, Tuple[str, str]] = foreign_keys or {}

    async def validate(self, document: Dict[str, Any], filestore: Any) -> bool:
        for field_name, field_def in self.fields.items():
            if field_name not in document:
                if field_def.required:
                    return False
                if field_def.default is not None:
                    document[field_name] = field_def.default
            else:
                if not await self._validate_field_type(document[field_name], field_def, filestore):
                    return False
        
        # Validate foreign key constraints
        for field_name, (ref_collection, ref_field) in self.foreign_keys.items():
            if field_name in document:
                if not await self._validate_foreign_key(document[field_name], ref_collection, ref_field, filestore):
                    return False
        
        return True

    async def _validate_field_type(self, value: Any, field_def: FieldDefinition, filestore: Any) -> bool:
        if field_def.field_type == 'REF':
            return await self._validate_ref(value, field_def.ref, filestore)
        elif field_def.field_type == 'string':
            return isinstance(value, str)
        elif field_def.field_type == 'integer':
            return isinstance(value, int)
        elif field_def.field_type == 'float':
            return isinstance(value, (int, float))
        elif field_def.field_type == 'boolean':
            return isinstance(value, bool)
        elif field_def.field_type == 'list':
            return isinstance(value, list)
        elif field_def.field_type == 'dict':
            return isinstance(value, dict)
        else:
            return True  # Unknown types are considered valid

    async def _validate_ref(self, value: Dict[str, Any], ref_collection: str, filestore: Any) -> bool:
        if not isinstance(value, dict) or 'collection' not in value or 'id' not in value:
            return False
        if value['collection'] != ref_collection:
            return False
        # Check if the referenced document exists
        referenced_doc = await filestore.get_document(value['id'])
        return referenced_doc is not None

    async def _validate_foreign_key(self, value: Any, ref_collection: str, ref_field: str, filestore: Any) -> bool:
        # Check if the referenced document exists and has the specified field
        referenced_doc = await filestore.get_document_by_field(ref_collection, ref_field, value)
        return referenced_doc is not None

def create_schema(schema_definition: Dict[str, Dict[str, Any]], foreign_keys: Optional[Dict[str, Tuple[str, str]]] = None) -> Schema:
    fields = {}
    for field_name, field_props in schema_definition.items():
        fields[field_name] = FieldDefinition(
            field_type=field_props.get('type', 'string'),
            required=field_props.get('required', False),
            default=field_props.get('default'),
            ref=field_props.get('ref')
        )
    return Schema(fields, foreign_keys)
