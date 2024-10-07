class FilestoreException(Exception):
    """Base exception class for all filestore-related exceptions."""

class DocumentException(FilestoreException):
    """Base exception for document-related errors."""

class DocumentNotFoundError(DocumentException):
    """Raised when a document is not found."""

class DocumentAlreadyExistsError(DocumentException):
    """Raised when trying to create a document with an ID that already exists."""

class DocumentValidationError(DocumentException):
    """Raised when a document fails schema validation."""

class CollectionException(FilestoreException):
    """Base exception for collection-related errors."""

class CollectionNotFoundError(CollectionException):
    """Raised when a collection is not found."""

class CollectionAlreadyExistsError(CollectionException):
    """Raised when trying to create a collection that already exists."""

class SchemaException(FilestoreException):
    """Base exception for schema-related errors."""

class SchemaValidationError(SchemaException):
    """Raised when there's an error in schema validation."""

class IndexException(FilestoreException):
    """Base exception for index-related errors."""

class IndexAlreadyExistsError(IndexException):
    """Raised when trying to create an index that already exists."""

class IndexNotFoundError(IndexException):
    """Raised when an index is not found."""

class TransactionException(FilestoreException):
    """Base exception for transaction-related errors."""

class TransactionAbortedError(TransactionException):
    """Raised when a transaction is aborted."""

class DeadlockDetectedError(TransactionException):
    """Raised when a deadlock is detected."""

class StorageException(FilestoreException):
    """Base exception for storage-related errors."""

class BlockAllocationError(StorageException):
    """Raised when there's an error allocating a block."""

class BlockReadError(StorageException):
    """Raised when there's an error reading a block."""

class BlockWriteError(StorageException):
    """Raised when there's an error writing a block."""

class ConfigurationException(FilestoreException):
    """Raised when there's an error in the configuration."""

class ConcurrencyException(FilestoreException):
    """Base exception for concurrency-related errors."""

class LockAcquisitionError(ConcurrencyException):
    """Raised when there's an error acquiring a lock."""

class OperationNotPermittedError(FilestoreException):
    """Raised when an operation is not permitted (e.g., writing to a read-only filestore)."""
