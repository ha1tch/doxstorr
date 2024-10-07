import asyncio
from typing import List, Dict, Any, Callable
import time
from logger import Logger
from filestore_exceptions import *

class Transaction:
    def __init__(self, transaction_id: int):
        self.id = transaction_id
        self.operations: List[Callable] = []
        self.rollback_operations: List[Callable] = []
        self.locks: List[asyncio.Lock] = []
        self.start_time = time.time()
        self.logger = Logger.get_logger()

    async def add_operation(self, operation: Callable, rollback: Callable, lock: asyncio.Lock):
        try:
            self.operations.append(operation)
            self.rollback_operations.append(rollback)
            self.locks.append(lock)
            await lock.acquire()
            self.logger.info(f"Transaction {self.id}: Added operation")
        except Exception as e:
            raise TransactionException(f"Failed to add operation to transaction {self.id}: {str(e)}")

    async def execute(self):
        for operation in self.operations:
            try:
                await operation()
            except Exception as e:
                raise TransactionAbortedError(f"Operation failed in transaction {self.id}: {str(e)}")
        self.logger.info(f"Transaction {self.id}: Executed all operations")

    async def rollback(self):
        for rollback_operation in reversed(self.rollback_operations):
            try:
                await rollback_operation()
            except Exception as e:
                self.logger.error(f"Rollback operation failed in transaction {self.id}: {str(e)}")
        self.logger.info(f"Transaction {self.id}: Rolled back all operations")

    def release_locks(self):
        for lock in self.locks:
            lock.release()
        self.logger.info(f"Transaction {self.id}: Released all locks")

class TransactionManager:
    def __init__(self, timeout: float = 5.0):
        self.lock = asyncio.Lock()
        self.transactions: Dict[int, Transaction] = {}
        self.next_transaction_id = 1
        self.timeout = timeout
        self.logger = Logger.get_logger()

    async def start_transaction(self) -> Transaction:
        async with self.lock:
            try:
                transaction_id = self.next_transaction_id
                self.next_transaction_id += 1
                transaction = Transaction(transaction_id)
                self.transactions[transaction_id] = transaction
                self.logger.info(f"Started transaction {transaction_id}")
                return transaction
            except Exception as e:
                raise TransactionException(f"Failed to start transaction: {str(e)}")

    async def run_transaction(self, transaction: Transaction):
        try:
            await asyncio.wait_for(self._run_transaction(transaction), timeout=self.timeout)
        except asyncio.TimeoutError:
            await self._handle_deadlock(transaction)
        except Exception as e:
            raise TransactionException(f"Failed to run transaction {transaction.id}: {str(e)}")

    async def _run_transaction(self, transaction: Transaction):
        try:
            await transaction.execute()
            self._commit_transaction(transaction)
        except Exception as e:
            self.logger.error(f"Error in transaction {transaction.id}: {str(e)}")
            await self._rollback_transaction(transaction)
            raise TransactionAbortedError(f"Transaction {transaction.id} aborted: {str(e)}")

    def _commit_transaction(self, transaction: Transaction):
        try:
            transaction.release_locks()
            del self.transactions[transaction.id]
            self.logger.info(f"Committed transaction {transaction.id}")
        except Exception as e:
            raise TransactionException(f"Failed to commit transaction {transaction.id}: {str(e)}")

    async def _rollback_transaction(self, transaction: Transaction):
        try:
            await transaction.rollback()
            transaction.release_locks()
            del self.transactions[transaction.id]
            self.logger.info(f"Rolled back transaction {transaction.id}")
        except Exception as e:
            raise TransactionException(f"Failed to rollback transaction {transaction.id}: {str(e)}")

    async def _handle_deadlock(self, transaction: Transaction):
        try:
            await self._rollback_transaction(transaction)
            self.logger.warning(f"Deadlock detected in transaction {transaction.id}")
            raise DeadlockDetectedError(f"Transaction {transaction.id} aborted due to potential deadlock")
        except Exception as e:
            raise TransactionException(f"Failed to handle deadlock for transaction {transaction.id}: {str(e)}")
