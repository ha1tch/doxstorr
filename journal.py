import asyncio
import json
from typing import Dict, Any, List

class Journal:
    def __init__(self, filename: str):
        self.filename = filename
        self.lock = asyncio.Lock()

    async def log_operation(self, operation: str, data: Dict[str, Any]):
        async with self.lock:
            try:
                entry = json.dumps({"operation": operation, "data": data})
                with open(self.filename, "a") as f:
                    f.write(entry + "\n")
            except IOError as e:
                print(f"Error logging operation: {e}")

    async def recover(self) -> List[Dict[str, Any]]:
        operations = []
        try:
            with open(self.filename, "r") as f:
                for line in f:
                    operations.append(json.loads(line))
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error recovering from journal: {e}")
        return operations

    async def clear(self):
        async with self.lock:
            try:
                open(self.filename, "w").close()
            except IOError as e:
                print(f"Error clearing journal: {e}")
