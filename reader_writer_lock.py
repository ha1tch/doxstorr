import asyncio

class ReaderWriterLock:
    def __init__(self):
        self._read_ready = asyncio.Event()
        self._read_ready.set()
        self._readers = 0
        self._writers = 0
        self._write_lock = asyncio.Lock()

    async def acquire_read(self):
        await self._read_ready.wait()
        async with self._write_lock:
            self._readers += 1
            if self._readers == 1:
                self._read_ready.clear()

    async def release_read(self):
        async with self._write_lock:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.set()

    async def acquire_write(self):
        async with self._write_lock:
            self._writers += 1
            if self._writers == 1:
                await self._read_ready.wait()

    async def release_write(self):
        self._writers -= 1
        if self._writers == 0:
            self._read_ready.set()

class AsyncLock:
    def __init__(self, lock):
        self.lock = lock

    async def __aenter__(self):
        await self.lock.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.lock.release()

def reader_lock(func):
    async def wrapper(self, *args, **kwargs):
        async with AsyncLock(await self.locks.get(args[0], create=True)):
            await self.lock.acquire_read()
            try:
                return await func(self, *args, **kwargs)
            finally:
                await self.lock.release_read()
    return wrapper

def writer_lock(func):
    async def wrapper(self, *args, **kwargs):
        async with AsyncLock(await self.locks.get(args[0], create=True)):
            await self.lock.acquire_write()
            try:
                return await func(self, *args, **kwargs)
            finally:
                await self.lock.release_write()
    return wrapper
