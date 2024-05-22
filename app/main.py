import asyncio
import logging

class AsyncServer:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.accept_connections, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logging.info(f"Server started at http://{addr[0]}:{addr[1]}")

        async with server:
            await server.serve_forever()

    async def accept_connections(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Connected by {addr}")
        request_handler = AsyncRequestHandler(reader, writer)
        await request_handler.process_request()

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def process_request(self) -> None:
        request = await self.reader.read(1024)
        logging.info(f"Request: {request}")
        await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        redis_ping = b"*1\r\n$4\r\nPING\r\n"
        redis_pong = b"+PONG\r\n"

        if request == redis_ping:
            global ping_count
            ping_count += 1
            logging.info(f"Received PING {ping_count}")
            self.writer.write(redis_pong)
            await self.writer.drain()
        else:
            logging.info("Received unexpected data")
        
        self.writer.close()
        await self.writer.wait_closed()

async def main() -> None:
    global ping_count
    ping_count = 0
    logging.basicConfig(level=logging.INFO)
    server = AsyncServer()
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
