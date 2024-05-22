import asyncio
import logging

class AsyncServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
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
        while True:
            request = await self.reader.read(1024)
            if not request:
                break
            logging.info(f"Request: {request}")
            await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        command = self.parse_redis_protocol(request)
        if not command:
            logging.info("Received invalid data")
            return

        cmd_name = command[0].upper()  # Command names are case-insensitive
        if cmd_name == "PING":
            global ping_count
            ping_count += 1
            logging.info(f"Received PING {ping_count}")
            response = "+PONG\r\n"
        elif cmd_name == "ECHO" and len(command) > 1:
            response = f"+{command[1]}\r\n"
        else:
            response = "-ERR unknown command\r\n"

        self.writer.write(response.encode())
        await self.writer.drain()

    def parse_redis_protocol(self, data: bytes):
        try:
            parts = data.split(b'\r\n')
            if not parts or parts[0][0] != ord(b'*'):
                return None
            
            num_elements = int(parts[0][1:])
            elements = []
            index = 1
            
            for _ in range(num_elements):
                if parts[index][0] != ord(b'$'):
                    return None
                
                length = int(parts[index][1:])
                index += 1
                elements.append(parts[index].decode('utf-8'))
                index += 1

            return elements
        except (IndexError, ValueError):
            return None

async def main() -> None:
    global ping_count
    ping_count = 0
    logging.basicConfig(level=logging.INFO)
    server = AsyncServer()
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
