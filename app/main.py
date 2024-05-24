import argparse
import asyncio
import logging
import time

class AsyncServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.memory = {}
        self.expiration = {}

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
        request_handler = AsyncRequestHandler(reader, writer, self.memory, self.expiration)
        await request_handler.process_request()

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, memory: dict, expiration: dict):
        self.reader = reader
        self.writer = writer
        self.memory = memory
        self.expiration = expiration

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
            response = await self.handle_ping(command)
        elif cmd_name == "ECHO" and len(command) > 1:
            response = await self.handle_echo(command)
        elif cmd_name == "SET" and len(command) > 2:
            response = await self.handle_set(command)
        elif cmd_name == "GET" and len(command) > 1:
            response = await self.handle_get(command)
        elif cmd_name == "INFO" and len(command) > 1:
            response = await self.handle_info(command)
        else:
            response = await self.handle_unknown(command)

        self.writer.write(response.encode())
        await self.writer.drain()

    async def handle_info(self, command: List[str]) -> str:
        if command[1].lower() == "replication":
            return "+role:master\r\n"
        else:
            return "-ERR unknown INFO section\r\n"

    async def handle_echo(self, command: List[str]) -> str:
        return f"+{command[1]}\r\n"

    async def handle_set(self, command: List[str]) -> str:
        self.memory[command[1]] = command[2]
        if(len(command) > 4 and command[3].upper() == "PX" and command[4].isnumeric()):
            expiration_duration = int(command[4]) / 1000  # Convert milliseconds to seconds
            self.expiration[command[1]] = time.time() + expiration_duration
        else:
            self.expiration[command[1]] = None  
        return "+OK\r\n"

    async def handle_get(self, command: List[str]) -> str:
        if self.expiration.get(command[1], None) and self.expiration[command[1]] < time.time():
            self.memory.pop(command[1], None)
            self.expiration.pop(command[1], None)
            return "$-1\r\n"
        else:
            value = self.memory.get(command[1], None)
            if value:
                return f"${len(value)}\r\n{value}\r\n"
            else:
                return "$-1\r\n"

    async def handle_unknown(self, command: List[str]) -> str:
        return "-ERR unknown command\r\n"

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
    parser = argparse.ArgumentParser(description="Run the Async Redis-like server.")
    parser.add_argument('--port', type=int, default=6379, help='Port to run the server on')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    server = AsyncServer(port=args.port)
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
