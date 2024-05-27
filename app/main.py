import argparse
import asyncio
import base64
import logging
import time
from typing import List
import random
import string

class AsyncServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None):
        self.host = host
        self.port = port
        self.replica_server = replica_server
        self.replica_port = replica_port
        self.memory = {}
        self.expiration = {}
        self.writers = []
        self.inner_server = None

    @classmethod
    async def create(cls, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None):
        instance = cls(host, port, replica_server, replica_port)
        instance.inner_server = await instance.start()
        
        if replica_server is not None and replica_port is not None:
            reader, writer = await asyncio.open_connection(replica_server, replica_port)
            response = await instance.send_ping(reader, writer)
            if response != "+PONG\r\n":
                raise ValueError("Failed to receive PONG from replica server")
            

            await instance.send_replconf_command(reader, writer, port)
            await instance.send_additional_replconf_command(reader, writer)
            await instance.send_psync_command(reader, writer)
            await asyncio.create_task(instance.accept_connections(reader, writer))
            #writer.close()
            #await writer.wait_closed()
        async with instance.inner_server as server:
            print("SERVER STARTED")
            await server.serve_forever()
            
        return instance

    async def send_replconf_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int) -> None:
        replconf_command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + str(port) + "\r\n"
        writer.write(replconf_command.encode())
        await writer.drain()
        replconf_response = await reader.read(1024)
        if replconf_response.decode() != "+OK\r\n":
            raise ValueError("Failed to receive +OK response from REPLCONF command")

    async def send_additional_replconf_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        replconf_command_additional = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        writer.write(replconf_command_additional.encode())
        await writer.drain()
        replconf_response_additional = await reader.read(1024)
        if replconf_response_additional.decode() != "+OK\r\n":
            raise ValueError("Failed to receive +OK response from additional REPLCONF command")

    async def send_psync_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        writer.write(psync_command.encode())
        await writer.drain()
        #psync_response = await reader.read(1024)
        #if not psync_response.startswith(b"+FULLRESYNC"):
        #    raise ValueError("Failed to receive +FULLRESYNC response from PSYNC command")

    async def send_ping(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str:
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        return response.decode()

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.accept_connections, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logging.info(f"Server started at http://{addr[0]}:{addr[1]}")
        return server

        

    async def accept_connections(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Connected by {addr}")
        request_handler = AsyncRequestHandler(reader, writer, self)
        await request_handler.process_request()

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: AsyncServer):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.memory = server.memory
        self.expiration = server.expiration
        self.replica_server = server.replica_server
        self.replica_port = server.replica_port

    async def process_request(self) -> None:
        while True:
            request = await self.reader.read(1024)
            if not request:
                break
            logging.info(f"Request: {request}")
            await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        commands = self.parse_redis_protocol(request)
        if not commands:
            logging.info("Received invalid data")
            return

        responses = []
        for cmd in commands:
            cmd_name = cmd[0].upper()  # Command names are case-insensitive
            if cmd_name == "PING":
                response = await self.handle_ping()
            elif cmd_name == "ECHO" and len(cmd) > 1:
                response = await self.handle_echo(cmd)
            elif cmd_name == "SET" and len(cmd) > 2:
                response = await self.handle_set(cmd)
            elif cmd_name == "GET" and len(cmd) > 1:
                response = await self.handle_get(cmd)
            elif cmd_name == "INFO" and len(cmd) > 1:
                response = await self.handle_info(cmd)
            elif cmd_name == "REPLCONF":
                response = await self.handle_replconf(cmd, self.writer)
            elif cmd_name == "PSYNC":
                response = await self.handle_psync(cmd)
            else:
                response = await self.handle_unknown()
            if response:
                responses.append(response)
        
        if responses:
            if self.replica_server is not None:
                responses = [response for response in responses if response == "+REPLCONF ACK 0\r\n"]
            self.writer.write(''.join(responses).encode())
            await self.writer.drain()

    async def handle_ping(self) -> str:
        return "+PONG\r\n"

    async def handle_replconf(self, command: List[str], writer: asyncio.StreamWriter) -> str:
        if len(command) > 2 and command[1] == "listening-port":
            self.server.writers.append(writer)
        elif len(command) > 2 and command[1] == "getack":
            response = "+REPLCONF ACK 0\r\n"
            return response
        return "+OK\r\n"

    async def handle_psync(self, command: List[str]) -> str:
        response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        binary_data = bytes.fromhex(rdb_hex)
        header = f"${len(binary_data)}\r\n"
        self.writer.write(response.encode())
        self.writer.write(header.encode())
        self.writer.write(binary_data)
        await self.writer.drain()
        return ""

    async def handle_info(self, command: List[str]) -> str:
        if command[1].lower() == "replication":
            if self.replica_server is None:
                master_replid = self.generate_random_string(40)
                master_repl_offset = "0"
                payload = f"role:master\nmaster_replid:{master_replid}\nmaster_repl_offset:{master_repl_offset}"
                response = self.as_bulk_string(payload)
                return response
            else:
                return "+role:slave\r\n"
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
        for writer in self.server.writers:
            writer.write(self.encode_redis_protocol(command))
            await writer.drain()
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

    async def handle_unknown(self) -> str:
        return "-ERR unknown command\r\n"

    def as_bulk_string(self, payload: str) -> str:
        return f"${len(payload)}\r\n{payload}\r\n"
    
    def encode_redis_protocol(self, data: List[str]) -> bytes:
        encoded_data = []
        encoded_data.append(f"*{len(data)}\r\n")
        
        for element in data:
            encoded_data.append(f"${len(element)}\r\n{element}\r\n")
        
        return ''.join(encoded_data).encode()
    def parse_redis_protocol(self, data: bytes):
        try:
            parts = data.split(b'\r\n')
            commands = []
            index = 0
            while index < len(parts) - 1:
                if parts[index] and parts[index][0] == ord(b'*'):
                    num_elements = int(parts[index][1:])
                    index += 1
                    elements = []
                    for _ in range(num_elements):
                        if parts[index] and parts[index][0] == ord(b'$'):
                            element_length = int(parts[index][1:])
                            index += 1
                            element = parts[index].decode('utf-8')
                            elements.append(element)
                            index += 1
                    commands.append(elements)
                else:
                    index += 1
            print("COMMANDS: ", commands)
            return commands
        except (IndexError, ValueError):
            return None

        
    def generate_random_string(self, length: int) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def main() -> None:
    global ping_count
    ping_count = 0
    
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Run the Async Redis-like server.")
    parser.add_argument('--port', type=int, default=6379, help='Port to run the server on')
    parser.add_argument('--replicaof', type=str, default=None, help='Replicate data from a master server')
    args = parser.parse_args()
    replica_server, replica_port = None, None

    if args.replicaof:
        replica_info = args.replicaof.split()
        if len(replica_info) != 2:
            raise ValueError("Invalid replicaof argument. Must be in the format 'server port'")
        replica_server = replica_info[0]
        replica_port = int(replica_info[1])
        # Use replica_server and replica_port as needed

    logging.basicConfig(level=logging.INFO)
    server = await AsyncServer.create(port=args.port, replica_server=replica_server, replica_port=replica_port)
    #await server.start()

if __name__ == "__main__":
    asyncio.run(main())

