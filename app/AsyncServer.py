import asyncio
import logging
from pathlib import Path
import time
from typing import Any, BinaryIO, List, Dict, Tuple

from app.AsyncHandler import AsyncRequestHandler
from app.utils.rdb_parser import parse_redis_file


class AsyncServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None, dir: str = '', dbfilename: str = ''):
        self.host = host
        self.port = port
        self.replica_server = replica_server
        self.replica_port = replica_port
        self.memory = {}
        self.expiration = {}
        self.streamstore = {}
        self.writers = []
        self.inner_server = None
        self.numacks = 0
        self.config = {"dir": dir, "dbfilename": dbfilename}

    @classmethod
    async def create(cls, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None, dir: str = '', dbfilename: str = ''):
        instance = cls(host, port, replica_server, replica_port, dir, dbfilename)
        if(dir and dbfilename):
            instance.memory, instance.expiration = parse_redis_file(Path(dir) / dbfilename)
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
            #psync_response = await reader.read(1024)
            
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
            #raise ValueError("Failed to receive +FULLRESYNC response from PSYNC command")

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
        
    def get_keys_array(self):
        hash_map, _ = parse_redis_file(Path(self.config["dir"]) / self.config["dbfilename"])
        encoded_keys = self.as_array(hash_map.keys())
        return encoded_keys

    def as_array(self, data: List[str]) -> str:
        encoded_data = []
        encoded_data.append(f"*{len(data)}\r\n")
        
        for element in data:
            encoded_data.append(f"${len(element)}\r\n{element}\r\n")
        
        return ''.join(encoded_data)

 

