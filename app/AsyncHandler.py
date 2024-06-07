
import asyncio
import logging

from typing import List
import random
import string

from typing import TYPE_CHECKING

import app.commands.commands as commands

if TYPE_CHECKING:
    from .AsyncServer import AsyncServer

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: 'AsyncServer'):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.memory = server.memory
        self.expiration = server.expiration
        self.replica_server = server.replica_server
        self.replica_port = server.replica_port
        self.offset = 0

    async def process_request(self) -> None:
        while True:
            request = await self.reader.read(1024)
            if not request:
                break
            logging.info(f"Request: {request}")
            await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        commands, lengths = self.parse_redis_protocol(request)
        if not commands:
            logging.info("Received invalid data")
            return

        for index, cmd in enumerate(commands):
            cmd_name = cmd[0].upper()  # Command names are case-insensitive
            if cmd_name == "PING":
                response = await commands.PingCommand().execute(self, cmd)
            elif cmd_name == "ECHO" and len(cmd) > 1:
                response = await commands.EchoCommand().execute(self, cmd)
            elif cmd_name == "SET" and len(cmd) > 2:
                response = await commands.SetCommand().execute(self, cmd)
            elif cmd_name == "GET" and len(cmd) > 1:
                response = await commands.GetCommand().execute(self, cmd)
            elif cmd_name == "INFO" and len(cmd) > 1:
                response = await commands.InfoCommand().execute(self, cmd)
            elif cmd_name == "REPLCONF":
                response = await commands.ReplConfCommand().execute(self, cmd, self.writer)
            elif cmd_name == "PSYNC":
                response = await commands.PSyncCommand().execute(self, cmd)
            elif cmd_name == "WAIT" and len(cmd) > 2:
                response = await commands.WaitCommand().execute(self, cmd)
            elif cmd_name == "CONFIG" and len(cmd) > 1:
                response = await commands.ConfigCommand().execute(self, cmd)
            elif cmd_name == "KEYS":
                response = await commands.KeysCommand().execute(self, cmd)
            elif cmd_name == "TYPE" and len(cmd) > 1:
                response = await commands.TypeCommand().execute(self, cmd)
            elif cmd_name == "XADD" and len(cmd) > 3:
                response = await commands.XAddCommand().execute(self, cmd)
            elif cmd_name == "XRANGE" and len(cmd) > 3:
                response = await commands.XRangeCommand().execute(self, cmd)
            elif cmd_name == "XREAD" and len(cmd) > 2:
                response = await commands.XReadCommand().execute(self, cmd)
            else:
                response = await commands.UnknownCommand().execute(self, cmd)

            if self.replica_server is not None and self.writer.get_extra_info("peername")[1] == self.replica_port:
                if response.startswith("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK"):
                    self.writer.write(response.encode())
                    await self.writer.drain()
                self.offset += lengths[index]
            else:
                if response:
                    print(f"sending response: {response} to {self.writer.get_extra_info('peername')} command: {cmd}")
                    self.writer.write(response.encode())
                    await self.writer.drain()
                self.offset += lengths[index]
        
    def generate_redis_array(self, string: str, lst: List[str]) -> str:
        redis_array = []
        redis_array.append(f"*2\r\n${len(string)}\r\n{string}\r\n")
        redis_array.append(f"*{len(lst)}\r\n")
        for element in lst:
            redis_array.append(f"${len(element)}\r\n{element}\r\n")
        return ''.join(redis_array)

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
            data = data[data.index(b'*'):]
            print(data)
            parts = data.split(b'\r\n')
            commands = []
            lengths = []  # New array to store the lengths of the substrings used for commands
            offset = 0  # Variable to keep track of the offset
            index = 0
            while index < len(parts) - 1:
                if parts[index] and parts[index][0] == ord(b'*'):
                    num_elements = int(parts[index][1:])
                    offset += len(str(num_elements)) + 3  # Add the length of the number of elements and the length of the * and \r\n characters
                    index += 1
                    elements = []
                    for _ in range(num_elements):
                        if parts[index] and parts[index][0] == ord(b'$'):
                            element_length = int(parts[index][1:])
                            offset += len(str(element_length)) + 3  # Add the length of the element length and the length of the $ and \r\n characters
                            index += 1
                            element = parts[index].decode('utf-8')
                            elements.append(element)
                            index += 1
                            offset += element_length + 2 
                    commands.append(elements)
                    lengths.append(offset)  # Store the offset as the length of the substring used for the command
                    offset = 0
                else:
                    index += 1

            print("COMMANDS: ", commands)
            print("LENGTHS: ", lengths)
            return commands, lengths  # Return the commands and lengths arrays
        except (IndexError, ValueError):
            return [], []  # Return empty arrays if there was an error

        
    def generate_random_string(self, length: int) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))