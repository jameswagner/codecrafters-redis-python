import argparse
import asyncio
import base64
import logging
from pathlib import Path
import time
from typing import Any, BinaryIO, List, Dict, Tuple
import random
import string
import bisect

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
            instance.memory, instance.expiration = instance.parse_redis_file(Path(dir) / dbfilename)
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
        hash_map, _ = self.parse_redis_file(Path(self.config["dir"]) / self.config["dbfilename"])
        encoded_keys = self.as_array(hash_map.keys())
        return encoded_keys

    def as_array(self, data: List[str]) -> str:
        encoded_data = []
        encoded_data.append(f"*{len(data)}\r\n")
        
        for element in data:
            encoded_data.append(f"${len(element)}\r\n{element}\r\n")
        
        return ''.join(encoded_data)

    def parse_redis_file(self, file_path: str) -> Tuple[List[Dict[str, str]]]:
        hash_map = {}
        expiry_times = {}
        try:
            with open(file_path, "rb") as file:
                print("File Path:", file_path)
                for line in file:
                    print(line)
        except FileNotFoundError:
            return hash_map, expiry_times

        
        with open(file_path, "rb") as file:
            magic_string = file.read(5)
            rdb_version = file.read(4)
            # Check magic string and rdb version
            print(f"Magic String: {magic_string}, RDB Version: {rdb_version}")
            while True:
                byte = file.read(1)
                if not byte:
                    return {}
                if byte == b"\xFE":
                    for _ in range(4):
                        print(file.read(1))
                    break
            while True:
                field_type = file.read(1)
                expiry_time = 0
                if field_type == b"\xFE":
                    # Database selector field
                    db_number = int.from_bytes(file.read(1), byteorder="little")
                    # Skip resizedb field
                elif field_type == b"\xfb":
                    file.read(2)
                elif field_type == b"\xFD":
                    # Key-value pair with expiry time in seconds
                    expiry_time = int.from_bytes(file.read(4), byteorder="little")
                    value_type = file.read(1)
                    key_length = int.from_bytes(file.read(1), byteorder="little")
                    key = file.read(key_length)
                    value = self.read_encoded_value(file, value_type)
                    # Check if key has expired
                    if expiry_time > 0 and expiry_time < time.time():
                        key = None
                elif field_type == b"\xFC":
                    # Key-value pair with expiry time in milliseconds
                    expiry_time = int.from_bytes(file.read(8), byteorder="little")
                    
                    value_type = file.read(1)
                    key_length = int.from_bytes(file.read(1), byteorder="little")
                    key = file.read(key_length)
                    value = self.read_encoded_value(file, value_type)
                    # Check if key has expired
                    if expiry_time > 0 and expiry_time < time.time() * 1000:
                        key = None
                    expiry_time = expiry_time / 1000
                    print("got expiry time", expiry_time)
                        
                elif field_type == b"\xFF":
                    # End of RDB file
                    break
                else:
                    # Key-value pair without expiry
                    value_type = field_type
                    key_length = int.from_bytes(file.read(1), byteorder="little")
                    key = file.read(key_length)
                    value = self.read_encoded_value(file, value_type)
                    print(f"Key length: {key_length} Key: {key}, Value: {value}")
                if key and value:
                    hash_map[key.decode()] = value.decode()
                if key and expiry_time:
                    expiry_times[key.decode()] = expiry_time
        print("HASH MAP:", hash_map)
        print("EXPIRY TIMES:", expiry_times)
        return hash_map, expiry_times

    def read_encoded_value(self, file: BinaryIO, value_type: bytes) -> Any:
        if value_type == b"\x00":
            # String value
            value_length = int.from_bytes(file.read(1), byteorder="little")
            return file.read(value_length)        
        else:
            # Unknown value type
            return None

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: AsyncServer):
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
            elif cmd_name == "WAIT" and len(cmd) > 2:
                response = await self.handle_wait(cmd)
            elif cmd_name == "CONFIG" and len(cmd) > 1:
                response = await self.handle_config_get(cmd)
            elif cmd_name == "KEYS":
                response = await self.handle_keys(cmd)
            elif cmd_name == "TYPE" and len(cmd) > 1:
                response = await self.handle_type(cmd)
            elif cmd_name == "XADD" and len(cmd) > 3:
                response = await self.handle_xadd(cmd)
            elif cmd_name == "XRANGE" and len(cmd) > 3:
                response = await self.handle_xrange(cmd)
            else:
                response = await self.handle_unknown()

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
                
    async def handle_keys(self, command: List[str]) -> str:
        keys = self.server.get_keys_array()
        return keys
    
    
    def validate_stream_id(self, stream_key: str, stream_id: str) -> string:
        
        if(stream_id <= "0-0"):
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        
        if stream_key not in self.server.streamstore:
            return ""
        
        last_entry_number = int(list(self.server.streamstore[stream_key].keys())[-1])
        print(f"Last entry number: {last_entry_number}")
        last_entry_sequence = int(list(self.server.streamstore[stream_key][last_entry_number].keys())[-1])

        current_entry_number = int(stream_id.split("-")[0])
        current_entry_sequence = int(stream_id.split("-")[1])
        
        err_string = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        if current_entry_number < last_entry_number:
            return err_string
        elif current_entry_number == last_entry_number and current_entry_sequence <= last_entry_sequence:
            return err_string
        return ""
    
    def generate_stream_id(self, stream_key: str, stream_id: str) -> str:
        parts = stream_id.split("-")
        if parts[0].isdigit() and parts[1].isdigit():
            return stream_id
        if parts[0].isdigit() and parts[1] == "*":
            parts[0] = int(parts[0])
            if stream_key in self.server.streamstore:
                last_entry_number = list(self.server.streamstore[stream_key].keys())[-1]
                last_entry_sequence = list(self.server.streamstore[stream_key][last_entry_number].keys())[-1]
                if(last_entry_number < parts[0]):
                    sequence_number = 0
                else:
                    sequence_number = last_entry_sequence + 1
            else:
                sequence_number = 1 if parts[0] == 0 else 0
            return f"{parts[0]}-{sequence_number}"
        if stream_id == "*" or (parts[0] == "*" and parts[1] == "*"):
            current_time = int(time.time() * 1000)
            sequence_number = 0 
            if stream_key in self.server.streamstore:
                last_entry_number = list(self.server.streamstore[stream_key].keys())[-1]
                last_entry_sequence = list(self.server.streamstore[stream_key][last_entry_number].keys())[-1]
                if(last_entry_number == current_time):
                    sequence_number = last_entry_sequence + 1
            stream_id = f"{current_time}-{sequence_number}"
            return stream_id
        return ""

    async def handle_xadd(self, command: List[str]) -> str:
        stream_key = command[1]
        stream_id = command[2]
        stream_id = self.generate_stream_id(stream_key, stream_id)
        err_message = self.validate_stream_id(stream_key, stream_id)
        if err_message:
            return err_message
        if stream_key not in self.server.streamstore:
            self.server.streamstore[stream_key] = {}
        stream_id_parts = stream_id.split("-")
        entry_number = int(stream_id_parts[0])
        sequence_number = int(stream_id_parts[1])
        if entry_number not in self.server.streamstore[stream_key]:
            self.server.streamstore[stream_key][entry_number] = {}

        self.server.streamstore[stream_key][entry_number][sequence_number] = command[3:]
        return f"${len(stream_id)}\r\n{stream_id}\r\n"
    
    async def handle_xrange(self, command: List[str]) -> str:
        stream_key = command[1]
        lower, upper = command[2], command[3]
        none_string = "+none\r\n"
        if stream_key not in self.server.streamstore:
            print(f"Stream key '{stream_key}' not found in streamstore")
            return none_string

        streamstore = self.server.streamstore[stream_key]
        keys = list(streamstore.keys())
        
        lower_outer, lower_inner = int(lower.split("-")[0]), int(lower.split("-")[1])
        upper_outer, upper_inner = int(upper.split("-")[0]), int(upper.split("-")[1])
        
        start_index, end_index = self.find_outer_indices(keys, lower_outer, upper_outer)
        print(f"Start index: {start_index}, End index: {end_index}")
        if start_index == -1 or end_index == -1 or start_index >= len(keys) or end_index < 0 or start_index > end_index:
            print("Invalid range indices")
            return none_string
        
        streamstore_start_index = self.find_inner_start_index(streamstore, keys, start_index, lower_outer, lower_inner)
        streamstore_end_index = self.find_inner_end_index(streamstore, keys, end_index, upper_outer, upper_inner)
        print(f"Streamstore start index: {streamstore_start_index}, Streamstore end index: {streamstore_end_index}")
        if streamstore_start_index == -1 or streamstore_end_index == -1:
            print("Invalid inner indices")
            return none_string

        elements = self.extract_elements(streamstore, keys, start_index, end_index, streamstore_start_index, streamstore_end_index)
        ret_string = f"*{len(elements)}\r\n"
        for key, value in elements.items():
            ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{self.server.as_array(value)}\r\n"
        print(f"Ret string: {ret_string}")
        return ret_string
    
    def generate_redis_array(self, string: str, lst: List[str]) -> str:
        redis_array = []
        redis_array.append(f"*2\r\n${len(string)}\r\n{string}\r\n")
        redis_array.append(f"*{len(lst)}\r\n")
        for element in lst:
            redis_array.append(f"${len(element)}\r\n{element}\r\n")
        return ''.join(redis_array)

    def find_outer_indices(self, keys: List[str], lower_outer: str, upper_outer: str) -> Tuple[int, int]:
        start_index = bisect.bisect_left(keys, lower_outer)
        end_index = bisect.bisect_right(keys, upper_outer) - 1
        if start_index >= len(keys) or end_index < 0:
            return -1, -1
        return start_index, end_index

    def find_inner_start_index(self, streamstore: Dict[str, Dict[str, str]], keys: List[str], start_index: int, lower_outer: str, lower_inner: str) -> int:
        if keys[start_index] == lower_outer:
            streamstore_start_index = bisect.bisect_left(list(streamstore[keys[start_index]].keys()), lower_inner)
            if streamstore_start_index == len(streamstore[keys[start_index]]):
                start_index += 1
                if start_index >= len(keys):
                    return -1
                streamstore_start_index = 0
        else:
            streamstore_start_index = 0
        return streamstore_start_index

    def find_inner_end_index(self, streamstore: Dict[str, Dict[str, str]], keys: List[str], end_index: int, upper_outer: str, upper_inner: str) -> int:
        if keys[end_index] == upper_outer:
            streamstore_end_index = bisect.bisect_right(list(streamstore[keys[end_index]].keys()), upper_inner) - 1
            if streamstore_end_index == -1:
                end_index -= 1
                if end_index < 0:
                    return -1
                streamstore_end_index = len(streamstore[keys[end_index]]) - 1
        else:
            streamstore_end_index = len(streamstore[keys[end_index]]) - 1
        return streamstore_end_index

    def extract_elements(self, streamstore: Dict[str, List[str]], keys: List[str], start_index: int, end_index: int, streamstore_start_index: int, streamstore_end_index: int) -> Dict[str, List[str]]:
        ret_dict = {}
        print(f"streamstore: {streamstore}, keys: {keys}, start_index: {start_index}, end_index: {end_index}, streamstore_start_index: {streamstore_start_index}, streamstore_end_index: {streamstore_end_index}")
        if start_index == end_index:
            current_key = keys[start_index]
            current_elements = streamstore[current_key]
            for i in range(streamstore_start_index, streamstore_end_index + 1):
                ret_dict[f"{current_key}-{i}"] = current_elements[i]
        else:
            for i in range(start_index, end_index + 1):
                current_key = keys[i]
                current_elements = streamstore[current_key]
                for j in range(len(current_elements)):
                    if (i == start_index and j < streamstore_start_index) or (i == end_index and j > streamstore_end_index):
                        continue
                ret_dict[f"{current_key}-{j}"] = current_elements[j]  
        return ret_dict
    
    async def handle_type(self, command: List[str]) -> str:
        key = command[1]
        if key in self.memory and (not self.expiration.get(key) or self.expiration[key] >= time.time()):
            return "+string\r\n"
        elif key in self.server.streamstore:
            return "+stream\r\n"
        else:
            return "+none\r\n"

    async def handle_config_get(self, command: List[str]) -> str:
        if len(command) > 1:
            config_params = command[2:]
            response = []
            for param in config_params:
                response.append(param)
                if param in self.server.config:
                    value = self.server.config[param]
                    response.append(value)
                else:
                    response.append("(nil)")
            return self.server.as_array(response)

    async def handle_wait(self, command: List[str]) -> str:
        max_wait_ms = int(command[2])
        num_replicas = int(command[1])
        
        
        for writer in self.server.writers:
            writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
            await writer.drain()
        
        start_time = time.time()
        while self.server.numacks < num_replicas and (time.time() - start_time) < (max_wait_ms / 1000):
            print(f"NUMACKS: {self.server.numacks} num_replicas: {num_replicas} max_wait_ms: {max_wait_ms} time: {time.time()} start_time: {start_time}")
            await asyncio.sleep(0.1)
        print("SENDING BACK", self.server.numacks)
        return f":{self.server.numacks}\r\n"

    async def handle_ping(self) -> str:
        return "+PONG\r\n"

    async def handle_replconf(self, command: List[str], writer: asyncio.StreamWriter) -> str:
        if len(command) > 2 and command[1] == "listening-port":
            self.server.writers.append(writer)
        elif len(command) > 2 and command[1] == "GETACK":
            response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(self.offset))}\r\n{self.offset}\r\n"
            print(f"REPLCONF ACK: {response}")
            return response
        elif len(command) > 2 and command[1] == "ACK":
            print("Incrementing num acks")
            self.server.numacks += 1
            return ""
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
        self.server.numacks += 1
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
        self.server.numacks = 0  
        for writer in self.server.writers:
            print(f"writing CMD {command} to writer: {writer.get_extra_info('peername')}")
            writer.write(self.encode_redis_protocol(command))
            await writer.drain()
        return "+OK\r\n"
        #return None

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
    


async def main() -> None:
    global ping_count
    ping_count = 0
    
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Run the Async Redis-like server.")
    parser.add_argument('--port', type=int, default=6379, help='Port to run the server on')
    parser.add_argument('--replicaof', type=str, default=None, help='Replicate data from a master server')
    parser.add_argument('--dir', type=str, default='', help='Path to the directory where the RDB file is stored')
    parser.add_argument('--dbfilename', type=str, default='', help='Name of the RDB file')
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
    server = await AsyncServer.create(port=args.port, replica_server=replica_server, replica_port=replica_port, dir=args.dir, dbfilename=args.dbfilename)
    #await server.start()

if __name__ == "__main__":
    asyncio.run(main())

