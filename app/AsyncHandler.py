import asyncio
import logging
import re
import time
from typing import Any, List, Dict, Tuple
import random
import string
import bisect
from AsyncServer import AsyncServer

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
            elif cmd_name == "XREAD" and len(cmd) > 2:
                response = await self.handle_xread(cmd)
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
    
    async def handle_xread(self, command: List[str]) -> str:
        
        start_index = 2
        stream_keys, stream_ids = None, None
        if command[1].lower() == "block":
            block_time = int(command[2])
            start_index += 2
            if block_time > 0:
                await asyncio.sleep(block_time / 1000)
                if command[len(command) - 1] == "$":
                    stream_keys = command[start_index:command.index(next(filter(lambda x: re.match(r'\$', x), command)))]
                    stream_ids = [self.get_last_stream_id(stream_key) for stream_key in stream_keys]               
            else:
                found = False
                if command[len(command) - 1] == "$":
                    stream_keys = command[start_index:command.index(next(filter(lambda x: re.match(r'\$', x), command)))]
                    print(f"Stream keys: {stream_keys}")
                    stream_ids = [self.get_last_stream_id(stream_key) for stream_key in stream_keys]  
                while not found:
                    stream_keys = stream_keys or command[start_index:command.index(next(filter(lambda x: re.match(r'\d+-\d+', x), command)))]
                    stream_ids = stream_ids or [x for x in command[command.index(next(filter(lambda x: re.match(r'\d+-\d+', x), command))):] if re.match(r'\d+-\d+', x)]
                    print(f"Stream keys: {stream_keys}\nStream ids: {stream_ids}")
                    for stream_key, stream_id in zip(stream_keys, stream_ids):
                        response = self.get_one_xread_response(stream_key, stream_id)
                        if response != "$-1\r\n":
                            found = True
                            break
                    await asyncio.sleep(0.05)
            
        stream_keys = stream_keys or command[start_index:command.index(next(filter(lambda x: re.match(r'\d+-\d+', x), command)))]
        stream_ids = stream_ids or [x for x in command[command.index(next(filter(lambda x: re.match(r'\d+-\d+', x), command))):] if re.match(r'\d+-\d+', x)]
        
        ret_string = f"*{len(stream_keys)}\r\n"
        for stream_key, stream_id in zip(stream_keys, stream_ids):
            ret_string += self.get_one_xread_response(stream_key, stream_id)
        return ret_string
    
    def get_last_stream_id(self, stream_key: str) -> str:
        if stream_key in self.server.streamstore:
            streamstore = self.server.streamstore[stream_key]
            if streamstore:
                last_entry_number = int(list(streamstore.keys())[-1])
                last_entry = streamstore[last_entry_number]
                last_entry_sequence = int(list(last_entry.keys())[-1])
                return f"{last_entry_number}-{last_entry_sequence}"
        return ""

    def get_one_xread_response(self, stream_key: str, stream_id: str) -> str:
        stream_id_parts = stream_id.split("-")

        entry_number = int(stream_id_parts[0])
        sequence_number = int(stream_id_parts[1])
        none_string = "$-1\r\n"
        
        if stream_key not in self.server.streamstore:
            return none_string
        
        streamstore = self.server.streamstore[stream_key]

        if entry_number in streamstore and sequence_number in streamstore[entry_number]:
            sequence_number += 1 # make it exclusive
        
        keys = list(streamstore.keys())
        
        upper = f"{keys[-1]}-{list(streamstore[keys[-1]].keys())[-1]}"
        
        lower_outer, lower_inner = entry_number, sequence_number
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
        ret_string = f"*2\r\n${len(stream_key)}\r\n{stream_key}\r\n*{len(elements)}\r\n"
        for key, value in elements.items():
            ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{self.server.as_array(value)}"
        print(f"Ret string: {ret_string}")
        return ret_string
    
    async def handle_xrange(self, command: List[str]) -> str:
        stream_key = command[1]
        lower, upper = command[2], command[3]
        if lower == "-":
            lower = "0-0"

        none_string = "+none\r\n"
        if stream_key not in self.server.streamstore:
            print(f"Stream key '{stream_key}' not found in streamstore")
            return none_string

        streamstore = self.server.streamstore[stream_key]

        keys = list(streamstore.keys())
        
        if upper == "+":
            upper = f"{keys[-1]}-{list(streamstore[keys[-1]].keys())[-1]}"
        
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
            ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{self.server.as_array(value)}"
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
            streamstore_keys = list(streamstore[current_key].keys())
            current_elements = streamstore[current_key]
            for i in range(streamstore_start_index, streamstore_end_index + 1):
                ret_dict[f"{current_key}-{streamstore_keys[i]}"] = current_elements[streamstore_keys[i]]
        else:
            for i in range(start_index, end_index + 1):
                current_key = keys[i]
                streamstore_keys = list(streamstore[current_key].keys())
                current_elements = streamstore[current_key]
                for j in range(len(current_elements)):
                    if (i == start_index and j < streamstore_start_index) or (i == end_index and j > streamstore_end_index):
                        continue
                ret_dict[f"{current_key}-{streamstore_keys[j]}"] = current_elements[streamstore_keys[j]]  
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