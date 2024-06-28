from abc import ABC, abstractmethod
import asyncio
import time
from typing import List
from app.utils.constants import DISCARD_WITHOUT_MULTI, EXEC_WITHOUT_MULTI, NON_INT_ERROR, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE
import app.utils.encoding_utils as encoding_utils
import app.utils.stream_utils as stream_utils

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

class RedisCommand(ABC):
    @abstractmethod
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        pass

class KeysCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        keys = handler.server.get_keys_array()
        return keys

class TypeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        if key in handler.memory and (not handler.expiration.get(key) or handler.expiration[key] >= time.time()):
            return "+string\r\n"
        elif key in handler.server.streamstore:
            return "+stream\r\n"
        else:
            return "+none\r\n"

class ConfigCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if len(command) > 1:
            config_params = command[2:]
            response = []
            for param in config_params:
                response.append(param)
                if param in handler.server.config:
                    value = handler.server.config[param]
                    response.append(value)
                else:
                    response.append("(nil)")
            return handler.server.as_array(response)

class WaitCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        max_wait_ms = int(command[2])
        num_replicas = int(command[1])
        for writer in handler.server.writers:
            writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
            await writer.drain()
        
        start_time = time.time()
        while handler.server.numacks < num_replicas and (time.time() - start_time) < (max_wait_ms / 1000):
            print(f"NUMACKS: {handler.server.numacks} num_replicas: {num_replicas} max_wait_ms: {max_wait_ms} time: {time.time()} start_time: {start_time}")
            await asyncio.sleep(0.1)
        print("SENDING BACK", handler.server.numacks)
        return f":{handler.server.numacks}\r\n"

class PingCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return "+PONG\r\n"

class ReplConfCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        writer = handler.writer
        if len(command) > 2 and command[1] == "listening-port":
            handler.server.writers.append(writer)
        elif len(command) > 2 and command[1] == "GETACK":
            response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(handler.offset))}\r\n{handler.offset}\r\n"
            print(f"REPLCONF ACK: {response}")
            return response
        elif len(command) > 2 and command[1] == "ACK":
            print("Incrementing num acks")
            handler.server.numacks += 1
            return ""
        return "+OK\r\n"

class PSyncCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        binary_data = bytes.fromhex(rdb_hex)
        header = f"${len(binary_data)}\r\n"
        handler.writer.write(response.encode())
        handler.writer.write(header.encode())
        handler.writer.write(binary_data)
        await handler.writer.drain()
        handler.server.numacks += 1
        return ""

class InfoCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if command[1].lower() == "replication":
            if handler.replica_server is None:
                master_replid = encoding_utils.generate_random_string(40)
                master_repl_offset = "0"
                payload = f"role:master\nmaster_replid:{master_replid}\nmaster_repl_offset:{master_repl_offset}"
                response = encoding_utils.as_bulk_string(payload)
                return response
            else:
                return "+role:slave\r\n"
        else:
            return "-ERR unknown INFO section\r\n"

class EchoCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return f"+{command[1]}\r\n"

class SetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        handler.memory[command[1]] = command[2]
        if(len(command) > 4 and command[3].upper() == "PX" and command[4].isnumeric()):
            expiration_duration = int(command[4]) / 1000  # Convert milliseconds to seconds
            handler.expiration[command[1]] = time.time() + expiration_duration
        else:
            handler.expiration[command[1]] = None
        handler.server.numacks = 0  
        for writer in handler.server.writers:
            print(f"writing CMD {command} to writer: {writer.get_extra_info('peername')}")
            writer.write(encoding_utils.encode_redis_protocol(command))
            await writer.drain()
        return "+OK\r\n"

class GetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if handler.expiration.get(command[1], None) and handler.expiration[command[1]] < time.time():
            handler.memory.pop(command[1], None)
            handler.expiration.pop(command[1], None)
            return "$-1\r\n"
        else:
            value = handler.memory.get(command[1], None)
            if value:
                return encoding_utils.create_redis_response(value)
            else:
                return "$-1\r\n"
            
class XAddCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_key = command[1]
        stream_id = command[2]
        stream_id = stream_utils.generate_stream_id(stream_key, stream_id, handler.server)
        err_message = stream_utils.validate_stream_id(stream_key, stream_id, handler.server)
        if err_message:
            return err_message
        if stream_key not in handler.server.streamstore:
            handler.server.streamstore[stream_key] = {}
        stream_id_parts = stream_id.split("-")
        entry_number = int(stream_id_parts[0])
        sequence_number = int(stream_id_parts[1])
        if entry_number not in handler.server.streamstore[stream_key]:
            handler.server.streamstore[stream_key][entry_number] = {}

        handler.server.streamstore[stream_key][entry_number][sequence_number] = command[3:]
        return f"${len(stream_id)}\r\n{stream_id}\r\n"
    

class XReadCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_keys, stream_ids = None, None
        if command[1].lower() == "block":
            stream_keys, stream_ids = await stream_utils.block_read(int(command[2]), command, handler.server)        
            
        if not stream_keys or not stream_ids:
            stream_keys, stream_ids = stream_utils._get_stream_keys_and_ids(command, handler.server)
        
        ret_string = f"*{len(stream_keys)}\r\n"
        for stream_key, stream_id in zip(stream_keys, stream_ids):
            ret_string += stream_utils.get_one_xread_response(stream_key, stream_id, handler.server)
        return ret_string
    
class XRangeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_key = command[1]
        lower, upper = command[2], command[3]
        if lower == "-":
            lower = "0-0"

        none_string = "+none\r\n"
        if stream_key not in handler.server.streamstore:
            print(f"Stream key '{stream_key}' not found in streamstore")
            return none_string

        streamstore = handler.server.streamstore[stream_key]

        keys = list(streamstore.keys())
        
        if upper == "+":
            upper = f"{keys[-1]}-{list(streamstore[keys[-1]].keys())[-1]}"
        
        lower_outer, lower_inner = int(lower.split("-")[0]), int(lower.split("-")[1])
        upper_outer, upper_inner = int(upper.split("-")[0]), int(upper.split("-")[1])
        
        start_index, end_index = stream_utils.find_outer_indices(keys, lower_outer, upper_outer)
        print(f"Start index: {start_index}, End index: {end_index}")
        if start_index == -1 or end_index == -1 or start_index >= len(keys) or end_index < 0 or start_index > end_index:
            print("Invalid range indices")
            return none_string
        
        streamstore_start_index = stream_utils.find_inner_start_index(streamstore, keys, start_index, lower_outer, lower_inner)
        streamstore_end_index = stream_utils.find_inner_end_index(streamstore, keys, end_index, upper_outer, upper_inner)
        print(f"Streamstore start index: {streamstore_start_index}, Streamstore end index: {streamstore_end_index}")
        if streamstore_start_index == -1 or streamstore_end_index == -1:
            print("Invalid inner indices")
            return none_string

        elements = stream_utils.extract_elements(streamstore, keys, start_index, end_index, streamstore_start_index, streamstore_end_index)
        ret_string = f"*{len(elements)}\r\n"
        for key, value in elements.items():
            ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{handler.server.as_array(value)}"
        print(f"Ret string: {ret_string}")
        return ret_string

class IncrByCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        increment = command[2]
        value = await encoding_utils.get_value(handler, key)
        if value == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if value == NOT_FOUND_RESPONSE:
            value = "0"
        try:
            increment = int(increment)
            value = int(value)
        except ValueError:
            return NON_INT_ERROR
        handler.memory[key] = str(int(value) + int(increment))
        return f":{handler.memory[key]}\r\n"
        
class IncrCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        incr_by_command = IncrByCommand()
        return await incr_by_command.execute(handler, ["INCRBY", command[1], "1"])


class MultiCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if not handler.command_queue:
            handler.command_queue = asyncio.Queue()
        return "+OK\r\n"

class ExecCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if not handler.command_queue:
            return EXEC_WITHOUT_MULTI

        responses = []
        while not handler.command_queue.empty():
            command = await handler.command_queue.get()
            print(f"EXECUTING COMMAND: {command}")
            command_name = command[0].upper()
            command_to_exec = handler.command_map.get(command_name, UnknownCommand())
            responses.append(await command_to_exec.execute(handler, command))
        handler.command_queue = None
        return encoding_utils.generate_redis_array(lst=responses)

class DiscardCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if not handler.command_queue:
            return DISCARD_WITHOUT_MULTI
        handler.command_queue = None
        return "+OK\r\n"

class UnknownCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return "-ERR unknown command\r\n"