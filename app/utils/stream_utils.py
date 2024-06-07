import asyncio
import bisect
import re
import time
from typing import Dict, List, Tuple
from .. import AsyncServer

def validate_stream_id(stream_key: str, stream_id: str, server: AsyncServer) -> str:
        
        if(stream_id <= "0-0"):
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        
        if stream_key not in server.streamstore:
            return ""
        
        last_entry_number = int(list(server.streamstore[stream_key].keys())[-1])
        print(f"Last entry number: {last_entry_number}")
        last_entry_sequence = int(list(server.streamstore[stream_key][last_entry_number].keys())[-1])

        current_entry_number = int(stream_id.split("-")[0])
        current_entry_sequence = int(stream_id.split("-")[1])
        
        err_string = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        if current_entry_number < last_entry_number:
            return err_string
        elif current_entry_number == last_entry_number and current_entry_sequence <= last_entry_sequence:
            return err_string
        return ""
    
def generate_stream_id(stream_key: str, stream_id: str, server: AsyncServer) -> str:
    parts = stream_id.split("-")

    if _is_valid_id(parts):
        return stream_id

    if _is_time_star(parts):
        return _generate_time_star_id(stream_key, parts, server)

    if stream_id == "*" or (parts[0] == "*" and parts[1] == "*"):
        return _generate_star_id(stream_key, server)

    return ""

def _is_valid_id(parts: List[str]) -> bool:
    return len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit()

def _is_time_star(parts: List[str]) -> bool:
    return len(parts) == 2 and parts[0].isdigit() and parts[1] == "*"

def _generate_time_star_id(stream_key: str, parts: List[str], server: AsyncServer) -> str:
    parts[0] = int(parts[0])
    sequence_number = _calculate_sequence_number(stream_key, parts[0], server)
    return f"{parts[0]}-{sequence_number}"

def _generate_star_id(stream_key: str, server: AsyncServer) -> str:
    current_time = int(time.time() * 1000)
    sequence_number = _calculate_sequence_number(stream_key, current_time, server)
    return f"{current_time}-{sequence_number}"

def _calculate_sequence_number(stream_key: str, timestamp: int, server: AsyncServer) -> int:
    if stream_key in server.streamstore:
        last_entry_number = list(server.streamstore[stream_key].keys())[-1]
        last_entry_sequence = list(server.streamstore[stream_key][last_entry_number].keys())[-1]
        if last_entry_number < timestamp:
            return 0
        else:
            return last_entry_sequence + 1
    else:
        return 1 if timestamp == 0 else 0
    
    
async def block_read(block_time: int, command: List[str], server: AsyncServer) -> Tuple[List[str], List[str]]:
    stream_keys, stream_ids = _get_stream_keys_and_ids(command, server)
    if block_time > 0:
        await asyncio.sleep(block_time / 1000)
    else:
        found = False
        while not found:
            for stream_key, stream_id in zip(stream_keys, stream_ids):
                response = get_one_xread_response(stream_key, stream_id, server)
                if response != "$-1\r\n":
                    found = True
                    break
            await asyncio.sleep(0.05)
    return stream_keys, stream_ids

def _get_stream_keys_and_ids(command: List[str], server: AsyncServer) -> Tuple[List[str], List[str]]:
    stream_keys, stream_ids = None, None
    start_index = 2
    if command[1].lower() == "block":
        start_index += 2
    if command[len(command) - 1] == "$":
        stream_keys = command[start_index:command.index(next(filter(lambda x: re.match(r'\$', x), command)))] # Rest of the array except last $ is stream_keys
        stream_ids = [get_last_stream_id(stream_key, server) for stream_key in stream_keys]
    else:
        stream_keys = command[start_index:command.index(next(filter(lambda x: re.match(r'\d+-\d+', x), command)))] # We have stream keys until the first stream id
        stream_ids = [x for x in command[start_index:] if re.match(r'\d+-\d+', x)]
    
    return stream_keys, stream_ids


def get_last_stream_id(stream_key: str, server: AsyncServer) -> str:
    if stream_key in server.streamstore:
        streamstore = server.streamstore[stream_key]
        if streamstore:
            last_entry_number = int(list(streamstore.keys())[-1])
            last_entry = streamstore[last_entry_number]
            last_entry_sequence = int(list(last_entry.keys())[-1])
            return f"{last_entry_number}-{last_entry_sequence}"
    return ""

def get_one_xread_response(stream_key: str, stream_id: str, server: AsyncServer) -> str:
    stream_id_parts = stream_id.split("-")

    entry_number = int(stream_id_parts[0])
    sequence_number = int(stream_id_parts[1])
    none_string = "$-1\r\n"
    
    if stream_key not in server.streamstore:
        return none_string
    
    streamstore = server.streamstore[stream_key]

    if entry_number in streamstore and sequence_number in streamstore[entry_number]:
        sequence_number += 1 # make it exclusive
    
    keys = list(streamstore.keys())
    
    upper = f"{keys[-1]}-{list(streamstore[keys[-1]].keys())[-1]}"
    
    lower_outer, lower_inner = entry_number, sequence_number
    upper_outer, upper_inner = int(upper.split("-")[0]), int(upper.split("-")[1])
    
    start_index, end_index = find_outer_indices(keys, lower_outer, upper_outer)
    print(f"Start index: {start_index}, End index: {end_index}")
    if start_index == -1 or end_index == -1 or start_index >= len(keys) or end_index < 0 or start_index > end_index:
        print("Invalid range indices")
        return none_string
    
    streamstore_start_index = find_inner_start_index(streamstore, keys, start_index, lower_outer, lower_inner)
    streamstore_end_index = find_inner_end_index(streamstore, keys, end_index, upper_outer, upper_inner)
    print(f"Streamstore start index: {streamstore_start_index}, Streamstore end index: {streamstore_end_index}")
    if streamstore_start_index == -1 or streamstore_end_index == -1:
        print("Invalid inner indices")
        return none_string

    elements = extract_elements(streamstore, keys, start_index, end_index, streamstore_start_index, streamstore_end_index)
    ret_string = f"*2\r\n${len(stream_key)}\r\n{stream_key}\r\n*{len(elements)}\r\n"
    for key, value in elements.items():
        ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{server.as_array(value)}"
    print(f"Ret string: {ret_string}")
    return ret_string

def find_outer_indices(keys: List[str], lower_outer: str, upper_outer: str) -> Tuple[int, int]:
    start_index = bisect.bisect_left(keys, lower_outer)
    end_index = bisect.bisect_right(keys, upper_outer) - 1
    if start_index >= len(keys) or end_index < 0:
        return -1, -1
    return start_index, end_index

def find_inner_start_index(streamstore: Dict[str, Dict[str, str]], keys: List[str], start_index: int, lower_outer: str, lower_inner: str) -> int:
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

def find_inner_end_index(streamstore: Dict[str, Dict[str, str]], keys: List[str], end_index: int, upper_outer: str, upper_inner: str) -> int:
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

def extract_elements(streamstore: Dict[str, List[str]], keys: List[str], start_index: int, end_index: int, streamstore_start_index: int, streamstore_end_index: int) -> Dict[str, List[str]]:
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