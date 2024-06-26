import random
import string
import time
from typing import List

from app.AsyncHandler import AsyncRequestHandler
from app.utils.constants import NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE


def generate_redis_array(string: str, lst: List[str]) -> str:
    redis_array = []
    redis_array.append(f"*2\r\n${len(string)}\r\n{string}\r\n")
    redis_array.append(f"*{len(lst)}\r\n")
    for element in lst:
        redis_array.append(f"${len(element)}\r\n{element}\r\n")
    return ''.join(redis_array)

def as_bulk_string(payload: str) -> str:
    return f"${len(payload)}\r\n{payload}\r\n"

def encode_redis_protocol(data: List[str]) -> bytes:
    encoded_data = []
    encoded_data.append(f"*{len(data)}\r\n")
    
    for element in data:
        encoded_data.append(f"${len(element)}\r\n{element}\r\n")
    
    return ''.join(encoded_data).encode()

def parse_redis_protocol(data: bytes):
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

    
def generate_random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


async def get_value(handler: 'AsyncRequestHandler', key: str) -> None|str:
    if handler.expiration.get(key, None) and handler.expiration[key] < time.time():
        handler.memory.pop(key, None)
        handler.expiration.pop(key, None)
        return None
    else:
        value = handler.memory.get(key, None)
        if value is None:
            return NOT_FOUND_RESPONSE
        elif not isinstance(value, str):
            return WRONG_TYPE_RESPONSE
        else:
            return value