import random
import string
import time
from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler
from app.utils.constants import EMPTY_ARRAY_RESPONSE, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE


def create_redis_response(response: Any) -> str:
    if response is None:
        return EMPTY_ARRAY_RESPONSE
    elif isinstance(response, str):
        if response == "OK":
            return f"+{response}\r\n"
        elif response.startswith("-ERR" or response.startswith("+ERR")):
            return f"{response}\r\n"
        return f"${len(response)}\r\n{response}\r\n"
    elif isinstance(response, int):
        return f":{response}\r\n"
    elif isinstance(response, list):
        return generate_redis_array(lst=response)

def generate_redis_array(lst: List[str] = None) -> str:
    redis_array = []
    if len(lst) == 0:
        return "*0\r\n"
    redis_array.append(f"*{len(lst)}\r\n")
    for element in lst:
        print("ELEMENT: ", element)
        if isinstance(element, list):
            redis_array.append(generate_redis_array(element))
            continue
        if element.endswith("\r\n"):
            element = element[:-2]

        if element.startswith(":") and element[1:].isdigit():
            redis_array.append(f"{element}\r\n")
            continue
        if element.startswith("+"):
            element = element[1:]
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