import time
from typing import Any, BinaryIO, Dict, Tuple



def read_encoded_value(file: BinaryIO, value_type: bytes) -> Any:
    if value_type == b"\x00":
        # String value
        value_length = int.from_bytes(file.read(1), byteorder="little")
        return file.read(value_length)        
    else:
        # Unknown value type
        return None

def handle_database_selector(file):
    db_number = int.from_bytes(file.read(1), byteorder="little")
    # Skip resizedb field
    file.read(2)

def handle_key_value_pair_with_expiry_seconds(file):
    expiry_time = int.from_bytes(file.read(4), byteorder="little")
    value_type = file.read(1)
    key_length = int.from_bytes(file.read(1), byteorder="little")
    key = file.read(key_length)
    value = read_encoded_value(file, value_type)
    if expiry_time > 0 and expiry_time < time.time():
        return None, None, 0
    return key, value, expiry_time

def handle_key_value_pair_with_expiry_milliseconds(file):
    expiry_time = int.from_bytes(file.read(8), byteorder="little")
    value_type = file.read(1)
    key_length = int.from_bytes(file.read(1), byteorder="little")
    key = file.read(key_length)
    value = read_encoded_value(file, value_type)
    if expiry_time > 0 and expiry_time < time.time() * 1000:
        return None, None, 0
    expiry_time = expiry_time / 1000
    return key, value, expiry_time

def handle_key_value_pair_without_expiry(file, value_type):
    key_length = int.from_bytes(file.read(1), byteorder="little")
    key = file.read(key_length)
    value = read_encoded_value(file, value_type)
    return key, value, 0

field_handlers = {
    b"\xFE": handle_database_selector,
    b"\xFD": handle_key_value_pair_with_expiry_seconds,
    b"\xFC": handle_key_value_pair_with_expiry_milliseconds,
}

def parse_redis_file(file_path: str) -> Tuple[Dict[str, str], Dict[str, float]]:
    hash_map = {}
    expiry_times = {}

    try:
        with open(file_path, "rb") as file:
            magic_string = file.read(5)
            rdb_version = file.read(4)
            while True:
                byte = file.read(1)
                if not byte:
                    break
                if byte == b"\xFE":
                    for _ in range(4):
                        print(file.read(1))
                    break

            while True:
                field_type = file.read(1)
                if field_type == b"\xFF":
                    break
                key, value, expiry_time = None, None, None

                if field_type in field_handlers:
                    key, value, expiry_time = field_handlers[field_type](file)
                else:
                    key, value, expiry_time = handle_key_value_pair_without_expiry(file, field_type)

                if key is not None and value is not None:
                    hash_map[key.decode()] = value.decode()
                if key is not None and expiry_time is not None:
                    expiry_times[key.decode()] = expiry_time

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error occurred while parsing the file: {str(e)}")

    return hash_map, expiry_times