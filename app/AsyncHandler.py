import asyncio
import logging


from typing import TYPE_CHECKING

import app.commands.commands as commands
import app.utils.encoding_utils as encoding_utils
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
        self.command_queue = None
        self.command_map = {
            "PING": commands.PingCommand(),
            "ECHO": commands.EchoCommand(),
            "SET": commands.SetCommand(),
            "GET": commands.GetCommand(),
            "INFO": commands.InfoCommand(),
            "REPLCONF": commands.ReplConfCommand(),
            "PSYNC": commands.PSyncCommand(),
            "WAIT": commands.WaitCommand(),
            "CONFIG": commands.ConfigCommand(),
            "KEYS": commands.KeysCommand(),
            "TYPE": commands.TypeCommand(),
            "XADD": commands.XAddCommand(),
            "XRANGE": commands.XRangeCommand(),
            "XREAD": commands.XReadCommand(),
            "INCR": commands.IncrCommand(),
            "INCRBY": commands.IncrByCommand(),
            "EXEC": commands.ExecCommand(),
            "MULTI": commands.MultiCommand(),
        }

    async def process_request(self) -> None:
        while True:
            request = await self.reader.read(1024)
            if not request:
                break
            logging.info(f"Request: {request}")
            await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        command_list, lengths = encoding_utils.parse_redis_protocol(request)
        
        if not command_list:
            logging.info("Received invalid data")
            return
        
        if self.command_queue is not None:
            await self.command_queue.put((command_list, lengths))
            response = encoding_utils.generate_redis_array("QUEUED")
            self.writer.write(response.encode())
            await self.writer.drain()
            return

        for index, cmd in enumerate(command_list):
            cmd_name = cmd[0].upper()  # Command names are case-insensitive
            command_class = self.command_map.get(cmd_name, commands.UnknownCommand())

            if cmd_name in self.command_map:
                response = await command_class.execute(self, cmd)
            else:
                response = await commands.UnknownCommand.execute(self, cmd)

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