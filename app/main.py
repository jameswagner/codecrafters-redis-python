import argparse
import asyncio
import logging
from .AsyncServer import AsyncServer

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
    await AsyncServer.create(port=args.port, replica_server=replica_server, replica_port=replica_port, dir=args.dir, dbfilename=args.dbfilename)

if __name__ == "__main__":
    asyncio.run(main())
