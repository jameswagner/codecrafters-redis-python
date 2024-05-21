import asyncio

async def handle_client(reader, writer):
    global ping_count  # Declare ping_count as global

    # Read data from the client
    data = await reader.read(1024)
    print(f"Received data: {data}")

    # Redis "PING" command encoding
    redis_ping = b"*1\r\n$4\r\nPING\r\n"
    # Redis "PONG" response encoding
    redis_pong = b"+PONG\r\n"

    # Check if the data matches "PING" command
    if data == redis_ping:
        ping_count += 1  # Increment ping_count
        print(f"Received PING {ping_count}")  # Log the count
        print("Sending PONG")
        # Respond with "PONG"
        writer.write(redis_pong)
        await writer.drain()
    else:
        print("Received unexpected data")

async def main():
    global ping_count  # Declare ping_count as global
    ping_count = 0  # Initialize ping_count

    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server is listening on port 6379...")

    async with server:
        await server.serve_forever(0.001)
        
    print("Server stopped listening")

if __name__ == "__main__":
    asyncio.run(main())
