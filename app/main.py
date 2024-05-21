import asyncio

async def handle_client(reader, writer):
    # Read data from the client
    data = await reader.read(1024)
    print(f"Received data: {data}")

    # Redis "PING" command encoding
    redis_ping = b"*1\r\n$4\r\nPING\r\n"
    # Redis "PONG" response encoding
    redis_pong = b"+PONG\r\n"

    # Check if the data matches "PING" command
    if data == redis_ping:
        print("Received PING, sending PONG")
        # Respond with "PONG"
        writer.write(redis_pong)
        await writer.drain()
    else:
        print("Received unexpected data")

    # Close the connection
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server is listening on port 6379...")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())