import socket

def main():
    # Create a server socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on port 6379...")

    while True:
        # Accept a client connection
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")

        try:
            # Read data from the client
            data = client_socket.recv(1024)
            print(f"Received data: {data}")

            # Redis "PING" command encoding
            redis_ping = b"*1\r\n$4\r\nPING\r\n"
            # Redis "PONG" response encoding
            redis_pong = b"+PONG\r\n"

            # Check if the data matches "PING" command
            if data == redis_ping:
                print("Received PING, sending PONG")
                # Respond with "PONG"
                client_socket.sendall(redis_pong)
            else:
                print("Received unexpected data")

        finally:
            # Close the client connection
            client_socket.close()

if __name__ == "__main__":
    main()
