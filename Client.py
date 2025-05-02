import socket

def tcp_client():
    server_ip = input("Enter the server IP Address: ")
    server_port = int(input("Enter the server Port number: "))

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        clientSocket.connect((server_ip, server_port))
    except socket.error as e:
        print(f"Connection error: {e}")
        return

    while True:
        print("\nSelect a query by typing the corresponding number:")
        print("1. What is the average moisture inside my kitchen fridge in the past three hours?")
        print("2. What is the average water consumption per cycle in my smart dishwasher?")
        print("3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?")
        print("Press q to quit.")

        user_input = input("Enter your choice (1, 2, 3, or q): ")
        if user_input in ['1', '2', '3']:
            clientSocket.send(user_input.encode())
            response = clientSocket.recv(1024).decode()
            print(f"Server response: {response}")
        elif user_input == 'q' or 'Q':
            print("Exiting")
            break
        else:
                print("Invalid Response. Please select a proper query.")
                continue

        #clientSocket.send(message.encode())
        
    

    clientSocket.close()
    print("Disconnected from server.")

if __name__ == "__main__":
    tcp_client()