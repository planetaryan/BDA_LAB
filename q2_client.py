import socket
import pickle

def send_file_path_to_server(file_path):
    host = '172.16.57.29'  # Replace with your server's IP address
    port = 8080  # Chosen port number

    # Create a TCP/IP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to the server
        client_socket.connect((host, port))
        
        # Serialize and send the file path to the server
        file_path_data = pickle.dumps(file_path)
        client_socket.sendall(file_path_data)

        # Receive the sorted dictionary from the server
        received_data = b''
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            received_data += chunk
        
        if received_data:
            sorted_dict = pickle.loads(received_data)
            return sorted_dict
        else:
            return None

    finally:
        client_socket.close()

if __name__ == "__main__":
    file_path = input("Please enter the path to the text file: ")
    sorted_dict = send_file_path_to_server(file_path)
    
    # Print the sorted dictionary received from the server
    if sorted_dict:
        for key, value in sorted_dict.items():
            print(f'{key}: {value}')
