import socket
import pickle

def count_words_in_file(file_path):
    try:
        with open(file_path, 'r') as file:
            content_list = file.read().split()
            d = {}
            for word in content_list:
                d[word] = d.get(word, 0) + 1

        return d
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}

def handle_client_connection(client_socket):
    try:
        print("Handling client connection...")
        
        # Receive the file path from the client
        file_path_data = b''
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            file_path_data += chunk
        
        if file_path_data:
            file_path = pickle.loads(file_path_data)
            print(f"Received file path from client: {file_path}")
            
            # Count words in the file
            word_count_dict = count_words_in_file(file_path)
            
            # Sort the dictionary by values
            sorted_dict = dict(sorted(word_count_dict.items(), key=lambda item: item[1], reverse=True))
            
            # Serialize and send the sorted dictionary back to the client
            output_data = pickle.dumps(sorted_dict)
            client_socket.sendall(output_data)
    finally:
        client_socket.close()
        print("Client connection closed.")

def start_server():
    host = '172.16.57.29'  # Server listens on the Ethernet interface
    port = 8080  # Chosen port number

    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)  # Listen for incoming connections

    print(f'Server listening on {host}:{port}...')

    while True:
        client_socket, addr = server_socket.accept()
        print(f'Accepted connection from {addr}')
        handle_client_connection(client_socket)

if __name__ == "__main__":
    start_server()
