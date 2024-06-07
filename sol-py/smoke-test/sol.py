import socket
import threading

def handle_client(conn, addr):
    """
    Handles a single client connection.
    Args:
        conn: The socket object for the connected client.
        addr: The IP address and port of the client.
    """
    print(f"[NEW CONNECTION] {addr} connected.")
    
    try:
        while True:
            data = conn.recv(4096)
            
            # If recv returns an empty bytes object (b''), the client has 
            # shut down their sending side.
            if not data:
                break
            
            # sendall ensures the entire buffer is sent, handling network 
            # buffers automatically.
            conn.sendall(data)
            
    except ConnectionResetError:
        print(f"[ERROR] Connection forcibly closed by {addr}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred with {addr}: {e}")
    finally:
        # This sends the final FIN packet to the client, acknowledging 
        # we are done processing.
        conn.close()
        print(f"[DISCONNECTED] {addr} disconnected.")

def start_server(host='0.0.0.0', port=65432):
    """
    Starts the main server loop.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((host, port))
        server.listen(5)
        print(f"[LISTENING] Server is listening on {host}:{port}")
        
        while True:
            # Block and wait for a new connection
            conn, addr = server.accept()
            
            # Spin up a new thread for the new client so the main loop
            # can go back to waiting for the next connection immediately.
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            # Daemon threads exit if the main program exits
            thread.daemon = True 
            thread.start()
            
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
            
    except KeyboardInterrupt:
        print("\n[SHUTTING DOWN] Server stopping...")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()
