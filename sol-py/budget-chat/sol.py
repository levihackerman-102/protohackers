import socket
import threading

# Global state
# We use a dictionary to map sockets to names: {conn: "name"}
# Only users who have successfully 'joined' (set a valid name) are in this dict.
clients = {}
clients_lock = threading.Lock()

def broadcast(message, sender_conn=None):
    """
    Sends a message to all joined clients.
    If sender_conn is provided, that specific client is excluded (used for chat messages).
    If sender_conn is None, everyone gets it (used for system announcements).
    """
    # We must lock while iterating to ensure the dictionary doesn't change size
    # (e.g., someone leaving) while we are looping through it.
    with clients_lock:
        for conn in clients:
            if conn != sender_conn:
                try:
                    conn.sendall(message.encode('utf-8'))
                except:
                    # If sending fails, we assume the client is dead.
                    # We don't remove them here; the reading thread will handle cleanup.
                    pass

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    
    # User is not 'joined' yet, so name is None
    name = None
    buffer = b""
    
    try:
        # 1. Prompt for name
        conn.sendall(b"Welcome to budgetchat! What shall I call you?\n")
        
        # 2. Wait for Name (Handshake)
        # We need a mini-loop here to ensure we get a full line for the name
        while True:
            data = conn.recv(1024)
            if not data:
                return # Client disconnected before naming
            buffer += data
            if b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                # Decode and strip whitespace (newlines/carriage returns)
                raw_name = line.decode('utf-8').strip()
                
                # Validation: Alphanumeric and at least 1 char
                if len(raw_name) >= 1 and raw_name.isalnum():
                    name = raw_name
                    break
                else:
                    # Invalid name: disconnect silently or with error
                    # Prompt says: "server may send an informative error... and must disconnect"
                    conn.sendall(b"Invalid name. Alphanumeric only.\n")
                    return # Disconnect logic runs in 'finally'

        # 3. Join the Room
        with clients_lock:
            # Generate user list string BEFORE adding the new user
            current_users = ", ".join(clients.values())
            
            # Announce to others
            # "The server must send all OTHER users a message... that the user has joined"
            msg = f"* {name} has entered the room\n"
            for other_conn in clients:
                other_conn.sendall(msg.encode('utf-8'))
            
            # Add self to registry
            clients[conn] = name

        # 4. Send the user list to the NEW user
        # "must start with an asterisk"
        conn.sendall(f"* The room contains: {current_users}\n".encode('utf-8'))

        # 5. Main Chat Loop
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            buffer += data
            
            while b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                message = line.decode('utf-8').strip()
                
                # Broadcast chat message: "[name] message"
                # Exclude self from broadcast
                formatted_msg = f"[{name}] {message}\n"
                broadcast(formatted_msg, sender_conn=conn)

    except Exception as e:
        print(f"[ERROR] {addr}: {e}")
    finally:
        # Cleanup
        if name:
            with clients_lock:
                if conn in clients:
                    del clients[conn]
            
            # Broadcast leave message to others
            # "The server must send this message... but NOT to connected clients that have not yet joined"
            # Our broadcast function uses the 'clients' dict, so unjoined clients are naturally excluded.
            broadcast(f"* {name} has left the room\n")
            
        conn.close()
        print(f"[DISCONNECTED] {addr} disconnected.")

def start_server(host='0.0.0.0', port=65432):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((host, port))
        server.listen(10) # Support at least 10 clients
        print(f"[LISTENING] Server is listening on {host}:{port}")
        
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.daemon = True
            thread.start()
            
    except KeyboardInterrupt:
        print("\n[SHUTTING DOWN] Server stopping...")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()
