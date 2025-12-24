import socket
import threading
import re

# config
UPSTREAM_HOST = 'chat.protohackers.com'
UPSTREAM_PORT = 16963
LISTEN_PORT = 65432
TONY_ADDRESS = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

def rewrite_line(line_bytes):
    """
    Decodes the line, replaces Boguscoin addresses, and re-encodes.
    """
    try:
        # Decode ignoring errors to prevent crashes on binary junk
        text = line_bytes.decode('utf-8', errors='ignore')
    except Exception:
        return line_bytes

    # Split by space to isolate potential addresses.
    # We use a purely regex approach on individual words to be safe.
    parts = text.split(' ')
    new_parts = []
    
    for part in parts:
        # Check if this specific part is a Boguscoin address
        # Logic: Starts with 7, alphanumeric, length 26-35.
        # We also remove any trailing newline/whitespace just for the check, 
        # though split(' ') usually leaves pure words. 
        # Note: We must be careful not to strip punctuation if it's attached,
        # but the prompt says "followed by a space" implies space is the delimiter.
        
        # However, newlines might be attached to the last word.
        clean_part = part.rstrip() 
        
        if len(clean_part) >= 26 and len(clean_part) <= 35 and \
           clean_part.startswith('7') and clean_part.isalnum():
            
            # Use replace to preserve any attached whitespace/newlines that rstrip removed
            # (Though usually split(' ') consumes the space, rstrip handles \n or \r at end of line)
            new_parts.append(part.replace(clean_part, TONY_ADDRESS))
        else:
            new_parts.append(part)
            
    # Reconstruct the line
    new_text = ' '.join(new_parts)
    return new_text.encode('utf-8')

def forward(source, destination, direction_name):
    buffer = b""
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break 

            buffer += data
            
            while b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                
                # Rewrite and add the newline back
                modified_line = rewrite_line(line) + b'\n'
                
                destination.sendall(modified_line)
                
    except Exception as e:
        # Errors here are normal during disconnects
        pass
    finally:
        # Ensure connection is closed cleanly
        try:
            source.shutdown(socket.SHUT_RDWR)
        except: pass
        source.close()
        try:
            destination.shutdown(socket.SHUT_RDWR)
        except: pass
        destination.close()

def handle_client(client_socket, client_addr):
    print(f"[NEW VICTIM] {client_addr} connected.")
    
    try:
        upstream_socket = socket.create_connection((UPSTREAM_HOST, UPSTREAM_PORT))
    except Exception as e:
        print(f"[ERROR] connecting to upstream: {e}")
        client_socket.close()
        return

    t_upstream = threading.Thread(target=forward, args=(client_socket, upstream_socket, "Upstream"))
    t_downstream = threading.Thread(target=forward, args=(upstream_socket, client_socket, "Downstream"))
    
    # Daemonize so they die if main thread dies
    t_upstream.daemon = True
    t_downstream.daemon = True
    
    t_upstream.start()
    t_downstream.start()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind(('0.0.0.0', LISTEN_PORT))
        server.listen(20)
        print(f"[LISTENING] MITM Proxy on {LISTEN_PORT} -> {UPSTREAM_HOST}:{UPSTREAM_PORT}")
        
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=handle_client, args=(client_socket, addr), daemon=True).start()
            
    except KeyboardInterrupt:
        pass
    finally:
        server.close()

if __name__ == "__main__":
    start_server()
