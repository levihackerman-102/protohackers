import socket

def start_server(host='0.0.0.0', port=65432):
    # UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    
    print(f"[LISTENING] UDP Server listening on {host}:{port}")
    
    # The database storage.
    # We pre-populate the 'version' key as required.
    # We store everything as bytes to handle arbitrary data safely.
    db = {
        b'version': b"Ken's Key-Value Store 1.0"
    }

    try:
        while True:
            data, addr = sock.recvfrom(1024)
            
            # The prompt defines 'Insert' by the presence of an equals sign.
            if b'=' in data:
                # --- INSERT ---
                # Split only on the *first* equals sign.
                # key=value=foo -> key="key", value="value=foo"
                key, value = data.split(b'=', 1)
                
                # Special Check: Ignore attempts to modify 'version'
                if key == b'version':
                    continue
                
                # Update/Insert the value
                db[key] = value
                # Insert requests get NO response.
                
            else:
                # --- RETRIEVE ---
                key = data
                
                # We only respond if the key exists.
                if key in db:
                    value = db[key]
                    # Format: key=value
                    response = key + b'=' + value
                    
                    # Send response back to the specific IP/Port that sent the request
                    sock.sendto(response, addr)
                
                # If key doesn't exist, we do nothing (per spec option: "return no response at all")

    except KeyboardInterrupt:
        print("\n[SHUTTING DOWN] Server stopping...")
    finally:
        sock.close()

if __name__ == "__main__":
    start_server()
