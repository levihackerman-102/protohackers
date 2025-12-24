import socket
import threading
import struct

'''
insertion and querying can be made more efficient by storing in a sorted order
by timestamp and using binary search to find the start and end indices for queries.
''' 
def handle_client(conn, addr):
    """
    Handles a single client session for the price tracker.
    Each session maintains its own isolated database of prices.
    """
    print(f"[NEW CONNECTION] {addr} connected.")
    
    # Each connection has its own isolated data store.
    # We use a simple list of tuples: [(timestamp, price), ...]
    prices_data = []

    # Buffer to hold incoming bytes until we have a full message (9 bytes)
    buffer = b""
    
    try:
        while True:
            # Receive data. 4096 is a standard buffer size.
            chunk = conn.recv(4096)
            if not chunk:
                break # Client disconnected
            
            buffer += chunk
            
            # Process strictly in 9-byte chunks
            while len(buffer) >= 9:
                # Slice off the first 9 bytes
                message = buffer[:9]
                buffer = buffer[9:]
                
                # Unpack the binary data
                # Format '>cii':
                #   > : Big Endian (Network Byte Order)
                #   c : char (1 byte) - The Type
                #   i : int (4 bytes) - Arg1 (Timestamp or Mintime)
                #   i : int (4 bytes) - Arg2 (Price or Maxtime)
                msg_type_byte, arg1, arg2 = struct.unpack('>cii', message)
                
                # msg_type_byte is bytes (e.g., b'I'), we decode to string
                msg_type = msg_type_byte.decode('ascii')

                if msg_type == 'I':
                    # Insert: arg1=timestamp, arg2=price
                    timestamp, price = arg1, arg2
                    prices_data.append((timestamp, price))
                    
                elif msg_type == 'Q':
                    # Query: arg1=mintime, arg2=maxtime
                    mintime, maxtime = arg1, arg2
                    
                    total_price = 0
                    count = 0
                    
                    # Logic: If mintime > maxtime, mean is 0.
                    if mintime <= maxtime:
                        for t, p in prices_data:
                            if mintime <= t <= maxtime:
                                total_price += p
                                count += 1
                    
                    if count == 0:
                        mean = 0
                    else:
                        # Integer division (//) floors the result, which satisfies 
                        # the "round either up or down" requirement.
                        mean = total_price // count
                    
                    # Send response: Single int32 in Big Endian
                    # Format '>i': Big Endian, int (4 bytes)
                    conn.sendall(struct.pack('>i', mean))
                
                else:
                    # Undefined behavior for invalid types.
                    # We choose to disconnect the client to be safe.
                    return 

    except Exception as e:
        print(f"[ERROR] with {addr}: {e}")
    finally:
        conn.close()
        print(f"[DISCONNECTED] {addr} disconnected.")

def start_server(host='0.0.0.0', port=65432):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((host, port))
        server.listen(5)
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
