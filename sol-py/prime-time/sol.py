import socket
import threading
import json
import math

def is_prime(n):
    """
    Checks if a number is prime.
    According to the spec: non-integers cannot be prime.
    """
    # In Python, json.loads parses 5.0 as a float and 5 as an int.
    # The prompt implies strictly non-integers (floats) are not prime.
    if type(n) is not int:
        return False
        
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
        
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def validate_and_process(request_str):
    """
    Parses the JSON and validates requirements.
    Returns: (response_dict, error_bool)
    """
    try:
        req = json.loads(request_str)
    except json.JSONDecodeError:
        return None, True # Malformed JSON

    # Check 1: Must be a valid JSON object (dict)
    if not isinstance(req, dict):
        return None, True

    # Check 2: "method" must exist and equal "isPrime"
    if req.get("method") != "isPrime":
        return None, True

    # Check 3: "number" must exist and be a number (int or float)
    number = req.get("number")
    if number is None or type(number) not in [int, float]:
        return None, True

    # Logic: Determine if prime
    is_p = is_prime(number)

    return {"method": "isPrime", "prime": is_p}, False

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    
    # We use a byte-buffer because recv might return partial lines
    buffer = b""
    
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            
            buffer += data
            
            # Process buffer line by line
            while b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                
                try:
                    # JSON requires UTF-8
                    decoded_line = line.decode('utf-8')
                except UnicodeDecodeError:
                    # If we can't decode text, it's malformed
                    conn.sendall(b"malformed\n")
                    return # Disconnect

                response_data, is_error = validate_and_process(decoded_line)

                if is_error:
                    # Send malformed response and disconnect immediately
                    conn.sendall(b"malformed\n")
                    return # This breaks the function and closes socket in 'finally'

                # Send success response
                response_bytes = json.dumps(response_data).encode('utf-8') + b'\n'
                conn.sendall(response_bytes)
            
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
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
            
    except KeyboardInterrupt:
        print("\n[SHUTTING DOWN] Server stopping...")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()
