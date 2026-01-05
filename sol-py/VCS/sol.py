import socket
import threading
import string

HOST = '0.0.0.0'
PORT = 30307

file_store = {}
store_lock = threading.Lock()

# Validation Constants
ALLOWED_FNAME_CHARS = set(string.ascii_letters + string.digits + "._/-")
TEXT_CHARS = set(range(32, 127)) | {ord('\n'), ord('\r'), ord('\t')}

def is_valid_path_structure(path):
    """
    Common structural checks for both files and directories.
    """
    # 1. Char Whitelist
    if not all(c in ALLOWED_FNAME_CHARS for c in path):
        return False
    # 2. Absolute Path
    if not path.startswith('/'):
        return False
    # 3. No empty segments (//)
    if '//' in path:
        return False
    # 4. No traversal
    parts = path.split('/')
    for p in parts:
        if p == '.' or p == '..':
            return False
    return True

def is_valid_filename(fname):
    """
    Strict validation for PUT/GET (Files).
    Must be a valid path AND cannot end with / (files aren't directories).
    """
    if not is_valid_path_structure(fname):
        return False
    # Files cannot be root or end in slash
    if fname == '/' or fname.endswith('/'):
        return False
    return True

def is_valid_dirname(dname):
    """
    Validation for LIST (Directories).
    Must be a valid path. Root '/' is allowed. Trailing slash is allowed.
    """
    return is_valid_path_structure(dname)

def is_valid_text_content(data_bytes):
    for b in data_bytes:
        if b not in TEXT_CHARS:
            return False
    return True

def send_line(conn, text):
    conn.sendall((text + "\n").encode('ascii'))

def get_listing_items(target_dir):
    prefix = target_dir
    if not prefix.endswith('/'):
        prefix += '/'
    
    entries = set()
    
    with store_lock:
        for fname in file_store.keys():
            if fname.startswith(prefix):
                relative = fname[len(prefix):]
                parts = relative.split('/')
                
                if len(parts) == 1:
                    rev_count = len(file_store[fname])
                    entries.add(f"{parts[0]} r{rev_count}")
                else:
                    entries.add(f"{parts[0]}/ DIR")

    return sorted(list(entries))

def handle_client(conn, addr):
    send_line(conn, "READY")
    
    buffer = b""
    try:
        while True:
            # --- Line Reading Loop ---
            while b'\n' not in buffer:
                chunk = conn.recv(4096)
                if not chunk: return
                buffer += chunk
            
            line_bytes, buffer = buffer.split(b'\n', 1)
            line = line_bytes.decode('ascii', errors='ignore').strip()
            
            if not line:
                send_line(conn, "READY")
                continue

            parts = line.split()
            cmd = parts[0].upper()

            # --- PUT COMMAND ---
            if cmd == 'PUT':
                if len(parts) != 3:
                    send_line(conn, "ERR usage: PUT file length newline data")
                else:
                    filename = parts[1]
                    try:
                        length = int(parts[2])
                    except ValueError:
                        send_line(conn, "ERR length must be integer")
                        send_line(conn, "READY")
                        continue

                    file_data = b""
                    take = min(len(buffer), length)
                    file_data += buffer[:take]
                    buffer = buffer[take:]
                    
                    remaining = length - len(file_data)
                    while remaining > 0:
                        chunk = conn.recv(min(4096, remaining))
                        if not chunk: return
                        file_data += chunk
                        remaining -= len(chunk)

                    if not is_valid_filename(filename):
                        send_line(conn, "ERR illegal filename")
                    elif not is_valid_text_content(file_data):
                        send_line(conn, "ERR text files only")
                    else:
                        with store_lock:
                            if filename not in file_store:
                                file_store[filename] = []
                                file_store[filename].append(file_data)
                                new_rev = 1
                            else:
                                last_rev_data = file_store[filename][-1]
                                if last_rev_data != file_data:
                                    file_store[filename].append(file_data)
                                new_rev = len(file_store[filename])
                        
                        send_line(conn, f"OK r{new_rev}")

            # --- GET COMMAND ---
            elif cmd == 'GET':
                if len(parts) < 2:
                    send_line(conn, "ERR usage: GET file [revision]")
                else:
                    filename = parts[1]
                    
                    if not is_valid_filename(filename):
                        send_line(conn, "ERR illegal filename")
                        send_line(conn, "READY")
                        continue

                    target_rev = None
                    if len(parts) >= 3:
                        rev_str = parts[2]
                        if rev_str.lower().startswith('r'):
                            rev_str = rev_str[1:]
                        try:
                            target_rev = int(rev_str)
                        except ValueError:
                            send_line(conn, "ERR invalid revision")
                            send_line(conn, "READY")
                            continue
                    
                    data = None
                    with store_lock:
                        if filename in file_store:
                            revs = file_store[filename]
                            if target_rev is None:
                                data = revs[-1]
                            elif 1 <= target_rev <= len(revs):
                                data = revs[target_rev - 1]

                    if data is not None:
                        send_line(conn, f"OK {len(data)}")
                        conn.sendall(data)
                    else:
                        send_line(conn, "ERR no such file or revision")

            # --- LIST COMMAND ---
            elif cmd == 'LIST':
                if len(parts) < 2:
                    send_line(conn, "ERR usage: LIST dir")
                else:
                    target_dir = parts[1]
                    
                    # 
                    # Validate directory name just like filename, but allow root
                    if not is_valid_dirname(target_dir):
                        send_line(conn, "ERR illegal dir name")
                    else:
                        entries = get_listing_items(target_dir)
                        send_line(conn, f"OK {len(entries)}")
                        for entry in entries:
                            send_line(conn, entry)

            # --- HELP COMMAND ---
            elif cmd == 'HELP':
                send_line(conn, "OK usage: HELP|GET|PUT|LIST")

            else:
                send_line(conn, "ERR unknown command")

            send_line(conn, "READY")

    except Exception:
        pass
    finally:
        conn.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(20)
    print(f"VCS Server listening on {PORT}...")
    
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()
