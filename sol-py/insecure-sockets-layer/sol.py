import socket
import threading
import select

OP_END = 0x00
OP_REVERSEBITS = 0x01
OP_XOR_N = 0x02
OP_XOR_POS = 0x03
OP_ADD_N = 0x04
OP_ADD_POS = 0x05

# Precompute bit reversal for all byte values (0-255) for speed
REVERSE_TABLE = [0] * 256
for i in range(256):
    b = 0
    val = i
    for _ in range(8):
        b = (b << 1) | (val & 1)
        val >>= 1
    REVERSE_TABLE[i] = b

class CipherContext:
    def __init__(self, spec):
        self.spec = spec
        # Separate position counters for enc and dec
        self.enc_pos = 0
        self.dec_pos = 0

    def decode_byte(self, byte_val):
        """
        Applies the INVERSE of the cipher spec to decode a byte.
        Operations must be applied in REVERSE order.
        Inverse of XOR(N) is XOR(N).
        Inverse of ADD(N) is SUB(N) (or ADD(256-N)).
        Inverse of REVERSEBITS is REVERSEBITS.
        """
        current = byte_val
        
        # Iterate backwards through operations
        for op in reversed(self.spec):
            code = op['code']
            arg = op.get('arg', 0)
            
            if code == OP_REVERSEBITS:
                current = REVERSE_TABLE[current]
                
            elif code == OP_XOR_N:
                current ^= arg
                
            elif code == OP_XOR_POS:
                current ^= (self.dec_pos % 256)
                
            elif code == OP_ADD_N:
                # Inverse of Add N is Subtract N
                current = (current - arg) % 256
                
            elif code == OP_ADD_POS:
                # Inverse of Add Pos is Subtract Pos
                current = (current - (self.dec_pos % 256)) % 256
                
        self.dec_pos += 1
        return current

    def encode_byte(self, byte_val):
        """
        Applies the cipher spec to encode a byte.
        Operations are applied in FORWARD order.
        """
        current = byte_val
        
        for op in self.spec:
            code = op['code']
            arg = op.get('arg', 0)
            
            if code == OP_REVERSEBITS:
                current = REVERSE_TABLE[current]
                
            elif code == OP_XOR_N:
                current ^= arg
                
            elif code == OP_XOR_POS:
                current ^= (self.enc_pos % 256)
                
            elif code == OP_ADD_N:
                current = (current + arg) % 256
                
            elif code == OP_ADD_POS:
                current = (current + (self.enc_pos % 256)) % 256
                
        self.enc_pos += 1
        return current

def parse_cipher_spec(sock):
    """
    Reads from socket byte-by-byte to parse the variable length cipher spec.
    Stops reading at 0x00.
    Returns: List of ops [{'code': int, 'arg': int}, ...]
    """
    ops = []
    
    # We buffer input because we might read part of the stream payload by accident 
    # if we tried to read chunks. We must read EXACTLY the spec.
    while True:
        b_data = sock.recv(1)
        if not b_data: raise ConnectionError("Connection closed during handshake")
        
        byte = b_data[0]
        
        if byte == OP_END:
            break
            
        elif byte == OP_REVERSEBITS:
            ops.append({'code': OP_REVERSEBITS})
            
        elif byte == OP_XOR_N:
            arg_data = sock.recv(1)
            if not arg_data: raise ConnectionError("Connection closed during handshake")
            ops.append({'code': OP_XOR_N, 'arg': arg_data[0]})
            
        elif byte == OP_XOR_POS:
            ops.append({'code': OP_XOR_POS})
            
        elif byte == OP_ADD_N:
            arg_data = sock.recv(1)
            if not arg_data: raise ConnectionError("Connection closed during handshake")
            ops.append({'code': OP_ADD_N, 'arg': arg_data[0]})
            
        elif byte == OP_ADD_POS:
            ops.append({'code': OP_ADD_POS})
            
        else:
            # "Clients won't try to use illegal cipher specs"
            # But good to return invalid anyway
            return None
            
    return ops

def is_noop(spec):
    """
    Checks if the cipher spec is a no-op by testing it against a dummy payload.
    Requirement: "If a client tries to use a cipher that leaves EVERY byte of input unchanged"
    """
    # We test bytes 0..255 at stream position 0.
    # If the cipher depends on position, testing pos 0 might be a false positive for "identity",
    # so we should test a few positions.
    
    # Test vector: check if encode(x) == x for all x
    # However, position dependence matters. 
    # Spec says: "leaves every byte of input unchanged". This implies the transformation is Identity.
    
    # Check 1: Empty spec
    if not spec: return True

    ctx = CipherContext(spec)
    
    # We check a range of bytes at different positions.
    # If ANY byte changes, it's not a no-op.
    for i in range(10): # Check first 10 stream positions
        original = i % 256
        encoded = ctx.encode_byte(original)
        if encoded != original:
            return False
            
    return True

def find_max_toy(line):
    """
    Parses "10x toy car,15x dog" and returns "15x dog"
    """
    try:
        parts = line.split(',')
        best_toy = ""
        max_count = -1
        
        for part in parts:
            if 'x' not in part: continue
            count_str, name = part.split('x', 1)
            count = int(count_str)
            
            if count > max_count:
                max_count = count
                best_toy = part
                
        return best_toy
    except:
        return ""

def handle_client(conn, addr):
    try:
        # 1. Handshake: Parse Cipher Spec
        spec = parse_cipher_spec(conn)
        if spec is None:
            return # Invalid
            
        # 2. Check No-op
        if is_noop(spec):
            # "Immediately disconnect without sending any data back"
            return 
        
        ctx = CipherContext(spec)
        
        # 3. Application Loop
        recv_buffer = bytearray()
        
        while True:
            # We read raw bytes
            chunk = conn.recv(4096)
            if not chunk:
                break
                
            # Decode chunk byte by byte
            for raw_byte in chunk:
                decoded_byte = ctx.decode_byte(raw_byte)
                recv_buffer.append(decoded_byte)
                
                # Check for newline (ASCII 10)
                if decoded_byte == 10:
                    # Process Line
                    # Decode as ASCII
                    line = recv_buffer[:-1].decode('ascii', errors='ignore')
                    
                    # Logic
                    response_str = find_max_toy(line) + "\n"
                    
                    # Encode Response
                    resp_bytes = response_str.encode('ascii')
                    encoded_resp = bytearray()
                    for b in resp_bytes:
                        encoded_resp.append(ctx.encode_byte(b))
                        
                    conn.sendall(encoded_resp)
                    
                    # Clear buffer
                    recv_buffer = bytearray()
                    
    except Exception:
        pass
    finally:
        conn.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 65432))
    server.listen(20)
    print("Insecure Socket Layer Server listening...")
    
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()
