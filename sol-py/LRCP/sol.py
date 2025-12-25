import socket
import time
import select
import sys
from typing import Dict, List, Optional, Tuple

HOST = '0.0.0.0'
PORT = 65432

# Protocol: "LRCP messages must be smaller than 1000 bytes" -> Max 999.
MAX_PACKET_SIZE = 999 

RETRANSMIT_TIMEOUT = 3.0
SESSION_EXPIRY_TIMEOUT = 60.0
MAX_INT = 2147483648

# Send Window: How many bytes ahead of the ACK we are willing to blast out.
# 4000 bytes is roughly 4-5 full packets.
SEND_WINDOW_SIZE = 4000 

def escape_char(char: int) -> str:
    # 92 is backslash, 47 is forward slash
    if char == 92: return '\\\\'
    if char == 47: return '\\/'
    return chr(char)

def unescape_data(data: str) -> str:
    unescaped = []
    i = 0
    length = len(data)
    while i < length:
        char = data[i]
        if char == '\\':
            if i + 1 < length:
                next_char = data[i + 1]
                if next_char == '\\':
                    unescaped.append('\\')
                    i += 2
                    continue
                elif next_char == '/':
                    unescaped.append('/')
                    i += 2
                    continue
            unescaped.append('\\')
            i += 1
        else:
            unescaped.append(char)
            i += 1
    return "".join(unescaped)

def split_lrcp_message(msg: str) -> Optional[List[str]]:
    if not msg.startswith('/') or not msg.endswith('/'):
        return None
    
    parts = []
    current_part = []
    i = 1 
    length = len(msg) - 1
    
    while i < length:
        char = msg[i]
        if char == '\\':
            current_part.append(char)
            if i + 1 < length:
                current_part.append(msg[i+1])
                i += 2
            else:
                i += 1
        elif char == '/':
            parts.append("".join(current_part))
            current_part = []
            i += 1
        else:
            current_part.append(char)
            i += 1
            
    parts.append("".join(current_part))
    return parts

class LRCPSession:
    def __init__(self, session_id: int, addr: Tuple[str, int], sock: socket.socket):
        self.session_id = session_id
        self.addr = addr
        self.sock = sock
        
        self.is_closed = False
        self.last_activity_time = time.time()
        self.last_retransmit_time = time.time()
        
        self.bytes_received = 0 
        self.bytes_sent_acked = 0
        
        self.tx_buffer: bytearray = bytearray()
        self.rx_buffer: bytearray = bytearray()

    def touch(self):
        self.last_activity_time = time.time()

    def send_packet(self, parts: List[str]):
        payload = "/" + "/".join(parts) + "/"
        self.sock.sendto(payload.encode('ascii'), self.addr)

    def send_ack(self):
        self.send_packet(["ack", str(self.session_id), str(self.bytes_received)])

    def close(self):
        if not self.is_closed:
            self.send_packet(["close", str(self.session_id)])
            self.is_closed = True

    def handle_data_packet(self, pos: int, payload_str: str):
        try:
            payload = unescape_data(payload_str)
        except Exception:
            return 

        payload_bytes = payload.encode('latin-1')
        length = len(payload_bytes)

        if pos == self.bytes_received:
            self.bytes_received += length
            self.rx_buffer.extend(payload_bytes)
            self.process_application_data()
            self.send_ack()
        else:
            # Duplicate or Gap -> Ack what we have
            self.send_ack()

    def handle_ack_packet(self, length: int):
        total_sent_buffered = self.bytes_sent_acked + len(self.tx_buffer)
        
        if length > total_sent_buffered:
            self.close()
            return
        
        if length > self.bytes_sent_acked:
            acked_amount = length - self.bytes_sent_acked
            del self.tx_buffer[:acked_amount]
            self.bytes_sent_acked = length
            
            self.last_retransmit_time = time.time()
            if self.tx_buffer:
                self.retransmit_data()
        
        elif length == self.bytes_sent_acked:
            # Fast Retransmit on duplicate ack
            if self.tx_buffer:
                if time.time() - self.last_retransmit_time > 0.2:
                    self.retransmit_data()

    def process_application_data(self):
        while b'\n' in self.rx_buffer:
            line_bytes, remaining = self.rx_buffer.split(b'\n', 1)
            self.rx_buffer = remaining
            
            try:
                line_str = line_bytes.decode('ascii')
                reversed_line = line_str[::-1]
                response = reversed_line + "\n"
                self.tx_buffer.extend(response.encode('ascii'))
            except UnicodeDecodeError:
                pass
        
        if self.tx_buffer:
            self.retransmit_data()

    def retransmit_data(self):
        """
        Sends pending data using Pipelining (Windowing).
        Sends up to SEND_WINDOW_SIZE bytes from the current buffer.
        """
        if not self.tx_buffer:
            return

        buffer_offset = 0
        total_buffered = len(self.tx_buffer)
        
        # PIPELINING LOOP
        # We loop to send multiple packets back-to-back
        while buffer_offset < total_buffered and buffer_offset < SEND_WINDOW_SIZE:
            
            # The current position in the absolute stream
            current_abs_pos = self.bytes_sent_acked + buffer_offset
            
            # Calculate header overhead for THIS packet
            header_str = f"/data/{self.session_id}/{current_abs_pos}/"
            header_len = len(header_str)
            available_space = MAX_PACKET_SIZE - header_len - 1 # -1 for trailing slash
            
            if available_space <= 0: break

            chunk_parts = []
            chunk_len_chars = 0
            
            # Greedy packing for this specific packet
            # We iterate through the tx_buffer starting at buffer_offset
            temp_offset = buffer_offset
            
            while temp_offset < total_buffered:
                byte_val = self.tx_buffer[temp_offset]
                # Cost calculation
                cost = 2 if (byte_val == 92 or byte_val == 47) else 1
                
                if chunk_len_chars + cost > available_space:
                    break
                
                chunk_parts.append(escape_char(byte_val))
                chunk_len_chars += cost
                temp_offset += 1
            
            if temp_offset == buffer_offset:
                # Couldn't fit even a single byte? Should not happen.
                break
                
            escaped_payload = "".join(chunk_parts)
            self.send_packet(["data", str(self.session_id), str(current_abs_pos), escaped_payload])
            
            # Advance buffer_offset by the number of RAW bytes we just packed
            bytes_packed = temp_offset - buffer_offset
            buffer_offset += bytes_packed

        self.last_retransmit_time = time.time()

    def tick(self):
        now = time.time()
        
        if now - self.last_activity_time > SESSION_EXPIRY_TIMEOUT:
            self.is_closed = True
            return

        if self.tx_buffer and (now - self.last_retransmit_time > RETRANSMIT_TIMEOUT):
            self.retransmit_data()

class LRCPServer:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((HOST, PORT))
        self.sessions: Dict[int, LRCPSession] = {}
        print(f"LRCP Server listening on {HOST}:{PORT}")

    def run(self):
        while True:
            ready = select.select([self.sock], [], [], 0.1)
            if ready[0]:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    self.handle_packet(data, addr)
                except Exception as e:
                    print(f"Receive error: {e}")

            self.tick_sessions()

    def handle_packet(self, raw_data: bytes, addr: Tuple[str, int]):
        if len(raw_data) > MAX_PACKET_SIZE:
            return
        
        try:
            msg_str = raw_data.decode('ascii')
        except UnicodeDecodeError:
            return 

        parts = split_lrcp_message(msg_str)
        if parts is None: return
        
        if len(parts) < 1: return
        cmd = parts[0]
        
        if cmd == 'connect':
            if len(parts) != 2: return
        elif cmd == 'close':
            if len(parts) != 2: return
        elif cmd == 'ack':
            if len(parts) != 3: return
        elif cmd == 'data':
            if len(parts) != 4: return
        else:
            return 

        try:
            session_id = int(parts[1])
            if session_id >= MAX_INT: return
        except ValueError:
            return

        if cmd == 'connect':
            self.handle_connect(session_id, addr)
            return

        session = self.sessions.get(session_id)
        if not session:
            if cmd != 'close':
                temp_sess = LRCPSession(session_id, addr, self.sock)
                temp_sess.close()
            return
        
        if session.addr != addr: return 

        session.touch()
        
        if cmd == 'data':
            try:
                pos = int(parts[2])
                payload = parts[3]
                if pos >= MAX_INT: return
                session.handle_data_packet(pos, payload)
            except ValueError:
                return

        elif cmd == 'ack':
            try:
                length = int(parts[2])
                if length >= MAX_INT: return
                session.handle_ack_packet(length)
            except ValueError:
                return
        
        elif cmd == 'close':
            session.close()
            self.sessions.pop(session_id, None)

    def handle_connect(self, session_id: int, addr: Tuple[str, int]):
        if session_id not in self.sessions:
            self.sessions[session_id] = LRCPSession(session_id, addr, self.sock)
        self.sessions[session_id].send_ack()

    def tick_sessions(self):
        to_remove = []
        for sid, session in self.sessions.items():
            session.tick()
            if session.is_closed:
                to_remove.append(sid)
        for sid in to_remove:
            self.sessions.pop(sid, None)

if __name__ == "__main__":
    server = LRCPServer()
    try:
        server.run()
    except KeyboardInterrupt:
        pass
