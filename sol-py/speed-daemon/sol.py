import socket
import threading
import struct
import time
from collections import defaultdict
from typing import List, Dict, Set, Tuple, TypedDict, Optional, Any

RoadID = int
Mile = int
Timestamp = int
Speed100 = int  # Speed * 100
Plate = str
Deciseconds = int

Observation = Tuple[Timestamp, Mile, int]

class Ticket(TypedDict):
    plate: Plate
    road: RoadID
    mile1: Mile
    timestamp1: Timestamp
    mile2: Mile
    timestamp2: Timestamp
    speed: Speed100

HOST: str = '0.0.0.0'
PORT: int = 65432

# Message Types
MSG_ERROR: int           = 0x10
MSG_PLATE: int           = 0x20
MSG_TICKET: int          = 0x21
MSG_WANT_HEARTBEAT: int  = 0x40
MSG_HEARTBEAT: int       = 0x41
MSG_I_AM_CAMERA: int     = 0x80
MSG_I_AM_DISPATCHER: int = 0x81

# --- Shared State ---
state_lock = threading.Lock()

# Map RoadID -> List of connected dispatcher ClientHandlers
# We use a string forward reference 'ClientHandler' because the class isn't defined yet
dispatchers: Dict[RoadID, List['ClientHandler']] = defaultdict(list)

# Map RoadID -> List of buffered Ticket dictionaries
pending_tickets: Dict[RoadID, List[Ticket]] = defaultdict(list)

# Map RoadID -> Plate -> List of historical Observations
observations: Dict[RoadID, Dict[Plate, List[Observation]]] = defaultdict(lambda: defaultdict(list))

# Map Plate -> Set of Days (represented as ints) the car has been ticketed on
issued_tickets: Dict[Plate, Set[int]] = defaultdict(set)


class ClientHandler:
    def __init__(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        self.conn = conn
        self.addr = addr
        
        # Identity flags
        self.is_camera: bool = False
        self.is_dispatcher: bool = False
        
        # Camera specific data (defaults to 0)
        self.limit: int = 0
        self.road: RoadID = 0
        self.mile: Mile = 0
        
        self.socket_lock = threading.Lock()
        self.running: bool = True

    def send_bytes(self, data: bytes) -> None:
        """Thread-safe socket send."""
        with self.socket_lock:
            try:
                self.conn.sendall(data)
            except OSError:
                self.running = False

    def send_error(self, msg_str: str) -> None:
        """Sends error message and disconnects."""
        print(f"[ERROR] Sending error to {self.addr}: {msg_str}")
        payload = struct.pack('>B', len(msg_str)) + msg_str.encode('ascii')
        self.send_bytes(struct.pack('>B', MSG_ERROR) + payload)
        self.running = False

    def start_heartbeat(self, interval_deciseconds: Deciseconds) -> None:
        """Starts a background thread to send heartbeats."""
        def beat() -> None:
            interval_sec = interval_deciseconds / 10.0
            while self.running:
                time.sleep(interval_sec)
                if not self.running: 
                    break
                self.send_bytes(struct.pack('>B', MSG_HEARTBEAT))
        
        t = threading.Thread(target=beat, daemon=True)
        t.start()

    def process_plate(self, plate: Plate, timestamp: Timestamp) -> None:
        """
        Core Logic:
        1. Store observation.
        2. Check against ALL previous observations for this car on this road.
        3. If violation found -> Issue Ticket.
        """
        new_obs: Observation = (timestamp, self.mile, self.limit)

        with state_lock:
            # Get history for this car on this road
            history = observations[self.road][plate]
            history.append(new_obs)
            
            # Compare new observation against all historical ones
            # We exclude the last one (which is the one we just added)
            for old_obs in history[:-1]: 
                t1, m1, _ = old_obs
                t2, m2, _ = new_obs
                
                # Ensure t1 is the earlier time
                if t1 > t2:
                    t1, m1, t2, m2 = t2, m2, t1, m1
                
                if t1 == t2: 
                    continue # Ignore infinite speed / duplicate timestamps

                # Speed Calculation: Speed = (Distance / Time) * 3600
                distance = abs(m2 - m1)
                time_delta = t2 - t1
                speed_mph = (distance / time_delta) * 3600
                
                # Protocol expects speed in 100x integers
                speed_100: Speed100 = int(speed_mph * 100)
                limit_100: Speed100 = self.limit * 100
                
                # Check Limit (Limit + 0.5 mph buffer)
                if speed_100 >= (limit_100 + 50):
                    # --- VIOLATION FOUND ---
                    
                    # Check Day Constraints (timestamps are seconds)
                    day1 = t1 // 86400
                    day2 = t2 // 86400
                    
                    # Range is inclusive [day1, day2]
                    days_involved = set(range(day1, day2 + 1))
                    
                    # If any of these days are already in the set, we skip ticketing
                    if not days_involved.intersection(issued_tickets[plate]):
                        # Mark days as ticketed
                        issued_tickets[plate].update(days_involved)
                        
                        # Create the ticket using our TypedDict structure
                        ticket: Ticket = {
                            'plate': plate,
                            'road': self.road,
                            'mile1': m1,
                            'timestamp1': t1,
                            'mile2': m2,
                            'timestamp2': t2,
                            'speed': speed_100
                        }
                        
                        self.dispatch_ticket(ticket)

    def dispatch_ticket(self, ticket: Ticket) -> None:
        """Sends ticket to a dispatcher or buffers it."""
        road_dispatchers = dispatchers.get(ticket['road'], [])
        
        if road_dispatchers:
            # Send to the first available dispatcher
            dispatcher = road_dispatchers[0]
            dispatcher.send_ticket_msg(ticket)
        else:
            # No dispatcher connected for this road, buffer it.
            pending_tickets[ticket['road']].append(ticket)

    def send_ticket_msg(self, t: Ticket) -> None:
        """Encodes and sends a ticket message (Server -> Dispatcher)."""
        plate_bytes = t['plate'].encode('ascii')
        plate_len = len(plate_bytes)
        
        # Construct message
        msg = struct.pack('>B B', MSG_TICKET, plate_len)
        msg += plate_bytes
        msg += struct.pack('>H H I H I H', 
                           t['road'], 
                           t['mile1'], t['timestamp1'], 
                           t['mile2'], t['timestamp2'], 
                           t['speed'])
        
        self.send_bytes(msg)

    def run(self) -> None:
        try:
            buf: bytes = b""
            
            while self.running:
                # Receive chunk
                chunk = self.conn.recv(4096)
                if not chunk: 
                    break
                buf += chunk
                
                # Process buffer
                while len(buf) > 0 and self.running:
                    msg_type = buf[0]
                    
                    if msg_type == MSG_WANT_HEARTBEAT:
                        if len(buf) < 5: break
                        interval = struct.unpack('>I', buf[1:5])[0]
                        buf = buf[5:] 
                        if interval > 0:
                            self.start_heartbeat(interval)
                            
                    elif msg_type == MSG_I_AM_CAMERA:
                        # 1(Type) + 2(Road) + 2(Mile) + 2(Limit) = 7 bytes
                        if len(buf) < 7: break
                        
                        if self.is_camera or self.is_dispatcher:
                            self.send_error("Already identified")
                            return

                        self.road, self.mile, self.limit = struct.unpack('>HHH', buf[1:7])
                        self.is_camera = True
                        buf = buf[7:]

                    elif msg_type == MSG_I_AM_DISPATCHER:
                        # 1(Type) + 1(NumRoads) = 2 bytes minimum
                        if len(buf) < 2: break
                        
                        if self.is_camera or self.is_dispatcher:
                            self.send_error("Already identified")
                            return

                        num_roads = buf[1]
                        required_len = 2 + (num_roads * 2)
                        
                        if len(buf) < required_len: break
                        
                        road_ids: List[RoadID] = []
                        offset = 2
                        for _ in range(num_roads):
                            r = struct.unpack('>H', buf[offset:offset+2])[0]
                            road_ids.append(r)
                            offset += 2
                        
                        self.is_dispatcher = True
                        buf = buf[required_len:]
                        
                        # Register Dispatcher
                        with state_lock:
                            for r in road_ids:
                                dispatchers[r].append(self)
                                # Flush pending tickets
                                while pending_tickets[r]:
                                    t = pending_tickets[r].pop(0)
                                    self.send_ticket_msg(t)

                    elif msg_type == MSG_PLATE:
                        # 1(Type) + 1(Len) + Str + 4(Time)
                        if len(buf) < 2: break
                        plate_len = buf[1]
                        required_len = 1 + 1 + plate_len + 4
                        
                        if len(buf) < required_len: break
                        
                        if not self.is_camera:
                            self.send_error("Not a camera")
                            return

                        plate_str = buf[2:2+plate_len].decode('ascii')
                        timestamp = struct.unpack('>I', buf[2+plate_len : 2+plate_len+4])[0]
                        
                        self.process_plate(plate_str, timestamp)
                        buf = buf[required_len:]
                    
                    else:
                        self.send_error(f"Unknown message type {msg_type}")
                        return

        except Exception:
            pass 
        finally:
            with state_lock:
                if self.is_dispatcher:
                    for r in dispatchers:
                        if self in dispatchers[r]:
                            dispatchers[r].remove(self)
            self.running = False
            self.conn.close()

def start_server() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(200)
    print(f"Speed Camera Server listening on {HOST}:{PORT}")
    
    while True:
        conn, addr = server.accept()
        handler = ClientHandler(conn, addr)
        t = threading.Thread(target=handler.run)
        t.start()

if __name__ == "__main__":
    start_server()
