import asyncio
import struct
import sys

AUTHORITY_HOST = "pestcontrol.protohackers.com"
AUTHORITY_PORT = 20547
LISTEN_PORT = 20547

MSG_HELLO = 0x50
MSG_ERROR = 0x51
MSG_OK = 0x52
MSG_DIAL_AUTHORITY = 0x53
MSG_TARGET_POPULATIONS = 0x54
MSG_CREATE_POLICY = 0x55
MSG_DELETE_POLICY = 0x56
MSG_POLICY_RESULT = 0x57
MSG_SITE_VISIT = 0x58

ACTION_CULL = 0x90
ACTION_CONSERVE = 0xa0

# --- Helper Classes ---

class ProtocolError(Exception):
    pass

class ConnectionClosed(Exception):
    pass

class Packet:
    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self.payload = payload

# --- Protocol Handling ---

class PestProtocol:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def read_packet(self) -> Packet:
        # 1. Read Type (1 byte)
        try:
            type_byte = await self.reader.readexactly(1)
        except asyncio.IncompleteReadError:
            raise ConnectionClosed()

        msg_type = type_byte[0]

        # 2. Read Length (4 bytes)
        try:
            len_bytes = await self.reader.readexactly(4)
        except asyncio.IncompleteReadError:
            raise ProtocolError("Truncated length")
        
        total_len = struct.unpack('>I', len_bytes)[0]
        
        # Min length: Type(1) + Len(4) + Checksum(1) = 6
        if total_len < 6:
            raise ProtocolError(f"Invalid length {total_len}")
        
        if total_len > 10_000_000:
            raise ProtocolError("Message too large")

        # 3. Read Content + Checksum
        # remaining = Total - Type(1) - Len(4) = Total - 5
        remaining_bytes = total_len - 5
        try:
            data = await self.reader.readexactly(remaining_bytes)
        except asyncio.IncompleteReadError:
            raise ProtocolError("Truncated body")

        # 4. Validate Checksum
        checksum_acc = msg_type
        for b in len_bytes: checksum_acc += b
        for b in data: checksum_acc += b
        
        if checksum_acc % 256 != 0:
            raise ProtocolError("Invalid checksum")

        # Payload is data excluding the last byte (checksum)
        payload = data[:-1]
        return Packet(msg_type, payload)

    def write_packet(self, msg_type: int, payload: bytes):
        total_len = 1 + 4 + len(payload) + 1
        header = struct.pack('>B I', msg_type, total_len)
        partial_msg = header + payload
        current_sum = sum(partial_msg)
        checksum_byte = (256 - (current_sum % 256)) % 256
        self.writer.write(partial_msg + bytes([checksum_byte]))

    async def send_error(self, message: str):
        try:
            self.write_packet(MSG_ERROR, self.pack_str(message))
            await self.writer.drain()
        except Exception:
            pass

    # --- Type Parsing/Writing Helpers ---

    @staticmethod
    def parse_u32(data, offset):
        if offset + 4 > len(data): raise ProtocolError("u32 out of bounds")
        val = struct.unpack('>I', data[offset:offset+4])[0]
        return val, offset + 4

    @staticmethod
    def parse_str(data, offset):
        length, offset = PestProtocol.parse_u32(data, offset)
        if offset + length > len(data): raise ProtocolError("str out of bounds")
        s = data[offset:offset+length].decode('ascii')
        return s, offset + length

    @staticmethod
    def pack_u32(val):
        return struct.pack('>I', val)

    @staticmethod
    def pack_str(s):
        b = s.encode('ascii')
        return struct.pack('>I', len(b)) + b

# --- Site Authority Controller ---

class SiteAuthorityController:
    def __init__(self, site_id):
        self.site_id = site_id
        self.proto: PestProtocol = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.target_populations = {} 
        self.active_policies = {}    
        self.current_actions = {}    

    async def ensure_connection(self):
        if self.connected: return

        try:
            reader, writer = await asyncio.open_connection(AUTHORITY_HOST, AUTHORITY_PORT)
            self.proto = PestProtocol(reader, writer)
            
            # Send Hello
            self.proto.write_packet(MSG_HELLO, 
                PestProtocol.pack_str("pestcontrol") + PestProtocol.pack_u32(1))
            
            resp = await self.proto.read_packet()
            if resp.msg_type != MSG_HELLO:
                raise ProtocolError("Expected Hello")
            
            # Dial Site
            self.proto.write_packet(MSG_DIAL_AUTHORITY, PestProtocol.pack_u32(self.site_id))
            
            resp = await self.proto.read_packet()
            if resp.msg_type != MSG_TARGET_POPULATIONS:
                raise ProtocolError("Expected TargetPopulations")
            
            off = 0
            site_check, off = PestProtocol.parse_u32(resp.payload, off)
            count, off = PestProtocol.parse_u32(resp.payload, off)
            
            self.target_populations = {}
            for _ in range(count):
                species, off = PestProtocol.parse_str(resp.payload, off)
                min_pop, off = PestProtocol.parse_u32(resp.payload, off)
                max_pop, off = PestProtocol.parse_u32(resp.payload, off)
                self.target_populations[species] = (min_pop, max_pop)
            
            # Reset policies on new connection
            self.active_policies = {}
            self.current_actions = {}
            self.connected = True
            
        except Exception as e:
            self.connected = False
            if self.proto:
                try: self.proto.writer.close()
                except: pass
            raise e

    async def process_visit(self, observations: dict):
        async with self.lock:
            await self.ensure_connection()
            
            desired_actions = {}
            for species, (min_p, max_p) in self.target_populations.items():
                count = observations.get(species, 0)
                if count < min_p: desired_actions[species] = ACTION_CONSERVE
                elif count > max_p: desired_actions[species] = ACTION_CULL
                else: desired_actions[species] = None
            
            # Reconcile policies
            # We must iterate over all species we know about (targets) to manage their policies
            # Also need to handle species that might have active policies but are no longer targets?
            # Spec: "Where a species is observed ... but not present in TargetPopulations ... do not advise any policy."
            
            for species, action in desired_actions.items():
                current_policy_id = self.active_policies.get(species)
                current_action = self.current_actions.get(species)
                
                if action is not None and current_policy_id is None:
                    await self.send_create(species, action)
                elif action is None and current_policy_id is not None:
                    await self.send_delete(current_policy_id)
                    del self.active_policies[species]
                    del self.current_actions[species]
                elif action is not None and current_policy_id is not None and action != current_action:
                    await self.send_delete(current_policy_id)
                    await self.send_create(species, action)

    async def send_create(self, species, action):
        payload = PestProtocol.pack_str(species) + bytes([action])
        self.proto.write_packet(MSG_CREATE_POLICY, payload)
        
        resp = await self.proto.read_packet()
        if resp.msg_type == MSG_ERROR:
             s, _ = PestProtocol.parse_str(resp.payload, 0)
             raise ProtocolError(f"Authority Error: {s}")
             
        if resp.msg_type != MSG_POLICY_RESULT:
            raise ProtocolError(f"Expected PolicyResult, got {resp.msg_type}")
            
        policy_id, _ = PestProtocol.parse_u32(resp.payload, 0)
        self.active_policies[species] = policy_id
        self.current_actions[species] = action

    async def send_delete(self, policy_id):
        self.proto.write_packet(MSG_DELETE_POLICY, PestProtocol.pack_u32(policy_id))
        resp = await self.proto.read_packet()
        if resp.msg_type != MSG_OK: raise ProtocolError("Expected OK on delete")

# --- Global Site Manager ---

class SiteManager:
    def __init__(self):
        self.controllers = {}
        self.lock = asyncio.Lock()

    async def get_controller(self, site_id):
        async with self.lock:
            if site_id not in self.controllers:
                self.controllers[site_id] = SiteAuthorityController(site_id)
            return self.controllers[site_id]

site_manager = SiteManager()

# --- Server / Client Handler ---

async def handle_client(reader, writer):
    proto = PestProtocol(reader, writer)
    
    try:
        # Send our Hello
        proto.write_packet(MSG_HELLO, 
            PestProtocol.pack_str("pestcontrol") + PestProtocol.pack_u32(1))
        
        # Read Client Hello
        try:
            pkt = await proto.read_packet()
        except ProtocolError as e:
            await proto.send_error(str(e))
            return

        if pkt.msg_type != MSG_HELLO:
            await proto.send_error("Expected Hello")
            return
            
        # Parse Hello
        try:
            off = 0
            protocol_name, off = PestProtocol.parse_str(pkt.payload, off)
            version, off = PestProtocol.parse_u32(pkt.payload, off)
            
            # CRITICAL CHECK: Unused bytes
            if off != len(pkt.payload):
                raise ProtocolError("Trailing bytes in Hello")

        except ProtocolError as e:
            await proto.send_error("Malformed Hello")
            return
        
        if protocol_name != "pestcontrol" or version != 1:
            await proto.send_error("Unsupported protocol/version")
            return

        # Main Loop
        while True:
            try:
                pkt = await proto.read_packet()
            except ProtocolError as e:
                await proto.send_error(str(e))
                break
            
            if pkt.msg_type == MSG_SITE_VISIT:
                try:
                    off = 0
                    site_id, off = PestProtocol.parse_u32(pkt.payload, off)
                    count, off = PestProtocol.parse_u32(pkt.payload, off)
                    
                    observations = {}
                    for _ in range(count):
                        species, off = PestProtocol.parse_str(pkt.payload, off)
                        cnt, off = PestProtocol.parse_u32(pkt.payload, off)
                        
                        if species in observations and observations[species] != cnt:
                            raise ProtocolError("Conflicting species counts")
                        observations[species] = cnt
                    
                    # CRITICAL CHECK: Unused bytes
                    if off != len(pkt.payload):
                        raise ProtocolError("Trailing bytes in SiteVisit")

                    # Process Valid Visit
                    controller = await site_manager.get_controller(site_id)
                    await controller.process_visit(observations)
                    
                except ProtocolError as e:
                    await proto.send_error(str(e))
                    break
                except Exception as e:
                    print(f"Logic Error: {e}")
            
            elif pkt.msg_type == MSG_ERROR:
                break
            else:
                await proto.send_error(f"Unexpected message type {pkt.msg_type}")
                break

    except ConnectionClosed:
        pass
    except Exception as e:
        print(f"Internal Error: {e}")
    finally:
        writer.close()
        try: await writer.wait_closed()
        except: pass

async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', LISTEN_PORT)
    print(f"Serving on port {LISTEN_PORT}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
