import socket
import types
import selectors
import json
import numbers
import sympy

sel = selectors.DefaultSelector()

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
lsock.bind((HOST, PORT))
lsock.listen(5)
print(f"Listening on {HOST}:{PORT}")
lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=None)

def check(req: bytes):
    # req_str = req.decode("utf-8")
    try:
        req_obj = json.loads(req)
    except:
        return b'{"error":"Malformed request"}'
    
    if not "method" in req_obj.keys() or not "number" in req_obj.keys():
        return b'{"error":"Malformed request"}'
        
    if not req_obj["method"] == "isPrime" or not isinstance(req_obj["number"], numbers.Number):
        return b'{"error":"Malformed request"}'
    
    if sympy.isprime(req_obj["number"]):
        return b'{"method":"isPrime","prime":true}'
    else:
        return b'{"method":"isPrime","prime":false}'

def accept_wrapper(sock: socket.socket):
    (conn, addr) = sock.accept()  # Should be ready to read
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)
    
def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            print(recv_data)
            resp = check(recv_data)
            data.outb += resp
        else:
            print(f"Closing connection to {data.addr}")
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print(f"Echoing {data.outb!r} to {data.addr}")
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]

try:
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)
except KeyboardInterrupt:
    print("Caught keyboard interrupt, exiting")
finally:
    sel.close()
  
