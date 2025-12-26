import asyncio
import json
import heapq
import itertools
from typing import Dict, List, Any, Optional, Set, Union, TypedDict, cast

class JobData(TypedDict):
    id: int
    pri: int
    queue: str
    job: Any  # The actual JSON payload

class Request(TypedDict, total=False):
    request: str
    queue: str
    queues: List[str]
    job: Any
    pri: int
    id: int
    wait: bool

class Response(TypedDict, total=False):
    status: str
    id: int
    job: Any
    pri: int
    queue: str
    error: str

class Job:
    def __init__(self, job_id: int, priority: int, queue_name: str, payload: Any):
        self.id = job_id
        self.priority = priority
        self.queue_name = queue_name
        self.payload = payload
        
        # State tracking
        self.is_deleted = False
        self.worker: Optional['ClientHandler'] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "pri": self.priority,
            "queue": self.queue_name,
            "job": self.payload
        }

class JobQueueServer:
    def __init__(self):
        self.id_counter = itertools.count(1)
        
        # Lookup all jobs by ID: id -> Job
        self.jobs: Dict[int, Job] = {}
        
        # Priority Queues: queue_name -> list of (-priority, unique_count, job)
        # We use a tie_breaker counter to ensure stable sorting for same-priority jobs
        self.queues: Dict[str, List[Any]] = {}
        self.tie_breaker = itertools.count()
        
        # Waiters: queue_name -> Set of Futures waiting for a job on this queue
        self.waiters: Dict[str, Set[asyncio.Future]] = {}

    def _push_to_queue(self, job: Job):
        """Pushes a job into its specific priority queue."""
        if job.queue_name not in self.queues:
            self.queues[job.queue_name] = []
        
        # Python heapq is a min-heap. We invert priority to make it a max-heap.
        # Format: (-priority, insertion_order, job)
        heapq.heappush(
            self.queues[job.queue_name], 
            (-job.priority, next(self.tie_breaker), job)
        )

    def _notify_waiters(self, job: Job) -> bool:
        """
        Checks if anyone is waiting for this job. 
        Returns True if the job was handed off to a waiter, False otherwise.
        """
        if job.queue_name not in self.waiters:
            return False

        waiting_futures = self.waiters[job.queue_name]
        
        while waiting_futures:
            # Pop a random waiter
            future = waiting_futures.pop()
            
            # If the waiter is still active (not cancelled/completed)
            if not future.done():
                # Hand the job directly to them!
                future.set_result(job)
                
                # Cleanup: Since this future is resolved, remove it from 
                # ANY other queues it might have been waiting on.
                # (The waiting logic in `get` handles this cleanup usually, 
                # but removing from this specific set is immediate).
                if not waiting_futures:
                    del self.waiters[job.queue_name]
                return True
        
        return False

    def put(self, priority: int, queue_name: str, payload: Any) -> int:
        job_id = next(self.id_counter)
        job = Job(job_id, priority, queue_name, payload)
        self.jobs[job_id] = job

        # 1. Try to give to a waiting client immediately
        if self._notify_waiters(job):
            # Assigned directly, state remains ready but held by future
            return job_id

        # 2. Otherwise, store in queue
        self._push_to_queue(job)
        return job_id

    def get_nowait(self, queue_names: List[str]) -> Optional[Job]:
        """
        Scans requested queues and returns the highest priority job found.
        Returns None if no jobs available.
        """
        best_queue = None
        best_priority = -float('inf')

        # Find which queue has the highest priority item top of heap
        for q_name in queue_names:
            pq = self.queues.get(q_name)
            if not pq:
                continue

            # Clean lazy-deleted jobs from top of heap
            while pq and (pq[0][2].is_deleted or pq[0][2].worker is not None):
                heapq.heappop(pq)
            
            if not pq:
                continue

            # Peek at priority (stored as negative)
            # pq[0] is (-priority, tie, job)
            current_pri = -pq[0][0]
            
            if current_pri > best_priority:
                best_priority = current_pri
                best_queue = q_name

        if best_queue:
            # Pop the actual job
            _, _, job = heapq.heappop(self.queues[best_queue])
            return job
        
        return None

    def delete(self, job_id: int) -> bool:
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        if job.is_deleted:
            return False

        # Mark as deleted.
        # If it's in a heap, it will be cleaned up lazily when popped.
        # If it's with a worker, the worker checks is_deleted before aborting.
        job.is_deleted = True
        
        # Remove from global lookup
        del self.jobs[job_id]
        
        # If a worker has it, they stop working on it effectively immediately
        if job.worker:
            job.worker.working_jobs.discard(job_id)
            job.worker = None
            
        return True

    def abort(self, job_id: int, client: 'ClientHandler') -> bool:
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        if job.is_deleted:
            return False
        
        # "It is an error for any client to attempt to abort a job 
        # that it is not currently working on."
        if job.worker != client:
            # Spec says "no-job" status for this case
            return False

        # Unassign
        job.worker = None
        client.working_jobs.discard(job_id)

        # Re-queue logic (Check waiters -> Queue)
        if not self._notify_waiters(job):
            self._push_to_queue(job)
            
        return True

class ClientHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: JobQueueServer):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.working_jobs: Set[int] = set()
        self.addr = writer.get_extra_info('peername')

    async def send_json(self, data: Response):
        try:
            msg = json.dumps(data) + "\n"
            self.writer.write(msg.encode('utf-8'))
            await self.writer.drain()
        except OSError:
            pass # Connection likely lost

    async def send_error(self, msg: str):
        await self.send_json({"status": "error", "error": msg})

    async def handle_put(self, req: Request):
        try:
            queue = req['queue']
            job_payload = req['job']
            pri = req['pri']
            
            if not isinstance(queue, str) or not isinstance(pri, int) or pri < 0:
                raise ValueError
        except (KeyError, ValueError, TypeError):
            await self.send_error("Invalid arguments for 'put'")
            return

        job_id = self.server.put(pri, queue, job_payload)
        await self.send_json({"status": "ok", "id": job_id})

    async def handle_get(self, req: Request):
        try:
            queues = req['queues']
            wait = req.get('wait', False)
            if not isinstance(queues, list) or not all(isinstance(q, str) for q in queues):
                raise ValueError
        except (KeyError, ValueError):
            await self.send_error("Invalid arguments for 'get'")
            return

        # 1. Try to get immediately
        job = self.server.get_nowait(queues)
        
        # 2. If no job and wait is True, we suspend
        if not job and wait:
            # Create a Future representing "waiting for a job"
            future = asyncio.get_running_loop().create_future()
            
            # Register this future for EVERY queue requested
            for q in queues:
                if q not in self.server.waiters:
                    self.server.waiters[q] = set()
                self.server.waiters[q].add(future)
            
            try:
                # Wait until one queue triggers the future OR client disconnects
                job = await future
            except asyncio.CancelledError:
                pass # Client disconnected or request cancelled
            finally:
                # Cleanup: Remove this future from all lists
                for q in queues:
                    if q in self.server.waiters:
                        self.server.waiters[q].discard(future)
                        if not self.server.waiters[q]:
                            del self.server.waiters[q]

        if job:
            # Mark as working
            job.worker = self
            self.working_jobs.add(job.id)
            
            resp = job.to_json()
            resp['status'] = 'ok'
            await self.send_json(resp)
        else:
            await self.send_json({"status": "no-job"})

    async def handle_delete(self, req: Request):
        try:
            job_id = req['id']
            if not isinstance(job_id, int): raise ValueError
        except (KeyError, ValueError):
            await self.send_error("Invalid arguments for 'delete'")
            return

        success = self.server.delete(job_id)
        if success:
            await self.send_json({"status": "ok"})
        else:
            await self.send_json({"status": "no-job"})

    async def handle_abort(self, req: Request):
        try:
            job_id = req['id']
            if not isinstance(job_id, int): raise ValueError
        except (KeyError, ValueError):
            await self.send_error("Invalid arguments for 'abort'")
            return

        # Spec: "It is an error for any client to attempt to abort a job 
        # that it is not currently working on." -> But spec also says send "no-job".
        # The text implies "no-job" status is the correct protocol response.
        
        success = self.server.abort(job_id, self)
        if success:
            await self.send_json({"status": "ok"})
        else:
            await self.send_json({"status": "no-job"})

    async def run(self):
        try:
            while True:
                # Read until newline
                line = await self.reader.readline()
                if not line:
                    break # EOF

                try:
                    req_obj = json.loads(line)
                    req_type = req_obj.get('request')
                except json.JSONDecodeError:
                    await self.send_error("Invalid JSON")
                    continue

                if req_type == 'put':
                    await self.handle_put(req_obj)
                elif req_type == 'get':
                    await self.handle_get(req_obj)
                elif req_type == 'delete':
                    await self.handle_delete(req_obj)
                elif req_type == 'abort':
                    await self.handle_abort(req_obj)
                else:
                    await self.send_error("Unknown request type")

        except Exception as e:
            pass # Disconnect
        finally:
            # Cleanup on disconnect
            # "all jobs that a client is working on are automatically aborted when that client disconnects"
            
            # Create a copy because abort modifies the set
            for job_id in list(self.working_jobs):
                self.server.abort(job_id, self)
            
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except: pass

async def main():
    server = JobQueueServer()

    async def cb(reader, writer):
        handler = ClientHandler(reader, writer, server)
        await handler.run()

    server_socket = await asyncio.start_server(cb, '0.0.0.0', 12345)
    print("Job Queue Server listening on 0.0.0.0:12345")
    
    async with server_socket:
        await server_socket.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
