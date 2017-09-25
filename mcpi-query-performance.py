#!/usr/bin/python
# Python 2.7

"""Minecraft MCPI query performance

This program is to test the performance of various techniques to
query large numbers (hundreds or thousands) of Minecraft blocks.
This applies only to the Minecraft Pi edition.

"""

from __future__ import absolute_import, division, print_function
#from __future__ import unicode_literals

# mcpi is found in /usr/lib/python2.7/dist-packages
import mcpi.minecraft as minecraft
import mcpi.block as block
from   mcpi.vec3 import Vec3
from   mcpi.connection import Connection
import collections
import io
import select
import socket
import threading
import time
import timeit
import Queue

global mc

class Cuboid:
    """A cuboid represents a 3-dimensional rectangular area

    The constructor takes ranges along the x, y, and z axis,
    where each range is a tuple(min, max) of world coordinates.
    Ranges are integers and are used like the Python built-in range
    function where the second number is one past the end.  Specifically,
    to get a single point along an axis, use something like tuple(0, 1).

    Examples:
      # A point at (100,12, 5)
      >>> point = Cuboid((100,101), (12,13), (5,6))

      # An 11 by 11 square centered around (0,0):
      >>> c = Cuboid((-5, 6), (0, 1), (-5, 6))
      >>> c.x_range
      (-5, 6)
      >>> g = c.generate()
      >>> for point in g: print(p)
    """
    def __init__(self, x_range, y_range, z_range):
        if x_range[0] >= x_range[1]: raise RuntimeError("bad x")
        if y_range[0] >= y_range[1]: raise RuntimeError("bad y")
        if z_range[0] >= z_range[1]: raise RuntimeError("bad z")
        self.x_range = x_range
        self.y_range = y_range
        self.z_range = z_range
    def __repr__(self):
        return "(%s, %s, %s)" % (str(self.x_range), str(self.y_range), str(self.z_range))
    def generate(self):
        for x in range(*self.x_range):
            for y in range(*self.y_range):
                for z in range(*self.z_range):
                    yield (x, y, z)
    def generate_xz(self):
        for x in range(*self.x_range):
            for z in range(*self.z_range):
                yield (x, z)

    def total_blocks(self):
        return ((self.x_range[1] - self.x_range[0]) *
                (self.y_range[1] - self.y_range[0]) *
                (self.z_range[1] - self.z_range[0]))


### ----------------------------------------------------------------------
### Try getting block data in a loop
### ----------------------------------------------------------------------
def try_basic_getBlock(input_cuboid):
    """Test the performance of using getBlock() in a loop"""
    my_blocks = {}
    starttime = timeit.default_timer()
    for pos in input_cuboid.generate():
        my_blocks[pos] = mc.getBlockWithData(pos)
    return my_blocks


### ----------------------------------------------------------------------
### Try getting block data using multiple threads
### ----------------------------------------------------------------------
def get_blocks_using_multiple_threads(c1, c2, thread_count=100):
    """get a cuboid of Minecraft block data

    Purpose:
        Use multiple threads to get Minecraft block data faster
        than by calling Minecraft.getBlockWithData() in a loop.

    parms:
        c1, c2: the corners of the cuboid, where each corner is
                a mcpi.vec3.Vec3
        thread_count: the number of threads (also number of connections)
    returns:
        dict from mcpi.vec3.Vec3 to mcpi.block.Block
    """
    # Set up the work queue
    c1.x, c2.x = sorted((c1.x, c2.x))
    c1.y, c2.y = sorted((c1.y, c2.y))
    c1.z, c2.z = sorted((c1.z, c2.z))
    workq = Queue.Queue()
    for x in range(c1.x, c2.x):
        for y in range(c1.y, c2.y):
            for z in range(c1.z, c2.z):
                workq.put((x,y,z))
    print("Getting data for %d blocks" % workq.qsize())

    # Create connections (with one socket for each) for each thread
    # To do: close the socket
    connections = [Connection("localhost", 4711) for i in range(0,thread_count)]

    # The worker threads
    def worker_fn(connection, workq, outq):
        try:
          while True:
            pos = workq.get(False)
            #print("working " + str(pos[0]) + str(pos[1]) + str(pos[2]))
            connection.send("world.getBlockWithData", pos[0], pos[1], pos[2])
            ans = connection.receive()
            #print("Got "+ans)
            blockid, blockdata = map(int, ans.split(","))
            outq.put((pos, (blockid, blockdata)))
        except Queue.Empty:
            pass

    # Create worker threads
    outq = Queue.Queue()
    workers = []
    for w in range(thread_count):
        t = threading.Thread(target = worker_fn,
                             args = (connections[w], workq, outq))
        t.start()
        workers.append(t)

    # Wait for workers to finish
    for w in workers:
        # print("waiting for", w.name)
        w.join()

    # Collect results
    answer = {}
    while not outq.empty():
        pos, block = outq.get()
        answer[pos] = block[0]   # only collect blockId
    return answer


def try_multiple_threads(input_cuboid):
    thread_count = 200
    corner1 = Vec3(input_cuboid.x_range[0],
                   input_cuboid.y_range[0],
                   input_cuboid.z_range[0])
    corner2 = Vec3(input_cuboid.x_range[1],
                   input_cuboid.y_range[1],
                   input_cuboid.z_range[1])
    my_blocks = get_blocks_using_multiple_threads(
        corner1,
        corner2,
        thread_count)
    return my_blocks

    # Performance experiments
    """Results:
        Hardware: Raspbery Pi 3 Model B V1.2 with heat sink
        Linux commands show:
          $ uname -a
          Linux raspberrypi 4.4.34-v7+ #930 SMP Wed Nov 23 15:20:41 GMT 2016 armv7l GNU/Linux
          $ lscpu
          Architecture:          armv7l
          Byte Order:            Little Endian
          CPU(s):                4
          On-line CPU(s) list:   0-3
          Thread(s) per core:    1
          Core(s) per socket:    4
          Socket(s):             1
          Model name:            ARMv7 Processor rev 4 (v7l)
          CPU max MHz:           1200.0000
          CPU min MHz:           600.0000
        GPU memory is 128 (unit is Mb, I think)

        Test getting 10201 blocks, and stack_size=128Kb
        varying the number of threads:
           threads   time(sec)  blocks/sec
           -------   ---------  ----------
            10       39.87
            25       19.46
            50       10.68
            75        7.29
           100        5.57
           115        5.01
           120        4.86
           125        4.75
           130        4.58
           150        4.47
           175        4.55
           200        4.24
           250        4.41
           400        4.60
        Observations:
         - Each thread process 15 to 25 blocks/sec
         - Some blocks take much longer to fetch, about 0.3 sec
         - performance peaks with 200 threads, at 2400 blocks/sec
         - creating threads is not free
            - can create 50 threads in 1 sec, 100 threads in 2.5 sec
            - memory consumption increases (not measured)
         - the tests were repeated while the game was being
           played interactively, specifically, flying at altitude
           and looking down so that new blocks were being fetched
           as quickly as possible.  This did not affect performance:
            + no graphical slowdowns or glitches were observed
            + the performance of fetching blocks was not affected
        Note:
            The expected case is to create the required threads once
            and keep them around for the lifetime of the program.
            The experimental code was designed to do just that.
            Some data was captured that suggests how expensive
            starting up hundreds of threads is.  Although it was
            not one of the objectives of the original study, it is
            given as an interesting observation.
        Conclusions:
           Eyeballing the data suggests that 200 threads is optimal.
           However, if the 6 seconds it takes to create the threads
           is not acceptable, consider using 100 threads which is
           about 30% slower, but only takes 1 second to create the
           threads.
    """
    '''
    threading.stack_size(128*1024)
    for thread_count in [100, 150, 200]:
        connections = []
        corner1 = Vec3(-50, 8, -50)
        corner2 = Vec3( 50, 8,  50)
        starttime = timeit.default_timer()
        blks = get_blocks_in_parallel(corner1, corner2, thread_count)
        endtime = timeit.default_timer()
        blks = get_blocks_in_parallel(corner1, corner2, thread_count)
        endtime2 = timeit.default_timer()
        print("entries=10201 thread_count=%s time1=%s time2=%s" % (
            str(thread_count),
            str(endtime-starttime),
            str(endtime2-endtime)))
    '''


### --------------------------------------------------------------------
### Try getting block data by stuffing queries into the socket
### without pausing for an answer.
### --------------------------------------------------------------------
def do_queries_with_socket_stuffing(connection, requests, fmt, parse_fn):
    """Perform many query operations and return the results.

    Purpose:
        Flood the server socket with queries, not waiting for a
        response before sending the next request, to get data
        about Minecraft blocks much faster than by calling
        Minecraft.getBlockWithData() in a loop.

    parms:
     - connection: connection to a running MC server
     - requests: an iterable of requests, usually coordinate tuples
     - fmt: how to format each request, used like:
            "world.getBlock(%d,%d,%d)" % request
     - parse_fn: How to parse each MC server response, like: int
    generates:
     Generates tuple(request, response) where
       each request is from requests, and
       each response is from parse_fn().
    example:
      >>>for coord, blockid in do_queries(conn,
      >>>                                 ((x,0,0) for x in range(-10,10)),
      >>>                                 "world.getBlock(%d,%d,%d)",
      >>>                                 int):
      >>>    print(coord, blockid)


    The flow of requests and responses is:

          requests (input parameter)
            |
            V
          request
            |
            +-----------------+
            |                 |
            V                 |
          request_buffer      |
            |                 V
            V            request_queue
          socket              |
            |                 |
            V                 |
          response_buffer     |
            |                 |
            V                 |
          response            |
            |                 V
            +<----------------+
            |
            V
          yield

      Key data structures in the data flow are:
        requests
                The input parameter which iterates all the requests.
        request
                A single request, e.g., tuple(x,y,z)
        request_queue
                A queue of requests that we sent to the server and
                are waiting for a response.
        request_buffer
                The string version of a request, e.g.,
                "world.getBlock(5,6,7)"
        socket
                The connection to the minecraft server
        response_buffer
                String output from the server, e.g., "0,0"
        response
                A single response, e.g., tuple(0,0)

    The tricky part is to perform socket I/O on both ends
    of the socket without blocking (waiting for data).  We use
    select.select() to work I/O properly.

    Performance:
      My Raspberry Pi 3 Model B running gets 640 blocks/second.
    """

    # We write requests (like b"world.getBlock(0,0,0)" into the
    # request_buffer and then into the request_file (socket).
    request_buffer = bytes()  # bytes, bytearray, or memoryview
    request_file = io.FileIO(connection.socket.fileno(), "w", closefd=False)
    request_queue = collections.deque()
    have_more_requests = True   # "requests" has more items

    # We read responses (like b"2,0") from the response_file (socket)
    # into the response_buffer.
    response_file = io.FileIO(connection.socket.fileno(), "r", closefd=False)
    response_buffer = bytes()

    #req_count = 0
    request_iter = iter(requests)
    while have_more_requests or len(request_queue) > 0:
        # Fill up the request buffer
        while have_more_requests and len(request_buffer) < 4096:
            try:
                request = request_iter.next()
            except StopIteration:
                have_more_requests = False
                continue
            request_buffer = request_buffer + (fmt % request) + "\n"
            request_queue.append(request)
            #req_count += 1

        # Select which I/0 we can perform without blocking
        w = [request_file] if len(request_buffer) > 0 else []
        #print("select'ing: sent=", req_count, "req_queue size=", len(request_queue))
        r, w, x = select.select([response_file], w, [], 5)
        allow_read = bool(r)
        allow_write = bool(w)
        #print("select'ed R=", allow_read, "W=", allow_write)

        # Write requests to the server
        if allow_write: # len(request_buffer) > 0:  # AND if select says okay
            # Write exactly once
            bytes_written = request_file.write(request_buffer)
            request_buffer = request_buffer[bytes_written:]
            if bytes_written == 0:
                raise RuntimeError("unexpected socket.file.write()=0")

        # Read responses from the server
        if allow_read:  # if select says okay
            # Read exactly once
            bytes_read = connection.socket.recv(1024)
            response_buffer = response_buffer + bytes_read
            if bytes_read == 0:
                raise RuntimeError("unexpected socket.recv()=0")

        # Parse the response strings
        # TO DO: Convert bytes to string
        responses = response_buffer.split("\n")  # Use splitlines?
        response_buffer = responses[-1]
        responses = responses[:-1]
        for response_string in responses:
            request = request_queue.popleft()
            #print("Answer:", request, "->", response_string)
            yield (request, parse_fn(response_string))


def try_socket_stuffing(input_cuboid):
    connection = mc.conn

    if True:
        my_blocks = {}
        for pos, blk in do_queries_with_socket_stuffing(
                            connection,
                            input_cuboid.generate(),
                            "world.getBlock(%d,%d,%d)",
                            int):
            my_blocks[pos] = blk
        return my_blocks

def try_socket_stuffing2(input_cuboid):
    """Show how to use getBlockWithData and getHeight."""
    connection = mc.conn

    # Test world.getBlockWithData(x,y,z) --> int,int
    # Also tests passing a list into do_queries
    if True:
        mc.setBlock(5, 5, 5, block.WOOL.id, 3)  # 3=green?
        mc.setBlock(5, 6, 5, block.WOOL.id, 5)  # 5=blue?
        def parse_two_ints(ans):
            return tuple(map(int, ans.split(",")))

        for pos, info in do_queries(connection,
                                    [(5,5,5), (5,6,5)],
                                    "world.getBlockWithData(%d,%d,%d)",
                                    parse_two_ints):
            print("  Block at", pos, "id=", info[0], "data=", info[1])

    # Test world.getHeight(x,z) --> int
    # Also a stress test
    # Displays a contour map of the world's height
    # You see a top view of the world, with darker squares for higher altitude.
    if False:
        my_square = Cuboid((-128,128), (0,1), (-128,128))
        # my_square = Cuboid((-20,20), (0,1), (-20,20))
        height = {}
        starttime = timeit.default_timer()
        for pos, blk in do_queries(connection,
                                   my_square.generate_xz(),
                                   "world.getHeight(%d,%d)",
                                   int):
            height[pos] = blk
        endtime = timeit.default_timer()
        print(endtime-starttime, 'do_queries')

        def height_to_char(h):
            bias = 0
            key = " .:-=+*#%@"
            #key = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            return key[max(0, min(len(key)-1, (h - bias) // 2))]
        for z in range(*my_square.z_range):
            s = ""
            for x in range(*my_square.x_range):
                s = s + height_to_char(height[(x, z)])
            print(s)

### --------------------------------------------------------------------
### Query the Minecraft server from multiple threads AND stuffing
### multiple requests into the socket before getting a reply.
### --------------------------------------------------------------------
"""query_blocks

Purpose:
  This generator is for getting a lot of data from the Minecraft server
  quickly.  For example, if you want data from a thousand blocks you
  could call getBlock for each block, but that would take a long time.
  This function essentially calls getBlock for many blocks at the same
  time, thus improving throughput.
  If you want data for just a few blocks, prefer to use getBlock.

  The following query functions are supported:
    world.getBlock(x,y,z) -> blockId
    world.getBlockWithData(x,y,z) -> blockId,blockData
    world.getHeight(x,z) -> y

  Parameters:
    requests
            An iterable of coordinate tuples.  See the examples.
            Note that this will be called from different threads.
    fmt
            The request format string, one of:
                world.getBlock(%d,%d,%d)
                world.getBlockWithData(%d,%d,%d)
                world.getHeight(%d,%d)
    parse_fn
            Function to parse the results from the server, one of:
                int
                tuple(map(int, ans.split(",")))
    thread_count
            Number of threads to create.

  Generated values:
    tuple(request, answer), where
      request - is a value from the "requests" input parameter
      answer - is the response from the server, parsed by parse_fn

Query the Minecraft server quickly using two techniques:
 1. Create worker threads, each with its own socket connection.
 2. Each thread sends requests into the socket without waiting
    for responses.  Responses are then matched with requests.

The low-level design notes:
 - This uses a straightforward thread model
 - Creating more than 50 threads gets expensive.

 - The main thread creates the following
     request_lock = threading.Lock()  # Serialize access to requests
     answer_queue = queueing.Queue()  # Get answers from threads
     threads = threading.Thread()     # Worker threads
 - each thread:
     more_requests = True
     pending_request_queue = deque()
     loop until more_requests==False and pending_request_queue is empty:
       if more_requests:
         with request_lock:
            try:
                request = request_iter.next()
            except StopIteration:
                more_requests = False
                continue
            request_buffer = request_buffer + (fmt % request) + "\n"
            pending_request_queue.append(request)
         etc...

Constraints:
 - the "requests" iterator is invoked serially from different threads,
   so lists and simple generators work okay, but fancy stuff may not.
 - the order in which answers come back is not deterministic
"""

def query_blocks(requests, fmt, parse_fn, thread_count = 20):
    def worker_fn(mc_socket, request_iter, request_lock, answer_queue,):
        more_requests = True
        request_buffer = bytes()
        response_buffer = bytes()
        pending_request_queue = collections.deque()
        while more_requests or len(pending_request_queue) > 0:
            # Grab more requests
            while more_requests and len(request_buffer) < 4096:
                with request_lock:
                    try:
                        request = request_iter.next()
                    except StopIteration:
                        more_requests = False
                        continue
                    request_buffer = request_buffer + (fmt % request) + "\n"
                    pending_request_queue.append(request)

            # Select I/0 we can perform without blocking
            w = [mc_socket] if len(request_buffer) > 0 else []
            r, w, x = select.select([mc_socket], w, [], 5)
            allow_read = bool(r)
            allow_write = bool(w)

            # Write requests to the server
            if allow_write:
                # Write exactly once
                bytes_written = mc_socket.send(request_buffer)
                request_buffer = request_buffer[bytes_written:]
                if bytes_written == 0:
                    raise RuntimeError("unexpected socket.send()=0")

            # Read responses from the server
            if allow_read:
                # Read exactly once
                bytes_read = mc_socket.recv(1024)
                response_buffer = response_buffer + bytes_read
                if bytes_read == 0:
                    raise RuntimeError("unexpected socket.recv()=0")

            # Parse the response strings
            # TO DO: Convert bytes to string
            responses = response_buffer.split("\n")  # Use splitlines?
            response_buffer = responses[-1]
            responses = responses[:-1]
            for response_string in responses:
                request = pending_request_queue.popleft()
                answer_queue.put((request, parse_fn(response_string)))

    request_lock = threading.Lock() # to serialize workers getting
                                    # the next request
    answer_queue = Queue.Queue()  # To store answers coming back from
                                  # the worker threads
    sockets = []
    try:
        for i in range(thread_count):
            sockets.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            sockets[-1].connect(("localhost", 4711))
        workers = []
        threading.stack_size(128 * 1024)  # bytes
        for w in range(thread_count):
            t = threading.Thread(target = worker_fn,
                                 args = (sockets[w],
                                         iter(requests),
                                         request_lock,
                                         answer_queue))
            t.start()
            workers.append(t)

        # Wait for workers to finish
        for w in workers:
            w.join()
    except socket.error as e:
        print("Socket error:", e)
        print("Is the Minecraft server running?")
        raise e
    finally:
        for s in sockets:
            try:
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            except socket.error as e:
                pass

    # Collect results
    while not answer_queue.empty():
        yield answer_queue.get()


# Try the multi-threaded socket-stuffing technique
def try_multiple_threads_socket_stuffing(input_cuboid):
    my_cuboid = Cuboid((-30,30), (0,1), (-20,20))
    my_cuboid = Cuboid((-128,128), (0,1), (-128,128))
    my_blocks = {}

    for pos, blk in query_blocks(
            input_cuboid.generate(),
            "world.getBlock(%d,%d,%d)",
            int,
            thread_count=15):
        my_blocks[pos] = blk
    return my_blocks

    # Best performance runs with 65536 blocks:
    #  Threads  time(seconds)   blocks/second
    #   5       24              2621
    #   8       17.0            3855
    #  10       14.8            4428
    #  15       15.0            4369
    #  20       12.8            5120
    #  25       12.7            5160
    #  30       13.2            4964


########################################################################
### main
########################################################################

if __name__ == "__main__":
    try:
        mc = minecraft.Minecraft.create()
    except socket.error as e:
        print("Cannot connect to minecraft server")
        raise e

    # Set up the query size
    big_cuboid = Cuboid((-128,128), (0,1), (-128,128)) # 65536 blocks
    smaller_cuboid = Cuboid((-128,128), (0,1), (0,10)) # 2560 blocks
    tiny_cuboid = Cuboid((-2,3), (0,1), (-2,3))

    my_cuboid = big_cuboid

    # Start the timer
    starttime = timeit.default_timer()

    # Run the test
    if False:
        what_test = "basic_getBlock"
        my_cuboid = smaller_cuboid
        my_blocks = try_basic_getBlock(my_cuboid)
    if False:
        what_test = "multiple_threads"
        my_blocks = try_multiple_threads(my_cuboid)
    if False:
        what_test = "socket_stuffing"
        my_blocks = try_socket_stuffing(my_cuboid)
    if True:
        what_test = "multiple_threads_socket_stuffing"
        my_blocks = try_multiple_threads_socket_stuffing(my_cuboid)

    # Stop the timer
    endtime = timeit.default_timer()
    total_block_count = my_cuboid.total_blocks()
    overall_time = endtime - starttime

    # Announce results
    print("Total", total_block_count, "blocks in",
          overall_time, 'seconds using', what_test)
    print("Overall:", total_block_count / overall_time,
          "blocks/second")

    # Validate results (visually)
    # This gives a top view of the block data
    # Displays a slice of the world at y==0
    # A "slice" is like a layered cake where you remove
    # everything above and below the layer you are interested in
    # and get a top view of just that slice.
    # A "top view" is from a high place looking down.

    if True:
        for z in range(*my_cuboid.z_range):
            s = ""
            for x in range(*my_cuboid.x_range):
                c = " " if my_blocks[(x, 0, z)] == block.AIR.id else "x"
                s = s + c
            print(s)
