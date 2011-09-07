"""
This module implements a client for the BloomD server.
"""
__all__ = ["BloomdError", "BloomdConnection", "BloomdClient", "BloomdFilter"]
__version__ = "0.1.0"
import logging
import socket
import errno
import time

class BloomdError(Exception):
    "Root of exceptions from the client library"
    pass

class BloomdConnection(object):
    "Provides a convenient interface to server connections"
    def __init__(self, server, timeout, attempts=3):
        """
        Creates a new Bloomd Connection.

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port".
                      Uses the default port of 8673 if none is provided.
            - timeout: The socket timeout to use.
            - attempts (optional): Maximum retry attempts on errors. Defaults to 3.
        """
        # Parse the host/port
        parts = server.split(":",1)
        if len(parts) == 2:
            host,port = parts[0],int(parts[1])
        else:
            host,port = parts[0],8673

        self.server = (host,port)
        self.timeout = timeout
        self.sock = self._create_socket()
        self.fh = None
        self.attempts = attempts
        self.logger = logging.getLogger("pybloom.BloomdConnection.%s.%d" % self.server)

    def _create_socket(self):
        "Creates a new socket, tries to connect to the server"
        # Connect the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.server)
        self.fh = None
        return s

    def send(self, cmd):
        "Sends a command with out the newline to the server"
        sent = False
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(cmd+"\n")
                sent = True
                break
            except socket.error, e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not sent:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError, "Cannot contact bloomd server!"

    def read(self):
        "Returns a single line from the file"
        if not self.fh: self.fh = self.sock.makefile()
        read = self.fh.readline().rstrip("\r\n")
        return read

    def readblock(self, start="START", end="END"):
        """
        Reads a response block from the server. The servers
        responses are between `start` and `end` which can be
        optionally provided. Returns an array of the lines within
        the block.
        """
        lines = []
        first = self.read()
        if first != start:
            raise BloomdError, "Did not get block start (%s)! Got '%s'!" % (start, first)
        while True:
            line = self.read()
            if line == end:
                break
            lines.append(line)
        return lines

    def send_and_receive(self, cmd):
        """
        Convenience wrapper around `send` and `read`. Sends a command,
        and reads the response, performing a retry if necessary.
        """
        done = False
        for attempt in xrange(self.attempts):
            try:
                self.send(cmd)
                return self.read()
            except socket.error, e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not done:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError, "Cannot contact bloomd server!"

    def response_block_to_dict(self):
        """
        Convenience wrapper around `readblock` to convert a block
        output into a dictionary by splitting on spaces, and using the
        first column as the key, and the remainder as the value.
        """
        resp_lines = self.readblock()
        return dict(tuple(l.split(" ",1)) for l in resp_lines)


class BloomdClient(object):
    "Provides a client abstraction around the BloomD interface."
    def __init__(self, servers, timeout=None):
        """
        Creates a new BloomD client. The client takes a list of
        servers, which are provided as strings in the "host" or "host:port"
        format. Optionally takes a socket timeout to use, but defaults
        to no timeout.
        """
        if len(servers) == 0: raise ValueError, "Must provide at least 1 server!"
        self.servers = servers
        self.timeout = timeout
        self.server_conns = {}
        self.server_info = None
        self.info_time = 0

    def _server_connection(self, server):
        "Returns a connection to a server, tries to cache connections."
        if server in self.server_conns:
            return self.server_conns[server]
        else:
            conn = BloomdConnection(server, self.timeout)
            self.server_conns[server] = conn
            return conn

    def _get_connection(self, filter, strict=True, explicit_server=None):
        """
        Gets a connection to a server which is able to service a filter.
        Because filters may exist on any server, all servers are queried
        for their filters and the results are cached. This allows us to
        partition data across multiple servers.

        :Parameters:
            - filter : The filter to connect to
            - strict (optional) : If True, an error is raised when a filter
                does not exist.
            - explicit_server (optional) : If provided, when a filter does
                not exist and strict is False, a connection to this server is made.
                Otherwise, the server with the fewest sets is returned.
        """
        if len(self.servers) == 1:
            serv = self.servers[0]
            return self._server_connection(serv)
        else:
            # Force checking if we have no info or 5 minutes has elapsed
            if not self.server_info or time.time() - self.info_time > 300:
                self.server_info = self.list_filters(inc_server=True)
                self.info_time = time.time()

            # Check if this filter is in a known location
            if filter in self.server_info:
                serv = self.server_info[filter][0]
                return self._server_connection(serv)

            # Possibly old data? Reload
            self.server_info = self.list_filters(inc_server=True)
            self.info_time = time.time()

            # Recheck
            if filter in self.server_info:
                serv = self.server_info[filter][0]
                return self._server_connection(serv)

            # Check if this is fatal
            if strict:
                raise BloomdError, "Filter does not exist!"

            # We have an explicit server provided to us, use that
            if explicit_server:
                return self._server_connection(explicit_server)

            # Does not exist, and is not not strict
            # we can select a server on any criteria then.
            # We will use the server with the minimal set count.
            counts = {}
            for server in self.servers:
                counts[server] = 0
            for filter,(server,info) in self.server_info.items():
                counts[server] += 1

            counts = [(count,srv) for srv,count in counts.items()]
            counts.sort()

            # Select the least used
            serv = counts[0][1]
            return self._server_connection(serv)

    def create_filter(self, name, capacity=None, prob=None, server=None):
        """
        Creates a new filter on the BloomD server and returns a BloomdFilter
        to interface with it. This may raise a BloomdError if the filter already
        exists.

        :Parameters:
            - name : The name of the new filter
            - capacity (optional) : The initial capacity of the filter
            - prob (optional) : The inital probability of false positives. If this is
                    provided, then size must also be provided. This is a bloomd limitation.
            - server (optional) : In a multi-server environment, this forces the
                    filter to be created on a specific server. Should be provided
                    in the same format as initialization "host" or "host:port".
        """
        if prob and not capacity: raise ValueError, "Must provide size with probability!"
        conn = self._get_connection(name, strict=False, explicit_server=server)
        cmd = "create %s" % name
        if capacity: cmd += " %d" % capacity
        if prob: cmd += " %f" % prob
        conn.send(cmd)
        resp = conn.read()
        if resp == "Done":
            return BloomdFilter(conn, name)
        else:
            raise BloomdError, "Got response: %s" % resp

    def __getitem__(self, name):
        "Gets a BloomdFilter object based on the name."
        conn = self._get_connection(name)
        return BloomdFilter(conn, name)

    def list_filters(self, inc_server=False):
        """
        Lists all the available filters across all servers.
        Returns a dictionary of {filter_name : filter_info}.

        :Parameters:
            - inc_server (optional) : If true, the dictionary values
               will be (server, filter_info) instead of filter_info.
        """
        # Send the list to all first
        for server in self.servers:
            conn = self._server_connection(server)
            conn.send("list")

        # Check response from all
        responses = {}
        for server in self.servers:
            conn = self._server_connection(server)
            resp = conn.readblock()
            for line in resp:
                name,info = line.split(" ",1)
                if inc_server:
                    responses[name] = server,info
                else:
                    responses[name] = info

        return responses

    def flush(self):
        "Instructs all servers to flush to disk"
        # Send the flush to all first
        for server in self.servers:
            conn = self._server_connection(server)
            conn.send("flush")

        # Check response from all
        for server in self.servers:
            conn = self._server_connection(server)
            resp = conn.read()
            if resp != "Done":
                raise BloomdError, "Got response: '%s' from '%s'" % (resp, server)

    def conf(self):
        "Returns the configuration dictionary of the first server."
        conn = self._server_connection(self.servers[0])
        conn.send("conf")
        return conn.response_block_to_dict()

class BloomdFilter(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, conn, name):
        """
        Creates a new BloomdFilter object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the filter
        """
        self.conn = conn
        self.name = name

    def add(self, key):
        "Adds a new key to the filter. Returns True/False if the key was added."
        resp = self.conn.send_and_receive("set %s %s" % (self.name, key))
        if resp in ("Yes","No"):
            return resp == "Yes"
        raise BloomdError, "Got response: %s" % resp

    def drop(self):
        "Deletes the filter from the server. This is permanent"
        resp = self.conn.send_and_receive("drop %s" % (self.name))
        if resp != "Done":
            raise BloomdError, "Got response: %s" % resp

    def __contains__(self, key):
        "Checks if the key is contained in the filter."
        resp = self.conn.send_and_receive("check %s %s" % (self.name, key))
        if resp in ("Yes","No"):
            return resp == "Yes"
        raise BloomdError, "Got response: %s" % resp

    def __len__(self):
        "Returns the count of items in the filter."
        info = self.info()
        return int(info["size"])

    def info(self):
        "Returns the info dictionary about the filter."
        self.conn.send("info %s" % (self.name))
        return self.conn.response_block_to_dict()

    def flush(self):
        "Forces the filter to flush to disk"
        resp = self.conn.send_and_receive("flush %s" % (self.name))
        if resp != "Done":
            raise BloomdError, "Got response: %s" % resp

    def conf(self):
        "Returns the configuration dictionary of the filter"
        self.conn.send("conf %s" % (self.name))
        return self.conn.response_block_to_dict()

