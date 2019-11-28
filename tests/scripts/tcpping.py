import socket
import struct
import random
import time
import select
from httprunner.exceptions import ScriptExecuteError


class Tcpping:

    ICMP_ECHO_REQUEST = 8
    ICMP_CODE = socket.getprotobyname('icmp')

    # def __init__(self):
    #     super().__init__()

    def execute(self, params: dict):
        """
        Sends one ping to the given "dest_addr" which can be an ip or hostname.
        "timeout" can be any integer or float except negatives and zero.
        Returns either the delay (in seconds) or None on timeout and an invalid
        address, respectively.
        """
        start = time.time()

        addr = params.get('url', '')
        # print('addr=', addr)
        try:
            my_socket = socket.socket(socket.AF_INET, socket.SOCK_RAW, Tcpping.ICMP_CODE)
        except socket.error as e:
            raise ScriptExecuteError(e)

        # try:
        #     host = socket.gethostbyname(addr)
        # except socket.gaierror as e:
        #     raise ScriptExecuteError(e)

        # Maximum for an unsigned short int c object counts to 65535 so
        # we have to sure that our packet id is not greater than that.

        timeout = params.get('timeout', 5)
        packet_id = int((id(timeout) * random.random()) % 65535)
        packet = self.create_packet(packet_id)
        while packet:
            # The icmp protocol does not use a port, but the function
            # below expects it, so we just give it a dummy port.
            sent = my_socket.sendto(packet, (addr, 1))
            packet = packet[sent:]
        delay = self.receive_ping(my_socket, packet_id, time.time(), timeout)
        # print("response_time:", delay)
        if delay:
            self.meta_data['stat']['response_time_ms'] = int(delay * 1000)
        self.meta_data['stat']['elapsed_ms'] = int((time.time() - start) * 1000)

        my_socket.close()
        return delay

    def create_packet(self, id):
        """Create a new echo request packet based on the given "id"."""
        # Header is type (8), code (8), checksum (16), id (16), sequence (16)
        header = struct.pack("bbHHh", Tcpping.ICMP_ECHO_REQUEST, 0, 0, id, 1)
        bytes_in_double = struct.calcsize("d")
        data = (192 - bytes_in_double) * "Q"
        data = struct.pack("d", time.time()) + bytes(data.encode('utf-8'))

        # Get the checksum on the data and the dummy header.
        my_checksum = self.checksum(header + data)
        header = struct.pack(
            "bbHHh", Tcpping.ICMP_ECHO_REQUEST, 0, socket.htons(my_checksum), id, 1
        )
        return header + data

    def checksum(self, source_string):
        # I'm not too confident that this is right but testing seems to
        # suggest that it gives the same answers as in_cksum in ping.c.
        sum = 0
        max_count = (len(source_string) / 2) * 2
        count = 0
        while count < max_count:
            val = source_string[count + 1] * 256 + source_string[count]
            sum = sum + val
            sum = sum & 0xffffffff
            count = count + 2

        if max_count < len(source_string):
            sum = sum + ord(source_string[len(source_string) - 1])
            sum = sum & 0xffffffff

        sum = (sum >> 16) + (sum & 0xffff)
        sum = sum + (sum >> 16)
        answer = ~sum
        answer = answer & 0xffff
        answer = answer >> 8 | (answer << 8 & 0xff00)
        return answer

    def receive_ping(self, sock, id, time_sent, timeout):
        # Receive the ping from the socket.
        time_remaining = timeout
        while True:
            start_time = time.time()
            readable = select.select([sock], [], [], time_remaining)
            time_spent = (time.time() - start_time)
            if readable[0] == []:  # Timeout
                return

            time_received = time.time()
            recv_packet, addr = sock.recvfrom(1024)
            icmp_header = recv_packet[20:28]
            type, code, checksum, packet_id, sequence = struct.unpack(
                "bbHHh", icmp_header
            )
            if packet_id == id:
                bytes_in_double = struct.calcsize("d")
                time_sent = struct.unpack("d", recv_packet[28:28 + bytes_in_double])[0]
                return time_received - time_sent

            time_remaining = time_remaining - time_spent
            if time_remaining <= 0:
                return

