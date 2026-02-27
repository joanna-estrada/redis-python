import asyncio
from collections import namedtuple
from io import BytesIO

class CommandError(Exception): pass
class Disconnect(Exception): pass

# A simple structure to represent Redis errors
Error = namedtuple('Error', ('message',))

#Wire Protocol
class ProtocolHandler:
    def __init__(self):
        # We must use byte prefixes (b'+') because asyncio readers return bytes
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict
        }

    async def handle_request(self, reader):
        first_byte = await reader.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Await the delegated handler
            return await self.handlers[first_byte](reader)
        except KeyError:
            raise CommandError('bad request')

    async def handle_simple_string(self, reader):
        line = await reader.readline()
        return line.rstrip(b'\r\n').decode('utf-8')

    async def handle_error(self, reader):
        line = await reader.readline()
        return Error(line.rstrip(b'\r\n').decode('utf-8'))

    async def handle_integer(self, reader):
        line = await reader.readline()
        return int(line.rstrip(b'\r\n'))

    async def handle_string(self, reader):
        line = await reader.readline()
        length = int(line.rstrip(b'\r\n'))
        if length == -1:
            return None  # Special-case for NULLs.
        length += 2  # Include the trailing \r\n in count.
        data = await reader.read(length)
        return data[:-2] # Returning as bytes, which is standard for Redis values

    async def handle_array(self, reader):
        line = await reader.readline()
        num_elements = int(line.rstrip(b'\r\n'))
        # Async list comprehension to read all elements
        return [await self.handle_request(reader) for _ in range(num_elements)]

    async def handle_dict(self, reader):
        line = await reader.readline()
        num_items = int(line.rstrip(b'\r\n'))
        elements = [await self.handle_request(reader) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    # --- SERIALIZATION ---

    async def write_response(self, writer, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        writer.write(buf.getvalue())
        # Drain ensures the data is actually sent over the network
        await writer.drain()

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(f"${len(data)}\r\n".encode('utf-8'))
            buf.write(data)
            buf.write(b"\r\n")
        elif isinstance(data, int):
            buf.write(f":{data}\r\n".encode('utf-8'))
        elif isinstance(data, Error):
            # Fixed from tutorial: changed error.message to data.message
            buf.write(f"-{data.message}\r\n".encode('utf-8'))
        elif isinstance(data, (list, tuple)):
            buf.write(f"*{len(data)}\r\n".encode('utf-8'))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f"%{len(data)}\r\n".encode('utf-8'))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b"$-1\r\n")
        else:
            raise CommandError(f"unrecognized type: {type(data)}")


# Skeleton
class Server:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self.host = host
        self.port = port
        self._limiter = asyncio.Semaphore(max_clients)
        self._protocol = ProtocolHandler()
        self.kv = {}

    async def connection_handler(self, reader, writer):
        async with self._limiter:
            address = writer.get_extra_info('peername')
            print(f"Connection received: {address}")

            try:
                while True:
                    try:
                        data = await self._protocol.handle_request(reader)
                    except Disconnect:
                        break
                    try:
                        resp = self.get_response(data)
                    except CommandError as exc:
                        resp = Error(exc.args[0])
                    await self._protocol.write_response(writer, resp)
            finally:
                writer.close()
                await writer.wait_closed()

    def get_response(self, data):
        #Redis commands
        pass

    async def run(self):
        server = await asyncio.start_server(
            self.connection_handler, self.host, self.port)
            
        async with server:
            print(f"Server started on {self.host}:{self.port}")
            await server.serve_forever()

if __name__ == "__main__":
    server = Server()
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("Server shutting down...")
        pass



            