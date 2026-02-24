import asyncio
from collections import namedtuple
from io import BytesIO

class CommandError(Exception): pass
class Disconnect(Exception): pass

# A simple structure to represent Redis errors
Error = namedtuple('Error', ('message',))

class ProtocolHandler:
    def handle_request(self, reader):
        #Logic to read and parse the Redis command
        pass

    def write_response(self, writer, data):
        #Logic to write the response back to the client
        pass

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



            