import asyncio
from mini_redis import Client, CommandError

async def main():
    client = Client()
    await client.connect()

    print("Mini Redis CLI")
    print("Type commands like: SET name Joanna")
    print("Type 'exit' to quit\n")

    while True:
        try:
            user_input = input("redis> ").strip()

            if user_input.lower() in ("exit", "quit"):
                break

            if not user_input:
                continue

            # Split command into parts
            parts = user_input.split()

            # Send to server
            result = await client.execute(*parts)

            def format_output(val):
                if isinstance(val, bytes):
                    return val.decode()
                if isinstance(val, list):
                    return [v.decode() if isinstance(v, bytes) else v for v in val]
                return val
            print(format_output(result))

        except CommandError as e:
            print(f"(error) {e}")
        except Exception as e:
            print(f"(unexpected error) {e}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())