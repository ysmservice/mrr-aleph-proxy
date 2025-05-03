import asyncio
import datetime
import json

LOCAL_HOST = '0.0.0.0'
LOCAL_PORT = 3366

REMOTE_HOST = 'jp-01.miningrigrentals.com'
REMOTE_PORT = 3366
DUMMY_PASSWORD = "x"

def log(msg):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

async def forward(reader, writer, direction, client_addr, rewrite=False):
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break

            text = data.decode(errors="replace").strip()
            if rewrite and direction == "client ➜ server":
                lines = text.splitlines()
                rewritten = []
                for line in lines:
                    try:
                        msg = json.loads(line)

                        # subscribeをスキップ（送らない）
                        if msg.get("method") == "mining.subscribe":
                            log(f"[{client_addr}] 🚫 skipping subscribe")
                            continue

                        # authorizeのparamsにパスワード追加
                        if msg.get("method") == "mining.authorize":
                            log(f"[{client_addr}] ✏️ rewriting authorize")
                            if len(msg["params"]) == 1:
                                msg["params"].append(DUMMY_PASSWORD)

                        line = json.dumps(msg)
                    except Exception:
                        pass
                    rewritten.append(line)

                new_data = ("\n".join(rewritten) + "\n").encode()
                writer.write(new_data)
                await writer.drain()
                log(f"[{client_addr}] {direction} {new_data.decode(errors='replace').strip()}")
                continue

            writer.write(data)
            await writer.drain()
            log(f"[{client_addr}] {direction} {text}")
    except Exception as e:
        log(f"[{client_addr}] ⚠️ Error in {direction}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def handle_connection(local_reader, local_writer):
    client_addr = local_writer.get_extra_info('peername')
    log(f"🔌 New connection from {client_addr}")

    try:
        remote_reader, remote_writer = await asyncio.open_connection(REMOTE_HOST, REMOTE_PORT)
        log(f"🌐 Connected to remote {REMOTE_HOST}:{REMOTE_PORT} for {client_addr}")

        await asyncio.gather(
            forward(local_reader, remote_writer, "client ➜ server", client_addr, rewrite=True),
            forward(remote_reader, local_writer, "server ➜ client", client_addr)
        )

    except Exception as e:
        log(f"[{client_addr}] ❌ Proxy connection error: {e}")
    finally:
        local_writer.close()
        await local_writer.wait_closed()
        log(f"[{client_addr}] ❎ Connection closed")

async def main():
    server = await asyncio.start_server(handle_connection, LOCAL_HOST, LOCAL_PORT)
    addr = server.sockets[0].getsockname()
    log(f"🚀 IceRiver→Alephium proxy (subscribe skipped) listening on {addr}")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
