import asyncio
import grpc

from cranelock.controller import CentralController
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys

DIRECTORY = "run/controller-demo"

async def main():
    config = Config.load(f"{DIRECTORY}/config.json")
    keys = load_keys(f"{DIRECTORY}/private-keys.json")
    channel = grpc.insecure_channel("127.0.0.1:50060")
    controller = CentralController(config, keys[config.controller_id], channel)

    try:
        # Start with CENTRAL authority, or restore it after agents have fallen back.
        state = await asyncio.to_thread(controller.restore_central)
        print(f"Controller active: epoch={state['epoch']} revision={state['revision']} "
              f"holder={state['holder']} mode={state['mode']}")

        port = await controller.start_heartbeat_server("127.0.0.1:50052")
        print(f"Heartbeat endpoint listening on 127.0.0.1:{port}")
        await asyncio.Event().wait()
    finally:
        await controller.stop_heartbeat_server()
        channel.close()

asyncio.run(main())