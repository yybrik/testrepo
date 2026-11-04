"""Async boundary around Max's existing signed client, not another gate protocol.

The supplied client is synchronous. Only its bounded I/O runs in the default
executor; all agent state remains on the asyncio event loop.
"""
import asyncio

import grpc

from cranelock_gate.client import Client, command
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys


class GateClient:
    def __init__(self, channel, config, keys, agent_id, timeout=3):
        if agent_id not in config.members or agent_id not in keys:
            raise ValueError("Agent needs a configured gate member identity and its signing key")
        key = keys[agent_id]
        if key.public_key().public_bytes_raw().hex() != config.members[agent_id]:
            raise ValueError("Signing key does not match gate membership")
        self.channel = channel
        self.agent_id = agent_id
        self.client = Client(channel, config, {agent_id: key}, timeout)

    @classmethod
    def connect(cls, address, config_file, keys_file, agent_id, timeout=3, credentials=None):
        config = Config.load(config_file)
        keys = load_keys(keys_file)
        channel = (grpc.secure_channel(address, credentials) if credentials
                   else grpc.insecure_channel(address))
        try:
            return cls(channel, config, keys, agent_id, timeout)
        except Exception:
            channel.close()
            raise

    def transfer_command(self, epoch, revision, destination, request_id):
        return command(self.client.config, self.agent_id, "TransferHolder",
                       request_id=request_id, epoch=epoch, revision=revision,
                       destination=destination)

    async def state(self):
        return await asyncio.to_thread(self.client.state)

    async def transfer(self, payload):
        # Reusing an unchanged payload preserves the gate's durable retry contract.
        return await asyncio.to_thread(self.client.send, payload)

    async def install_recovery(self, certificate):
        payload = certificate["payload"]
        return await asyncio.to_thread(self.client.call, self.agent_id, "InstallAuthority",
            certificate=certificate, expected_epoch=payload["expected_epoch"],
            expected_revision=payload["expected_revision"])

    async def close(self):
        await asyncio.to_thread(self.channel.close)
