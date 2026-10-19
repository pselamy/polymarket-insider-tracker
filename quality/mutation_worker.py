"""Serve the pinned native Cosmic handler on a race-free private loopback socket."""

import asyncio
import importlib
import json
import socket
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

from aiohttp import web

from quality.report_data import record, text
from quality.trial_launcher import prepare_launcher, require_launcher

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


def native_handler() -> Handler:
    """The pinned SDK handler owns mutation, trial deadlines and response semantics."""
    module = importlib.import_module("cosmic_ray.distribution.http")
    return cast(Handler, module.handle_mutate_and_test)


async def serve(root: Path, nonce: str) -> None:
    """Write readiness only after listening; stop only on the private nonce marker."""
    app = web.Application()
    app.add_routes([web.post("/", journal_handler(root, native_handler()))])
    runner = web.AppRunner(app)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        try:
            await runner.setup()
            listener.bind(("127.0.0.1", 0))
            await web.SockSite(runner, listener).start()
            output = root / ".quality-results"
            output.mkdir(exist_ok=True)
            ready = {
                "nonce": nonce,
                "url": f"http://127.0.0.1:{listener.getsockname()[1]}",
            }
            temporary = output / "worker-ready.tmp"
            temporary.write_text(json.dumps(ready), encoding="utf-8")
            temporary.replace(output / "worker-ready.json")
            await asyncio.to_thread(wait_stop, output / "worker-stop.json", nonce)
        finally:
            await runner.cleanup()


def journal_handler(root: Path, handler: Handler) -> Handler:
    """Retain native requests after completion without changing SDK responses."""

    expected = prepare_launcher(root)

    async def handle(request: web.Request) -> web.StreamResponse:
        require_launcher(root, expected)
        response = await handler(request)
        require_launcher(root, expected)
        value: object = await request.json()
        with (root / ".quality-results" / "worker-jobs.jsonl").open(
            "a", encoding="utf-8"
        ) as stream:
            stream.write(json.dumps({"request": value, "status": response.status}) + "\n")
        return response

    return handle


def wait_stop(path: Path, nonce: str) -> None:
    """Wrong or malformed shutdown markers fail instead of silently stopping."""
    while not path.exists():
        time.sleep(0.05)
    value: object = json.loads(path.read_text(encoding="utf-8"))
    if text(record(value)["nonce"]) != nonce:
        raise ValueError("worker shutdown nonce differs")


def worker(root: Path, nonce: str) -> None:
    """Run an isolated worker with a new event loop on every supported runtime."""
    asyncio.run(serve(root, nonce))
