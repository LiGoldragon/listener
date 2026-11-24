"""Translator effect for one stopped Ogg Opus recording."""

import argparse
import asyncio
import base64
from contextlib import asynccontextmanager
import json
import sys
from urllib.parse import parse_qs, urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException


class DirectConnection(connect):
    """Keep the Authorization header at the configured service address."""

    def process_redirect(self, exception):
        return exception


class TranscriptionFailure(Exception):
    """A safe diagnostic, independent of provider data and credentials."""


@asynccontextmanager
async def capture_output(*arguments):
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.communicate()


async def credential(program, entry):
    async with capture_output(program, entry) as process:
        result, _ = await process.communicate()
        key = result.decode("ascii").rstrip("\r\n")
        if process.returncode or not key or any(not 33 <= ord(c) <= 126 for c in key):
            raise TranscriptionFailure("The configured credential could not be read.")
        return key


async def send(connection, event):
    await connection.send(json.dumps(event))


async def events(connection):
    async for message in connection:
        event = json.loads(message)
        if not isinstance(event, dict) or not isinstance(event.get("type"), str):
            raise TranscriptionFailure("The service sent an invalid event.")
        if event["type"] == "error":
            raise TranscriptionFailure("The service rejected the transcription turn.")
        yield event
    raise TranscriptionFailure("The service closed before completing the transcription.")


async def session_ready(connection, model):
    await send(connection, {
        "type": "session.update",
        "session": {
            "type": "transcription",
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "transcription": {"model": model},
                    "turn_detection": None,
                },
            },
        },
    })
    async for event in events(connection):
        if event["type"] == "session.updated":
            session = event.get("session")
            if not isinstance(session, dict) or session.get("type") != "transcription":
                raise TranscriptionFailure("The service did not open a transcription session.")
            return


async def append_recording(connection, decoder, audio):
    async with capture_output(
        decoder, "-nostdin", "-v", "error", "-xerror", "-f", "ogg", "-c:a", "opus",
        "-i", audio, "-map", "0:a:0", "-ac", "1", "-ar", "24000", "-f", "s16le",
        "-c:a", "pcm_s16le", "pipe:1",
    ) as process:
        sent_audio = False
        # 100 ms of mono signed 16-bit audio at the service's required rate.
        while True:
            try:
                chunk = await process.stdout.readexactly(4800)
            except asyncio.IncompleteReadError as end:
                chunk = end.partial
            if not chunk:
                break
            if len(chunk) % 2:
                raise TranscriptionFailure("The decoder returned a partial audio sample.")
            await send(connection, {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(chunk).decode("ascii"),
            })
            sent_audio = True
        if await process.wait() or not sent_audio:
            raise TranscriptionFailure("The stopped recording could not be decoded.")


async def final_transcript(connection):
    await send(connection, {"type": "input_audio_buffer.commit"})
    item_id = None
    async for event in events(connection):
        match event["type"]:
            case "input_audio_buffer.committed":
                new_id = event.get("item_id")
                if item_id is not None or not isinstance(new_id, str) or not new_id:
                    raise TranscriptionFailure("The service did not acknowledge exactly one turn.")
                item_id = new_id
            case "conversation.item.input_audio_transcription.failed" if item_id is not None:
                if event.get("item_id") == item_id:
                    raise TranscriptionFailure("The service could not transcribe the recording.")
            case "conversation.item.input_audio_transcription.completed" if item_id is not None:
                if event.get("item_id") == item_id and event.get("content_index") == 0:
                    text = event.get("transcript")
                    if not isinstance(text, str):
                        raise TranscriptionFailure("The service sent an invalid transcript.")
                    return {"item_id": item_id, "transcript": text}


async def transcribe(arguments):
    address = urlsplit(arguments.endpoint)
    if (address.scheme != "wss" or not address.hostname or address.username is not None
            or address.password is not None or address.fragment
            or parse_qs(address.query, keep_blank_values=True) != {"intent": ["transcription"]}):
        raise TranscriptionFailure("Configure a TLS WebSocket address with transcription intent.")
    if arguments.timeout <= 0 or not arguments.model:
        raise TranscriptionFailure("Configure a model and a positive timeout.")
    async with asyncio.timeout(arguments.timeout):
        key = await credential(arguments.secret_program, arguments.secret_entry)
        async with DirectConnection(
            arguments.endpoint, additional_headers={"Authorization": f"Bearer {key}"},
            open_timeout=None, proxy=None,
        ) as connection:
            await session_ready(connection, arguments.model)
            await append_recording(connection, arguments.decoder, arguments.audio)
            return await final_transcript(connection)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--secret-program", required=True)
    parser.add_argument("--secret-entry", required=True)
    parser.add_argument("--timeout", type=int, required=True)
    parser.add_argument("--decoder", required=True)
    parser.add_argument("--audio", required=True)
    arguments = parser.parse_args()
    try:
        result = asyncio.run(transcribe(arguments))
    except TranscriptionFailure as failure:
        print(failure, file=sys.stderr)
    except TimeoutError:
        print("The transcription turn timed out.", file=sys.stderr)
    except (OSError, WebSocketException, ValueError):
        print("The transcription transport or local program failed.", file=sys.stderr)
    else:
        print(json.dumps(result))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
