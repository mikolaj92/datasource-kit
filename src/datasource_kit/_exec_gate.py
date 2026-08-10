"""Private ACK-gated exec wrapper for fail-closed worker launch."""
from __future__ import annotations

import json
import os


def main() -> int:
    fd = int(os.environ.pop("DATASOURCE_KIT_ACK_FD"))
    command = json.loads(os.environ.pop("DATASOURCE_KIT_EXEC_COMMAND"))
    # This process is already a session leader (Popen(start_new_session=True)).
    os.write(fd, b"READY\n")
    ack = b""
    while not ack.endswith(b"\n"):
        chunk = os.read(fd, 16)
        if not chunk:
            return 0  # parent died before durable provenance
        ack += chunk
    os.close(fd)
    if ack != b"ACK\n":
        return 0
    os.execvpe(command[0], command, os.environ)
    return 127

if __name__ == "__main__":
    raise SystemExit(main())
