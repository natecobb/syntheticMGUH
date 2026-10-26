"""Read timestamped Synthea onset markers without exposing them as clinical prose."""

from datetime import datetime
import re

MARKER = re.compile(
    r"^<!-- synthea-transfer-v1 policy=(cardiac|liver) recognized_at=(\S+) code=(\d+) -->\s*$",
    re.MULTILINE,
)


def extract_signals(text: str) -> tuple[str, list[dict[str, str]]]:
    signals = []
    for match in MARKER.finditer(text):
        policy, timestamp, code = match.groups()
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("Synthea transfer recognition must include its timezone")
        signals.append(dict(policy=policy, recognized_at=timestamp, source_code=code))
    return MARKER.sub("", text).strip(), signals
