import json

from de_challenge.api.sse import format_sse_event


def test_format_sse_event():
    payload = {"status": "COMPLETED", "ingestion_id": "ing-1"}
    message = format_sse_event(event_type="status", data=payload)

    assert message.startswith("event: status\n")
    assert "data: " in message
    assert message.endswith("\n\n")
    data_line = [line for line in message.splitlines() if line.startswith("data: ")][0]
    decoded = json.loads(data_line.replace("data: ", ""))
    assert decoded == payload
