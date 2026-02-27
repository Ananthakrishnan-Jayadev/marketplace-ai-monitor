import logging
import os
import re


REDACTION_TOKEN = "[REDACTED]"


def redact_text(text: object) -> str:
    raw = str(text)
    redacted = raw

    secret_values = [
        os.environ.get("OLLAMA_API_KEY", "").strip(),
        os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
        os.environ.get("DASHBOARD_ACCESS_TOKEN", "").strip(),
    ]
    for value in secret_values:
        if value:
            redacted = redacted.replace(value, REDACTION_TOKEN)

    # Basic bearer/token pattern redaction for unexpected payloads.
    redacted = re.sub(r"(?i)\b(bearer\s+)[a-z0-9._\-]{16,}\b", r"\1" + REDACTION_TOKEN, redacted)
    redacted = re.sub(r"(?i)\b(token\s*[:=]\s*)[a-z0-9._\-]{16,}\b", r"\1" + REDACTION_TOKEN, redacted)
    return redacted


class SecretRedactionFilter(logging.Filter):
    """Log filter that redacts known secrets from log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_text(record.msg)
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(_redact_arg(arg) for arg in record.args)
            elif isinstance(record.args, dict):
                record.args = {k: _redact_arg(v) for k, v in record.args.items()}
            else:
                record.args = _redact_arg(record.args)
        return True


def _redact_arg(value: object) -> object:
    """Preserve non-string logging args so %-format specifiers keep working."""
    if isinstance(value, str):
        return redact_text(value)
    return value
