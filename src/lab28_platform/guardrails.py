"""Input limits and PII handling for the serving path.

The slide places a guardrails check between the model response and the client,
and lists "PII pipeline handling" under the security pillar. Two things happen
here: inbound text is bounded and screened before it reaches the model, and any
detected personal data is redacted before the text is logged, traced or stored.

Redaction is deliberately conservative and pattern-based. It is a teaching
implementation, not a compliance-grade classifier, and the runbook says so.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

MAX_QUESTION_CHARS = 1000
MAX_ANSWER_CHARS = 4000
MAX_SOURCES = 10


class PiiKind(StrEnum):
    EMAIL = "email"
    PHONE_VN = "phone_vn"
    ID_NUMBER = "id_number"
    CREDIT_CARD = "credit_card"


#: Ordered so that the most specific pattern wins. Vietnamese mobile numbers are
#: 10 digits starting 0[3|5|7|8|9]; the +84 form is normalised by the same rule.
_PATTERNS: tuple[tuple[PiiKind, re.Pattern[str]], ...] = (
    (PiiKind.EMAIL, re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")),
    (PiiKind.CREDIT_CARD, re.compile(r"\b(?:\d[ -]?){13,19}\b")),
    (PiiKind.PHONE_VN, re.compile(r"(?:\+84|0)(?:3|5|7|8|9)\d{8}\b")),
    (PiiKind.ID_NUMBER, re.compile(r"\b\d{12}\b")),
)

_PROMPT_INJECTION = re.compile(
    r"(ignore (all )?(previous|prior) instructions"
    r"|bỏ qua (mọi )?(hướng dẫn|chỉ dẫn) (trước|phía trên)"
    r"|system prompt"
    r"|reveal your (system )?prompt)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GuardrailResult:
    """Outcome of a guardrail pass over one piece of text."""

    text: str
    blocked: bool = False
    reason: str | None = None
    findings: tuple[PiiKind, ...] = field(default_factory=tuple)

    @property
    def redacted(self) -> bool:
        return bool(self.findings)


def redact(text: str) -> GuardrailResult:
    """Replace personal data with a stable placeholder token.

    The placeholder keeps the kind so a reviewer can see *what* was removed
    without seeing the value.
    """
    findings: list[PiiKind] = []
    redacted = text
    for kind, pattern in _PATTERNS:
        if pattern.search(redacted):
            findings.append(kind)
            redacted = pattern.sub(f"[redacted:{kind}]", redacted)
    return GuardrailResult(text=redacted, findings=tuple(findings))


def check_question(text: str) -> GuardrailResult:
    """Bound and screen an inbound question before it reaches retrieval.

    Blocking is reserved for input the platform should not process at all.
    Personal data is redacted rather than blocked, because a user quoting their
    own email address is a normal support question.
    """
    if len(text) > MAX_QUESTION_CHARS:
        return GuardrailResult(
            text=text[:MAX_QUESTION_CHARS],
            blocked=True,
            reason=f"question exceeds {MAX_QUESTION_CHARS} characters",
        )
    if _PROMPT_INJECTION.search(text):
        return GuardrailResult(
            text=text,
            blocked=True,
            reason="question contains a prompt-injection pattern",
        )
    return redact(text)


def check_answer(text: str) -> GuardrailResult:
    """Screen the model response before it leaves the platform."""
    trimmed = text[:MAX_ANSWER_CHARS]
    return redact(trimmed)


def safe_for_telemetry(text: str, *, limit: int = 120) -> str:
    """Produce a short, redacted excerpt that is safe to attach to a span.

    Raw user text never becomes a span attribute. Only this excerpt does, and
    the collector applies a second redaction pass on top of it.
    """
    return redact(text).text[:limit]
