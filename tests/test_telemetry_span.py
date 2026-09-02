"""The span helper must also work when OTLP export is disabled."""

from __future__ import annotations

from opentelemetry import trace

from lab28_platform import telemetry


def test_span_accepts_a_non_recording_span(monkeypatch) -> None:
    context = trace.SpanContext(
        trace_id=1,
        span_id=1,
        is_remote=False,
        trace_flags=trace.TraceFlags.DEFAULT,
        trace_state=trace.TraceState(),
    )
    non_recording = trace.NonRecordingSpan(context)

    class Tracer:
        def start_as_current_span(self, *_args, **_kwargs):
            return trace.use_span(non_recording, end_on_exit=False)

    monkeypatch.setattr(telemetry, "get_tracer", lambda: Tracer())

    with telemetry.span("lab28.test") as active:
        assert active is non_recording
