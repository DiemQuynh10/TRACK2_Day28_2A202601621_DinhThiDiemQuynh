# Bundled corpus

Two JSONL files, both authored for this lab, both free of personal data, and
both released with the repository under CC0-1.0. Each line is a request body for
the matching ingestion endpoint, so `lab28 seed` posts them verbatim — there is
no adapter between the file and the contract, which means a corpus that drifts
away from the contract fails loudly instead of silently.

## `documents.jsonl` — 13 documents

The retrieval corpus. Each line validates against `DocumentSubmission` and goes
to `POST /api/v1/documents`.

The corpus is the platform documenting itself: one document per integration
point (IP01–IP10), plus the health semantics, the latency budgets, and the
degradation policy. That is deliberate. Retrieval can only be judged when you
already know the right answer, and a question like *"which port does Prometheus
scrape Feast on?"* has exactly one correct grounded answer that a reader of this
repository can check without trusting the model.

## `feedback.jsonl` — 12 records, 6 askers

Each line validates against `FeedbackSubmission` and goes to
`POST /api/v1/feedback`.

Askers repeat on purpose: `asker_id` is the Feast entity key, so several records
per asker are what make the online feature values (recent question count,
average rating, freshness) non-trivial. Ratings span 1–5 and two records are in
English, so `locale` is exercised rather than being constant.

## Deliberate omissions

No `idempotency_key` is set on any line. The API derives one from the content
hash when a client omits it, and seeding twice is the cheapest way to prove that
replay collapses to the same rows in Delta, the same entity updates in Feast and
the same point count in Qdrant.

The corpus is sized for integration exercises, not for measuring model quality.
Thirteen documents cannot support a retrieval benchmark, and any accuracy number
computed from twelve feedback records would be noise.
