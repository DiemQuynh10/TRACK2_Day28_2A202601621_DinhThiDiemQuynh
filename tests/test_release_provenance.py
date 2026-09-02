"""UT-release-provenance — what a registered version must be able to answer (IP06).

An incident review asks one question of the registry: *which prompt, which
model, which data version produced this answer?* A version that cannot answer it
is a number, not a release. These tests pin the provenance fields; IT-J3 proves
the alias actually moves against a live MLflow.
"""

from __future__ import annotations

import dataclasses

import pytest

from lab28_platform.model_registry import (
    TAG_COLLECTION,
    TAG_DELTA_VERSION,
    TAG_EMBEDDING_MODEL,
    TAG_FEATURE_SERVICE,
    TAG_PROMPT_VERSION,
    TAG_VLLM_MODEL,
    Release,
    ReleaseSpec,
)

pytestmark = pytest.mark.matrix("UT-release-provenance")

TEMPLATE = "Ngữ cảnh:\n{context}\n\nCâu hỏi: {question}\nTrả lời:"


def spec(**overrides: object) -> ReleaseSpec:
    fields: dict[str, object] = {
        "prompt_version": "v1",
        "prompt_template": TEMPLATE,
        "vllm_model_id": "Qwen/Qwen3-1.7B",
        "embedding_model_id": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2@faf4aa4",
        "qdrant_collection": "lab28_documents",
        "feature_service": "asker_serving_v1",
        "top_k": 3,
        "delta_version": 7,
        "evaluation": {"groundedness": 0.81},
    }
    fields.update(overrides)
    return ReleaseSpec(**fields)  # type: ignore[arg-type]


class TestProvenanceTags:
    def test_every_provenance_tag_is_populated(self) -> None:
        tags = spec().as_tags()

        assert tags == {
            TAG_PROMPT_VERSION: "v1",
            TAG_VLLM_MODEL: "Qwen/Qwen3-1.7B",
            TAG_EMBEDDING_MODEL: (
                "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2@faf4aa4"
            ),
            TAG_DELTA_VERSION: "7",
            TAG_COLLECTION: "lab28_documents",
            TAG_FEATURE_SERVICE: "asker_serving_v1",
        }

    def test_tags_are_namespaced_so_they_survive_a_shared_registry(self) -> None:
        assert all(key.startswith("lab28.") for key in spec().as_tags())

    def test_tag_values_are_strings_because_mlflow_stores_strings(self) -> None:
        assert all(isinstance(value, str) for value in spec().as_tags().values())

    def test_the_embedding_model_is_pinned_by_revision(self) -> None:
        """The same text must embed identically, or retrieval silently drifts."""
        assert "@" in spec().as_tags()[TAG_EMBEDDING_MODEL]


class TestReleaseParameters:
    def test_retrieval_configuration_is_logged_as_parameters(self) -> None:
        params = spec().as_params()

        assert params["top_k"] == "3"
        assert params["qdrant_collection"] == "lab28_documents"
        assert params["feature_service"] == "asker_serving_v1"

    def test_a_release_without_a_data_version_is_still_representable(self) -> None:
        """A prompt-only change has no new Delta version; it must not crash."""
        params = spec(delta_version=None).as_params()

        assert params["delta_version"] == "None"

    def test_the_spec_is_immutable(self) -> None:
        """A release definition that mutates after logging is unreproducible."""
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec().prompt_version = "v2"  # type: ignore[misc]


class TestResolvedRelease:
    def test_the_serving_evidence_fields_round_trip(self) -> None:
        release = Release(
            name="lab28-rag-release",
            version="4",
            run_id="abc123",
            alias="champion",
            prompt_version="v1",
            prompt_template=TEMPLATE,
            vllm_model_id="Qwen/Qwen3-1.7B",
            embedding_model_id="paraphrase@rev",
            delta_version=7,
            top_k=3,
        )

        payload = release.to_dict()

        assert payload["version"] == "4"
        assert payload["run_id"] == "abc123"
        assert payload["delta_version"] == 7

    def test_the_prompt_template_is_not_serialised_into_the_evidence(self) -> None:
        """Evidence is pasted into reports; a full prompt there is noise and leakage."""
        release = Release(
            name="lab28-rag-release",
            version="4",
            run_id="abc123",
            alias="champion",
            prompt_version="v1",
            prompt_template=TEMPLATE,
            vllm_model_id="Qwen/Qwen3-1.7B",
            embedding_model_id="paraphrase@rev",
            delta_version=7,
            top_k=3,
        )

        assert "prompt_template" not in release.to_dict()
        assert release.to_dict()["prompt_version"] == "v1"


def test_the_evaluation_metrics_are_optional_but_typed() -> None:
    assert spec(evaluation={}).evaluation == {}
    assert spec().evaluation["groundedness"] == pytest.approx(0.81)
