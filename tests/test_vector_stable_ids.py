"""UT-vector-stable-ids — deterministic point identity for IP05.

Qdrant upserts by point ID. If re-indexing the same document produces a new ID,
the collection accumulates duplicates and retrieval starts returning the same
source several times, which quietly degrades every answer. The ID is therefore
derived from the document ID, never generated.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from lab28_platform.contracts import ID_NAMESPACE, stable_point_id
from lab28_platform.vector_store import IndexableDocument, documents_from_rows

pytestmark = pytest.mark.matrix("UT-vector-stable-ids")


class TestPointIdentity:
    def test_the_same_document_id_always_yields_the_same_point_id(self) -> None:
        assert stable_point_id("policy-1") == stable_point_id("policy-1")

    def test_different_document_ids_yield_different_point_ids(self) -> None:
        assert stable_point_id("policy-1") != stable_point_id("policy-2")

    def test_the_point_id_is_a_uuid_qdrant_accepts(self) -> None:
        """Qdrant takes an unsigned integer or a UUID, and nothing else."""
        assert UUID(stable_point_id("policy-1")).version == 5

    def test_the_id_is_pinned_to_the_lab_namespace(self) -> None:
        """A bare uuid5 of the doc id would collide with any other lab's points."""
        from uuid import uuid5

        assert stable_point_id("policy-1") == str(uuid5(ID_NAMESPACE, "policy-1"))

    def test_the_namespace_is_stable_across_processes(self) -> None:
        """Regenerating the namespace per run would re-key the whole collection."""
        assert str(ID_NAMESPACE) == "1410621c-8323-582d-be90-f363eb789019"

    @pytest.mark.parametrize(
        "doc_id",
        ["policy-1", "chính-sách", "doc/with/slashes", "a" * 128, "1"],
    )
    def test_any_document_id_maps_to_a_valid_point_id(self, doc_id: str) -> None:
        assert UUID(stable_point_id(doc_id))


class TestDocumentAdaptation:
    def test_delta_rows_become_indexable_documents(self) -> None:
        rows = [
            {
                "doc_id": "policy-1",
                "title": "Chính sách hoàn tiền",
                "text": "Hoàn tiền trong 14 ngày.",
                "locale": "vi",
                "tags": ["policy"],
            }
        ]

        assert documents_from_rows(rows) == [
            IndexableDocument(
                doc_id="policy-1",
                title="Chính sách hoàn tiền",
                text="Hoàn tiền trong 14 ngày.",
                locale="vi",
                tags=("policy",),
            )
        ]

    def test_a_row_without_tags_indexes_cleanly(self) -> None:
        """Delta writes NULL for an empty array; ``None`` must not become ``[None]``."""
        document = documents_from_rows([{"doc_id": "policy-1", "tags": None}])[0]

        assert document.tags == ()

    def test_the_same_row_indexed_twice_targets_one_point(self) -> None:
        row = {"doc_id": "policy-1", "title": "T", "text": "X"}
        first, second = documents_from_rows([row, row])

        assert stable_point_id(first.doc_id) == stable_point_id(second.doc_id)
