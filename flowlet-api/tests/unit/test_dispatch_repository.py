"""Unit tests for dispatch-key idempotent submission.

Covers the DispatchKeyRepository put-if-absent mapping (local backend) and
the controller-level submit_flow behaviour: first submission creates the
run, repeated submissions with the same key collapse onto it, and
dispatch_key without storage is rejected explicitly.
"""
import json
from uuid import uuid7

import pytest
from fastapi import HTTPException

from flowlet.api.controller import FlowController
from flowlet.api.models import FlowArguments
from cairndb.storage.filesystem import FilesystemStorage

from flowlet.repository.dispatch import DispatchKeyRepository


@pytest.fixture
def dispatch_repo(tmp_path):
    return DispatchKeyRepository(store=FilesystemStorage(tmp_path))


# ============================================================================
# DispatchKeyRepository — put-if-absent mapping
# ============================================================================


class TestResolveOrCreate:
    def test_first_claim_creates(self, dispatch_repo):
        run_id = uuid7()
        resolved, created = dispatch_repo.resolve_or_create("flow", "key-1", run_id)
        assert created is True
        assert resolved == run_id

    def test_second_resolve_returns_existing(self, dispatch_repo):
        first = uuid7()
        dispatch_repo.resolve_or_create("flow", "key-1", first)
        resolved, created = dispatch_repo.resolve_or_create("flow", "key-1", uuid7())
        assert created is False
        assert resolved == first

    def test_distinct_keys_are_independent(self, dispatch_repo):
        a, b = uuid7(), uuid7()
        assert dispatch_repo.resolve_or_create("flow", "key-a", a) == (a, True)
        assert dispatch_repo.resolve_or_create("flow", "key-b", b) == (b, True)

    def test_same_key_different_flows_are_independent(self, dispatch_repo):
        a, b = uuid7(), uuid7()
        assert dispatch_repo.resolve_or_create("flow_a", "key", a) == (a, True)
        assert dispatch_repo.resolve_or_create("flow_b", "key", b) == (b, True)

    def test_mapping_file_records_raw_key_for_audit(self, dispatch_repo, tmp_path):
        run_id = uuid7()
        dispatch_repo.resolve_or_create("flow", "webhook-42", run_id)
        digest = DispatchKeyRepository.digest("webhook-42")
        path = tmp_path / "dispatch" / "flow" / f"{digest}.json"
        data = json.loads(path.read_text())
        assert data["run_id"] == str(run_id)
        assert data["dispatch_key"] == "webhook-42"
        assert data["flow_name"] == "flow"

    def test_digest_is_path_safe_hex(self):
        digest = DispatchKeyRepository.digest("weird/key with spaces:éé")
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)


# ============================================================================
# Controller — idempotent submit_flow
# ============================================================================


class TestSubmitFlowWithDispatchKey:
    @pytest.fixture
    def controller(self, registry_with_flows, mock_queue, dispatch_repo):
        return FlowController(
            registry=registry_with_flows,
            queue=mock_queue,
            dispatch_repo=dispatch_repo,
        )

    def test_first_submission_is_not_deduplicated(self, controller):
        resp = controller.submit_flow(
            "my_flow", FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k1")
        )
        assert resp.deduplicated is False

    def test_duplicate_submission_resolves_same_run(self, controller):
        args = FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k1")
        first = controller.submit_flow("my_flow", args)
        second = controller.submit_flow("my_flow", args)
        assert second.deduplicated is True
        assert second.run_id == first.run_id
        assert second.job_id != first.job_id  # message ids stay unique

    def test_duplicate_still_enqueues_wakeup(self, controller, mock_queue):
        args = FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k1")
        controller.submit_flow("my_flow", args)
        controller.submit_flow("my_flow", args)
        assert mock_queue.enqueue.call_count == 2

    def test_different_keys_create_different_runs(self, controller):
        first = controller.submit_flow(
            "my_flow", FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k1")
        )
        second = controller.submit_flow(
            "my_flow", FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k2")
        )
        assert first.run_id != second.run_id

    def test_no_key_never_deduplicates(self, controller):
        args = FlowArguments(kwargs={"x": 1, "y": 2})
        first = controller.submit_flow("my_flow", args)
        second = controller.submit_flow("my_flow", args)
        assert first.run_id != second.run_id
        assert second.deduplicated is False

    def test_key_without_dispatch_repo_is_503(self, registry_with_flows, mock_queue):
        controller = FlowController(registry=registry_with_flows, queue=mock_queue)
        with pytest.raises(HTTPException) as exc:
            controller.submit_flow(
                "my_flow", FlowArguments(kwargs={"x": 1, "y": 2}, dispatch_key="k1")
            )
        assert exc.value.status_code == 503
