"""The runner's `--once` summary, and that it stays plain text off a terminal."""

from __future__ import annotations

import re

import pytest
from flowlet import cli_render as render

from flowlet_runner.cli import once_report

ANSI = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def colored(monkeypatch):
    monkeypatch.setattr(render, "use_color", lambda *_: True)


@pytest.fixture
def plain(monkeypatch):
    monkeypatch.setattr(render, "use_color", lambda *_: False)


def test_idle_pass_reports_zeroes(plain):
    assert once_report([], False) == "recovered=0 processed=0"


def test_a_pass_that_did_work(plain):
    assert once_report(["t-1", "t-2"], True) == "recovered=2 processed=1"


def test_colour_never_changes_the_text(colored):
    assert ANSI.sub("", once_report(["t-1"], True)) == "recovered=1 processed=1"


# --- the echo path -----------------------------------------------------------------


class FakeConsumer:
    def __init__(self, recovered: list[str], processed: bool) -> None:
        self._recovered, self._processed = recovered, processed

    async def recover(self) -> list[str]:
        return self._recovered

    async def run_once(self) -> bool:
        return self._processed


class FakeLoaded:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


async def test_once_pass_prints_its_report_and_closes(capsys, plain):
    from flowlet_runner.cli import _serve

    loaded = FakeLoaded()
    await _serve(FakeConsumer(["t-1", "t-2"], True), loaded, once=True)
    out = capsys.readouterr().out
    assert out.splitlines() == ["recovered=2 processed=1", "  attached t-1", "  attached t-2"]
    assert loaded.closed


async def test_idle_pass_lists_nothing(capsys, plain):
    from flowlet_runner.cli import _serve

    await _serve(FakeConsumer([], False), FakeLoaded(), once=True)
    assert capsys.readouterr().out == "recovered=0 processed=0\n"
