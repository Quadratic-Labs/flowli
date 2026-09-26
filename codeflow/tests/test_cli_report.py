"""The controller's `--once` summaries, and that they stay plain text off a terminal."""

from __future__ import annotations

import re

import pytest
from flowli import cli_render as render

from flowli_codeflow.cli import board_report, merge_report

ANSI = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def colored(monkeypatch):
    monkeypatch.setattr(render, "use_color", lambda *_: True)


@pytest.fixture
def plain(monkeypatch):
    monkeypatch.setattr(render, "use_color", lambda *_: False)


@pytest.mark.parametrize("merged,expected", [(True, "merged=1"), (False, "merged=0")])
def test_merge_report(merged, expected, plain):
    assert merge_report(merged) == expected


@pytest.mark.parametrize("count,expected", [(0, "reconciled=0"), (3, "reconciled=3")])
def test_board_report(count, expected, plain):
    assert board_report(count) == expected


def test_colour_never_changes_the_text(colored):
    assert ANSI.sub("", merge_report(True)) == "merged=1"
    assert ANSI.sub("", board_report(3)) == "reconciled=3"


# --- the echo path -----------------------------------------------------------------


class FakeLoaded:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


async def test_merge_once_prints_its_report_and_closes(capsys, plain):
    from flowli_codeflow.cli import _serve

    loaded = FakeLoaded()

    async def run() -> bool:
        return True

    await _serve(run, loaded, once=True, report=merge_report)
    assert capsys.readouterr().out == "merged=1\n"
    assert loaded.closed


async def test_board_once_prints_its_report(capsys, plain):
    from flowli_codeflow.cli import _serve

    async def run() -> int:
        return 3

    await _serve(run, FakeLoaded(), once=True, report=board_report)
    assert capsys.readouterr().out == "reconciled=3\n"
