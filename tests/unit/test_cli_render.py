"""CLI styling: colour on a terminal, and the very same bytes everywhere else.

The `status` and `--journal` layouts are a public interface: operators grep them and
`tests/integration/test_cli.py` pins their columns. These tests hold the line that
adding colour never moves a column or changes a character.
"""

from __future__ import annotations

import re

import pytest
from typer import colors

from flowli import cli_render as render

ANSI = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def colored(monkeypatch):
    """Force the styled path on, whatever the test runner's stdout is."""
    monkeypatch.setattr(render, "use_color", lambda *_: True)


@pytest.fixture
def plain(monkeypatch):
    monkeypatch.setattr(render, "use_color", lambda *_: False)


# --- the layout contract ---------------------------------------------------------------


CASES = [
    (render.field, ("queue", "default")),
    (render.line, ("status", "completed")),
    (render.status_value, ("completed",)),
    (render.workflow_value, ("greet", "1")),
    (render.actor_value, ("human", "thomas@example.com")),
    (render.event_value, ("execution.failed",)),
    (render.journal_header, ()),
    (
        render.journal_row,
        (7, "frame.started", "root/greet#0", "2026-09-11T00:00:00Z", "worker", "w-1"),
    ),
    (render.counters, ([("archived", 1), ("cleaned", 0)],)),
    (render.json_block, ({"value": "hi", "n": 2},)),
]


@pytest.mark.parametrize("fn,args", CASES, ids=lambda v: getattr(v, "__name__", ""))
def test_styling_never_changes_the_text(fn, args, monkeypatch):
    """Stripping the ANSI from the styled form gives the plain form, byte for byte."""
    monkeypatch.setattr(render, "use_color", lambda *_: True)
    styled = fn(*args)
    monkeypatch.setattr(render, "use_color", lambda *_: False)
    plain_text = fn(*args)
    assert ANSI.sub("", styled) == plain_text


@pytest.mark.parametrize("fn,args", CASES, ids=lambda v: getattr(v, "__name__", ""))
def test_plain_output_carries_no_escapes(fn, args, plain):
    assert "\x1b" not in fn(*args)


def test_status_labels_keep_their_column(plain):
    """Every label pads to the width the integration tests pin."""
    for label in ("eid", "status", "workflow", "queue", "created", "parent", "key", "last"):
        assert render.field(label, "x") == f"{label:<11}x"


def test_field_defaults_to_no_bold(colored):
    """`bold` defaults to False: a bare call styles only the (dim) label, not the value."""
    assert render.field("label", "value") == "\x1b[2mlabel      \x1b[0mvalue"


def test_field_honors_fg_and_bold_when_given(colored):
    out = render.field("label", "value", colors.CYAN, bold=True)
    assert out == "\x1b[2mlabel      \x1b[0m\x1b[36m\x1b[1mvalue\x1b[0m"


def test_journal_row_keeps_its_columns(plain):
    row = render.journal_row(7, "frame.started", "root/greet#0", "2026-09-11T00:00:00Z", "w", "1")
    expected = (
        f"{7:>14}  {'frame.started':<22} {'root/greet#0':<32} {'2026-09-11T00:00:00Z':<24} w:1"
    )
    assert row == expected


def test_journal_header_labels_and_widths(plain):
    """The header's labels and column widths must match `journal_row`'s exactly."""
    assert render.journal_header() == (f"{'seq':>14}  {'type':<22} {'fid':<32} {'at':<24} actor")


# --- colour choices ---------------------------------------------------------------------


def test_paint_with_no_style_requested_is_a_no_op_even_when_colored(colored):
    """`fg`, `bold` and `dim` all falsy means nothing to style -- text comes back bare."""
    assert render.paint("hi") == "hi"


def test_paint_defaults_bold_and_dim_to_false(colored):
    """A bare `fg` styles only the colour: no bold, no dim, unless asked for."""
    out = render.paint("hi", colors.GREEN)
    assert "\x1b[32m" in out
    assert "\x1b[1m" not in out
    assert "\x1b[2m" not in out


@pytest.mark.parametrize(
    "state,color",
    [("completed", "32"), ("failed", "31"), ("suspended", "33"), ("running", "34")],
)
def test_status_colors_track_meaning(state, color, colored):
    out = render.status_value(state)
    assert f"\x1b[{color}m" in out
    assert "\x1b[1m" in out  # status values are always bold


def test_archived_status_falls_back_without_crashing(colored):
    """`archived (2026-...)` is not an ExecutionStatus; it must still render."""
    out = render.status_value("archived (2026-09-11T00:00:00Z)")
    assert ANSI.sub("", out) == "archived (2026-09-11T00:00:00Z)"


def test_status_value_falls_back_to_cyan_for_an_unrecognized_state(colored):
    """A state with no entry in `STATUS_COLORS` still gets a colour, not none at all."""
    out = render.status_value("archived (2026-09-11T00:00:00Z)")
    assert "\x1b[36m" in out  # cyan, the documented fallback
    assert "\x1b[1m" in out


def test_status_value_colors_by_the_word_before_any_trailing_detail(colored):
    """The colour lookup keys on the text before the first space, not the whole state."""
    assert "\x1b[34m" in render.status_value("running now")


def test_workflow_value_pins_the_exact_styling(colored):
    """The name is cyan *and* bold; the version is dim -- both must hold together."""
    assert render.workflow_value("greet", "1") == ("\x1b[36m\x1b[1mgreet\x1b[0m \x1b[2mv1\x1b[0m")


def test_counters_dim_the_zeroes_and_bold_the_rest(colored):
    out = render.counters([("archived", 1), ("cleaned", 0)])
    assert ANSI.sub("", out) == "archived=1 cleaned=0"
    assert "\x1b[1m" in out  # the non-zero count is bold


def test_counters_pins_the_exact_styling_for_zero_and_nonzero(colored):
    """Zero is dim only; a nonzero count is green *and* bold; the name is always dim."""
    out = render.counters([("archived", 1), ("cleaned", 0)])
    assert out == (
        "\x1b[2marchived\x1b[0m=\x1b[32m\x1b[1m1\x1b[0m \x1b[2mcleaned\x1b[0m=\x1b[2m0\x1b[0m"
    )


def test_journal_header_is_dim_and_bold(colored):
    """The header is styled with both dim and bold, unlike a plain field label."""
    out = render.journal_header()
    assert "\x1b[1m" in out  # bold
    assert "\x1b[2m" in out  # dim


def test_line_label_is_dim_not_bold(colored):
    """`line`'s label uses `paint(..., dim=True)`: dim only, never bold."""
    out = render.line("status", "value")
    assert "\x1b[2m" in out
    assert "\x1b[1m" not in out


@pytest.mark.parametrize(
    "event_type,color",
    [
        ("execution.completed", "32"),
        ("execution.failed", "31"),
        ("execution.cancelled", "35"),
        ("execution.suspended", "33"),
        ("execution.started", "34"),
    ],
)
def test_event_colors_track_meaning(event_type, color, colored):
    out = render.event_value(event_type)
    assert f"\x1b[{color}m" in out
    assert "\x1b[1m" in out  # event types are always bold


def test_event_value_colors_frame_events_cyan(colored):
    out = render.event_value("frame.started")
    assert "\x1b[36m" in out
    assert "\x1b[1m" in out


def test_frame_color_is_cyan_only_for_frame_dot_events():
    """`_frame_color` keys strictly on the `frame.` prefix -- nothing else qualifies."""
    assert render._frame_color("frame.started") == colors.CYAN
    assert render._frame_color("something.else") is None


def test_event_value_has_no_color_for_an_unknown_event(colored):
    """An event that is neither in EVENT_COLORS nor a `frame.*` gets bold, no colour."""
    out = render.event_value("something.else")
    assert ANSI.sub("", out) == "something.else"
    assert "\x1b[1m" in out
    assert not any(f"\x1b[{c}m" in out for c in ("30", "31", "32", "33", "34", "35", "36", "37"))


def test_journal_row_pins_the_exact_styling_for_a_frame_event(colored):
    out = render.journal_row(7, "frame.started", "fid", "2026-01-01T00:00:00Z", "k", "i")
    assert out == (
        f"\x1b[2m{7:>14}\x1b[0m"
        "  "
        f"\x1b[36m{'frame.started':<22}\x1b[0m"
        " "
        f"{'fid':<32}"
        " "
        f"\x1b[2m{'2026-01-01T00:00:00Z':<24}\x1b[0m"
        " "
        "\x1b[2mk:\x1b[0mi"
    )


def test_journal_row_pins_the_exact_styling_for_a_known_event(colored):
    out = render.journal_row(
        3, "execution.completed", "root/greet#0", "2026-09-11T00:00:00Z", "human", "alice"
    )
    assert out == (
        f"\x1b[2m{3:>14}\x1b[0m"
        "  "
        f"\x1b[32m{'execution.completed':<22}\x1b[0m"
        " "
        f"{'root/greet#0':<32}"
        " "
        f"\x1b[2m{'2026-09-11T00:00:00Z':<24}\x1b[0m"
        " "
        "\x1b[2mhuman:\x1b[0malice"
    )


# --- payloads ---------------------------------------------------------------------------


def test_json_block_is_sorted_plain_json(plain):
    assert render.json_block({"b": 1, "a": 2}) == '{\n  "a": 2,\n  "b": 1\n}'


def test_json_block_highlights_on_a_terminal(colored):
    out = render.json_block({"amount": 7})
    assert "\x1b[" in out
    assert ANSI.sub("", out) == '{\n  "amount": 7\n}'


def test_json_block_survives_values_json_cannot_hold(plain):
    """`default=str` keeps the CLI from dying on a payload holding an odd object."""
    assert render.json_block({"when": object()}).startswith('{\n  "when": "<object object')


def test_json_block_pins_the_exact_ansi_dark_encoding(colored):
    """Locks in the lexer, theme and background `json_block` renders with.

    A wrong lexer name drops all token colour, and a theme other than `ansi_dark`
    switches from the terminal's own sixteen colours to a fixed 256-colour palette
    (breaking the "suits light and dark themes alike" promise) -- both change these
    exact bytes even though the visible characters never move.
    """
    out = render.json_block({"n": 7, "s": "hi"})
    assert out == (
        "\x1b[49m{\x1b[0m\n"
        '\x1b[90;49m  \x1b[0m\x1b[94;49m"n"\x1b[0m\x1b[49m:\x1b[0m\x1b[90;49m \x1b[0m\x1b[94;49m7\x1b[0m\x1b[49m,\x1b[0m\n'  # noqa: E501
        '\x1b[90;49m  \x1b[0m\x1b[94;49m"s"\x1b[0m\x1b[49m:\x1b[0m\x1b[90;49m \x1b[0m\x1b[33;49m"hi"\x1b[0m\n'  # noqa: E501
        "\x1b[49m}\x1b[0m"
    )


def test_json_block_never_truncates_a_long_line(colored):
    """A single value long enough to exceed the fixed render width must still show whole.

    `Console(..., width=10_000)` only matters if `soft_wrap` were off; `soft_wrap=True`
    is what actually guarantees a giant line is never hard-wrapped or cropped.
    """
    value = {"blob": "x" * 12_000}
    out = render.json_block(value)
    assert ANSI.sub("", out) == '{\n  "blob": "' + "x" * 12_000 + '"\n}'


# --- colour gating -----------------------------------------------------------------------


def test_no_color_env_disables_styling(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr("sys.stdout", type("T", (), {"isatty": lambda self: True})())
    assert render.use_color() is False


def test_color_needs_a_terminal(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr("sys.stdout", type("T", (), {"isatty": lambda self: False})())
    assert render.use_color() is False
    monkeypatch.setattr("sys.stdout", type("T", (), {"isatty": lambda self: True})())
    assert render.use_color() is True


def test_use_color_false_for_a_stream_with_no_isatty_method(monkeypatch):
    """A stream lacking `isatty` entirely (the `getattr` default) must not crash."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    assert render.use_color(object()) is False


# --- errors and warnings ------------------------------------------------------------------


def test_prefixed_echoes_word_colon_message_to_stderr(monkeypatch):
    calls = []
    monkeypatch.setattr(render.typer, "echo", lambda msg, err=False: calls.append((msg, err)))
    monkeypatch.setattr(render, "use_color", lambda *_: False)
    render.error("boom")
    assert calls == [("error: boom", True)]


def test_error_colors_the_word_red_on_a_terminal(monkeypatch):
    calls = []
    monkeypatch.setattr(render.typer, "echo", lambda msg, err=False: calls.append((msg, err)))
    monkeypatch.setattr(render, "use_color", lambda *_: True)
    render.error("boom")
    msg, err = calls[0]
    assert err is True
    assert ANSI.sub("", msg) == "error: boom"
    assert "\x1b[31m" in msg  # red
    assert "\x1b[1m" in msg  # bold


def test_prefixed_colors_the_word_on_a_terminal(monkeypatch):
    calls = []
    monkeypatch.setattr(render.typer, "echo", lambda msg, err=False: calls.append((msg, err)))
    monkeypatch.setattr(render, "use_color", lambda *_: True)
    render.warning("careful")
    msg, err = calls[0]
    assert err is True
    assert ANSI.sub("", msg) == "warning: careful"
    assert "\x1b[33m" in msg  # yellow
    assert "\x1b[1m" in msg  # bold


def test_prefixed_checks_color_for_stderr_not_stdout(monkeypatch):
    """`_prefixed` must gate on `sys.stderr`, since that is where it writes."""
    calls = []
    monkeypatch.setattr(render.typer, "echo", lambda msg, err=False: calls.append((msg, err)))
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr("sys.stdout", type("T", (), {"isatty": lambda self: True})())
    monkeypatch.setattr("sys.stderr", type("T", (), {"isatty": lambda self: False})())
    render.error("boom")
    assert calls == [("error: boom", True)]
