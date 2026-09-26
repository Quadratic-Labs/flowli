"""Terminal styling for the CLI: colour on a terminal, plain text everywhere else.

Every helper returns a string for `typer.echo`, which strips ANSI when stdout is not
a terminal. Piped output, captured output and the test suite therefore see the same
bytes they saw before colour existed; the column widths below are the ones the
`status` and `--journal` layouts have always used. `log.py` gates colour the same way.

Set NO_COLOR to drop styling on a terminal too.
"""

from __future__ import annotations

import io
import json
import os
import sys
from typing import Any

import typer
from typer import colors

LABEL_WIDTH = 11
"""Width of the label column in `status`: 'eid', 'status', 'workflow', 'waiting'..."""

SEQ_WIDTH = 14
TYPE_WIDTH = 22
FID_WIDTH = 32
AT_WIDTH = 24

STATUS_COLORS = {
    "pending": colors.BRIGHT_BLACK,
    "running": colors.BLUE,
    "suspended": colors.YELLOW,
    "completed": colors.GREEN,
    "failed": colors.RED,
    "cancelled": colors.MAGENTA,
}

EVENT_COLORS = {
    "execution.completed": colors.GREEN,
    "execution.failed": colors.RED,
    "execution.cancelled": colors.MAGENTA,
    "execution.suspended": colors.YELLOW,
    "execution.started": colors.BLUE,
}


def use_color(stream: Any = None) -> bool:
    """True when the stream is a terminal and NO_COLOR is unset. Default: stdout."""
    if os.environ.get("NO_COLOR"):
        return False
    target = sys.stdout if stream is None else stream
    return bool(getattr(target, "isatty", lambda: False)())


def paint(text: str, fg: str | None = None, *, bold: bool = False, dim: bool = False) -> str:
    """Style `text`, or return it unchanged when colour is off."""
    if not use_color() or (fg is None and not bold and not dim):
        return text
    return typer.style(text, fg=fg, bold=bold or None, dim=dim or None)


# --- status ---------------------------------------------------------------------------


def line(label: str, value: str) -> str:
    """A `status` line whose value is already styled: dim label, then the value as given."""
    return paint(label.ljust(LABEL_WIDTH), dim=True) + value


def field(label: str, value: str, fg: str | None = None, *, bold: bool = False) -> str:
    """A `status` line: the label dim in its column, then the value.

    The label keeps its padding outside the style so the column never moves.
    """
    return paint(label.ljust(LABEL_WIDTH), dim=True) + paint(value, fg, bold=bold)


def status_value(state: str) -> str:
    """Colour a status by what it means: green done, red failed, yellow waiting."""
    return paint(state, STATUS_COLORS.get(state.split(" ")[0], colors.CYAN), bold=True)


def workflow_value(name: str, version: str) -> str:
    return paint(name, colors.CYAN, bold=True) + " " + paint(f"v{version}", dim=True)


def actor_value(kind: str, ident: str) -> str:
    """`human:thomas@example.com` with the kind dim and the identity plain."""
    return paint(f"{kind}:", dim=True) + ident


# --- journal --------------------------------------------------------------------------


def event_value(event_type: str) -> str:
    """Colour an event type the way its status colours: completed green, failed red."""
    return paint(event_type, EVENT_COLORS.get(event_type, _frame_color(event_type)), bold=True)


def _frame_color(event_type: str) -> str | None:
    return colors.CYAN if event_type.startswith("frame.") else None


def journal_header() -> str:
    head = (
        f"{'seq':>{SEQ_WIDTH}}  {'type':<{TYPE_WIDTH}} "
        f"{'fid':<{FID_WIDTH}} {'at':<{AT_WIDTH}} actor"
    )
    return paint(head, dim=True, bold=True)


def journal_row(seq: int, event_type: str, fid: str, at: str, kind: str, ident: str) -> str:
    """One journal line. Every cell is padded before it is styled."""
    fg = EVENT_COLORS.get(event_type, _frame_color(event_type))
    return (
        paint(f"{seq:>{SEQ_WIDTH}}", dim=True)
        + "  "
        + paint(f"{event_type:<{TYPE_WIDTH}}", fg)
        + " "
        + f"{fid:<{FID_WIDTH}}"
        + " "
        + paint(f"{at:<{AT_WIDTH}}", dim=True)
        + " "
        + actor_value(kind, ident)
    )


# --- payloads -------------------------------------------------------------------------


def json_block(value: Any, *, indent: int = 2) -> str:
    """Pretty JSON, syntax highlighted on a terminal.

    Uses rich's `ansi_dark` theme, which paints with the terminal's own sixteen
    colours rather than a fixed palette, so it suits light and dark themes alike.
    """
    text = json.dumps(value, indent=indent, sort_keys=True, default=str)
    if not use_color():
        return text
    from rich.console import Console
    from rich.syntax import Syntax

    buf = io.StringIO()
    Console(file=buf, force_terminal=True, soft_wrap=True, width=10_000).print(
        Syntax(text, "json", theme="ansi_dark", background_color="default")
    )
    return "\n".join(line.rstrip() for line in buf.getvalue().rstrip("\n").split("\n"))  # pragma: no mutate


# --- reports and errors ---------------------------------------------------------------


def counters(pairs: list[tuple[str, int]]) -> str:
    """`timers_fired=1 recovered=0`, with the counts that did something in bold."""
    out = []
    for name, count in pairs:
        value = paint(str(count), colors.GREEN, bold=True) if count else paint("0", dim=True)
        out.append(f"{paint(name, dim=True)}={value}")
    return " ".join(out)


def _prefixed(word: str, fg: str, message: str) -> None:
    """`word: message` on stderr, coloured when stderr is a terminal."""
    prefix = typer.style(word, fg=fg, bold=True) if use_color(sys.stderr) else word
    typer.echo(f"{prefix} {message}", err=True)


def error(message: str) -> None:
    """Write `error: ...` to stderr, red on a terminal."""
    _prefixed("error:", colors.RED, message)


def warning(message: str) -> None:
    """Write `warning: ...` to stderr, yellow on a terminal."""
    _prefixed("warning:", colors.YELLOW, message)
