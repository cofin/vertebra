"""Generate light and dark themed architecture SVG diagrams for Zensical documentation."""

import argparse
import html
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

__all__ = (
    "DIAGRAM_SPECS",
    "DiagramPalette",
    "DARK_PALETTE",
    "LIGHT_PALETTE",
    "main",
    "render_all_diagrams",
    "render_circuit_breaker",
    "render_cloud_dialers",
    "render_runtime_architecture",
    "render_shm_broker",
)

SANS_FONT = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
MONO_FONT = "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"


@dataclass(frozen=True)
class DiagramPalette:
    """Color tokens aligned with Vertebra's light and dark documentation themes."""

    name: str
    group_bg: str
    group_border: str
    card_bg: str
    card_border: str
    text: str
    muted: str
    arrow: str
    blue_bg: str
    blue_border: str
    blue_text: str
    gold_bg: str
    gold_border: str
    gold_text: str
    green_bg: str
    green_border: str
    green_text: str
    rose_bg: str
    rose_border: str
    rose_text: str
    pill_bg: str
    pill_border: str


LIGHT_PALETTE = DiagramPalette(
    name="light",
    group_bg="#f8fafc",
    group_border="#cbd5e1",
    card_bg="#ffffff",
    card_border="#cbd5e1",
    text="#0f172a",
    muted="#475569",
    arrow="#64748b",
    blue_bg="#eff6ff",
    blue_border="#93c5fd",
    blue_text="#1d4ed8",
    gold_bg="#fffbeb",
    gold_border="#f59e0b",
    gold_text="#b45309",
    green_bg="#f0fdf4",
    green_border="#86efac",
    green_text="#15803d",
    rose_bg="#fff1f2",
    rose_border="#fda4af",
    rose_text="#be123c",
    pill_bg="#f1f5f9",
    pill_border="#cbd5e1",
)

DARK_PALETTE = DiagramPalette(
    name="dark",
    group_bg="#111827",
    group_border="#334155",
    card_bg="#1e293b",
    card_border="#334155",
    text="#f1f5f9",
    muted="#94a3b8",
    arrow="#94a3b8",
    blue_bg="#172554",
    blue_border="#3b82f6",
    blue_text="#93c5fd",
    gold_bg="#27200d",
    gold_border="#d97706",
    gold_text="#edb641",
    green_bg="#052e16",
    green_border="#22c55e",
    green_text="#86efac",
    rose_bg="#2f1119",
    rose_border="#f43f5e",
    rose_text="#fda4af",
    pill_bg="#1e293b",
    pill_border="#334155",
)


def _esc(value: str) -> str:
    """Escape text for safe inclusion in SVG elements."""
    return html.escape(value, quote=True)


def _svg_header(width: int, height: int, title: str, p: DiagramPalette) -> list[str]:
    """Return opening SVG markup, title, shared styles, and arrow markers."""
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img" aria-label="{_esc(title)}">',
        f"  <title>{_esc(title)}</title>",
        "  <defs>",
        _arrow_marker("arrow-muted", p.arrow),
        _arrow_marker("arrow-blue", p.blue_border),
        _arrow_marker("arrow-gold", p.gold_border),
        _arrow_marker("arrow-green", p.green_border),
        _arrow_marker("arrow-rose", p.rose_border),
        "  </defs>",
        "  <style>",
        f"    .sans {{ font-family: {SANS_FONT}; }}",
        f"    .mono {{ font-family: {MONO_FONT}; }}",
        "    .title { font-size: 13.5px; font-weight: 600; }",
        "    .title-lg { font-size: 14.5px; font-weight: 600; }",
        "    .sub { font-size: 11px; font-weight: 400; }",
        "    .kicker { font-size: 10px; font-weight: 700; letter-spacing: 0.08em; }",
        "    .label { font-size: 11px; font-weight: 500; }",
        "    .caption { font-size: 11.5px; font-weight: 400; }",
        "  </style>",
    ]


def _arrow_marker(marker_id: str, color: str) -> str:
    """Render a crisp filled triangular arrowhead marker."""
    return (
        f'    <marker id="{marker_id}" viewBox="0 0 10 10" refX="8" refY="5" '
        f'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        f'<path d="M 1 1.5 L 9 5 L 1 8.5 z" fill="{color}"/></marker>'
    )


def _group_box(
    x: int,
    y: int,
    w: int,
    h: int,
    kicker: str,
    note: str,
    p: DiagramPalette,
    *,
    border: str | None = None,
    kicker_color: str | None = None,
    fill: str | None = None,
) -> list[str]:
    """Render a dashed container box representing a process or subsystem boundary."""
    stroke = border or p.group_border
    bg = fill or p.group_bg
    k_color = kicker_color or p.muted
    lines = [
        f'  <rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" '
        f'fill="{bg}" stroke="{stroke}" stroke-width="1.25" stroke-dasharray="5 4"/>',
        f'  <text x="{x + 16}" y="{y + 22}" class="sans kicker" fill="{k_color}">{_esc(kicker)}</text>',
    ]
    if note:
        lines.append(
            f'  <text x="{x + w - 16}" y="{y + 22}" text-anchor="end" '
            f'class="sans sub" fill="{p.muted}">{_esc(note)}</text>'
        )
    return lines


def _card(
    x: int,
    y: int,
    w: int,
    h: int,
    title: str,
    subtitle: str,
    p: DiagramPalette,
    *,
    bg: str | None = None,
    border: str | None = None,
    title_color: str | None = None,
    large: bool = False,
    subtitle_mono: bool = False,
    badge: tuple[str, str, str] | None = None,
) -> list[str]:
    """Render a rounded component card with centered or left-aligned text."""
    fill = bg or p.card_bg
    stroke = border or p.card_border
    t_color = title_color or p.text
    cx = x + w / 2
    t_cls = "sans title-lg" if large else "sans title"
    s_cls = "mono sub" if subtitle_mono else "sans sub"
    lines = [
        f'  <rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="1.25"/>',
    ]
    if badge is not None:
        badge_text, badge_bg, badge_fg = badge
        bw = max(54, len(badge_text) * 7 + 18)
        bx = x + w - bw - 12
        by = y + 10
        lines.extend(
            [
                f'  <rect x="{bx}" y="{by}" width="{bw}" height="18" rx="9" fill="{badge_bg}"/>',
                f'  <text x="{bx + bw / 2:.1f}" y="{by + 12.5:.1f}" text-anchor="middle" '
                f'class="sans kicker" fill="{badge_fg}">{_esc(badge_text)}</text>',
            ]
        )
    if subtitle:
        ty = y + h * (0.56 if badge is not None else 0.44)
        sy = y + h * (0.82 if badge is not None else 0.74)
        lines.extend(
            [
                f'  <text x="{cx:.1f}" y="{ty:.1f}" text-anchor="middle" class="{t_cls}" '
                f'fill="{t_color}">{_esc(title)}</text>',
                f'  <text x="{cx:.1f}" y="{sy:.1f}" text-anchor="middle" class="{s_cls}" '
                f'fill="{p.muted}">{_esc(subtitle)}</text>',
            ]
        )
    else:
        ty = y + h / 2 + 4.5
        lines.append(
            f'  <text x="{cx:.1f}" y="{ty:.1f}" text-anchor="middle" class="{t_cls}" '
            f'fill="{t_color}">{_esc(title)}</text>'
        )
    return lines


def _arrow(
    d: str,
    color: str,
    marker: str,
    *,
    width: float = 1.6,
    dashed: bool = False,
    start_marker: str | None = None,
) -> str:
    """Render an SVG path connector with directional arrowheads."""
    dash_attr = ' stroke-dasharray="5 4"' if dashed else ""
    start_attr = f' marker-start="url(#{start_marker})"' if start_marker else ""
    return (
        f'  <path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
        f'stroke-linecap="round" stroke-linejoin="round"{dash_attr}{start_attr} '
        f'marker-end="url(#{marker})"/>'
    )


def _pill(
    cx: float, cy: float, text: str, p: DiagramPalette, *, color: str | None = None, bg: str | None = None
) -> list[str]:
    """Render a compact pill label centered at (cx, cy)."""
    w = max(56, len(text) * 6.5 + 18)
    h = 20
    x = cx - w / 2
    y = cy - h / 2
    fill = bg or p.pill_bg
    fg = color or p.muted
    return [
        f'  <rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h}" rx="10" '
        f'fill="{fill}" stroke="{p.pill_border}" stroke-width="1"/>',
        f'  <text x="{cx:.1f}" y="{cy + 3.8:.1f}" text-anchor="middle" '
        f'class="sans label" fill="{fg}">{_esc(text)}</text>',
    ]


def render_runtime_architecture(p: DiagramPalette) -> str:
    """Render the dual-topology runtime overview diagram."""
    lines = _svg_header(860, 520, "Vertebra Runtime Architecture", p)

    lines.extend(
        _card(
            230,
            14,
            400,
            52,
            "Clients",
            "Browser  ·  @vertebra/client  ·  gRPC  ·  MCP",
            p,
        )
    )
    lines.append(_arrow("M 430 66 L 430 96", p.arrow, "arrow-muted"))

    lines.extend(
        _card(
            110,
            100,
            372,
            72,
            "Rust HTTP Server",
            "HTTP/1.1 · HTTP/2 · HTTP/3 · gRPC · WS · SSE",
            p,
            bg=p.blue_bg,
            border=p.blue_border,
            title_color=p.blue_text,
            large=True,
        )
    )

    lines.append(_arrow("M 482 136 L 548 136", p.green_border, "arrow-green"))
    lines.append(f'  <text x="515" y="127" text-anchor="middle" class="sans sub" fill="{p.green_text}">offload</text>')
    lines.extend(
        _card(
            552,
            104,
            288,
            64,
            "Handled in Rust",
            "Static files · CORS · compression · /healthz",
            p,
            bg=p.green_bg,
            border=p.green_border,
            title_color=p.green_text,
        )
    )

    lines.append(f'  <path d="M 296 172 L 296 196" fill="none" stroke="{p.arrow}" stroke-width="1.6"/>')
    lines.append(f'  <path d="M 218 196 L 642 196" fill="none" stroke="{p.arrow}" stroke-width="1.6"/>')
    lines.append(_arrow("M 218 196 L 218 216", p.blue_border, "arrow-blue"))
    lines.append(_arrow("M 642 196 L 642 216", p.gold_border, "arrow-gold"))

    lines.extend(
        _group_box(
            20,
            220,
            396,
            174,
            "EMBEDDED MODE",
            "Single OS process",
            p,
            border=p.blue_border,
            kicker_color=p.blue_text,
        )
    )
    lines.extend(
        _card(
            42,
            254,
            352,
            46,
            "In-Process Batch Bridge",
            "Direct memory handoff, zero IPC",
            p,
        )
    )
    lines.append(_arrow("M 218 300 L 218 320", p.arrow, "arrow-muted"))
    lines.extend(
        _card(
            42,
            324,
            352,
            54,
            "Python App & Worker Threads",
            "Default for 1 worker or free-threaded Python",
            p,
        )
    )

    lines.extend(
        _group_box(
            444,
            220,
            396,
            174,
            "SHARED-MEMORY MODE",
            "Multi-process",
            p,
            border=p.gold_border,
            kicker_color=p.gold_text,
        )
    )
    lines.extend(
        _card(
            466,
            254,
            352,
            46,
            "Lock-Free SHM Rings",
            "Tiered ring buffers + futex / ulock wakeups",
            p,
            bg=p.gold_bg,
            border=p.gold_border,
            title_color=p.gold_text,
        )
    )
    lines.append(f'  <path d="M 642 300 L 642 311" fill="none" stroke="{p.arrow}" stroke-width="1.5"/>')
    lines.append(f'  <path d="M 520 311 L 764 311" fill="none" stroke="{p.arrow}" stroke-width="1.5"/>')
    lines.append(_arrow("M 520 311 L 520 321", p.arrow, "arrow-muted"))
    lines.append(_arrow("M 642 311 L 642 321", p.arrow, "arrow-muted"))
    lines.append(_arrow("M 764 311 L 764 321", p.arrow, "arrow-muted"))
    for idx, wx in enumerate((466, 588, 710)):
        label = f"Worker {idx + 1}" if idx < 2 else "Worker N"
        lines.extend(_card(wx, 324, 108, 54, label, "Python proc", p))

    lines.append(_arrow("M 218 394 L 218 420", p.arrow, "arrow-muted"))
    lines.append(_arrow("M 642 394 L 642 420", p.arrow, "arrow-muted"))

    lines.extend(
        _group_box(
            20,
            424,
            820,
            84,
            "NATIVE RUST SUBSYSTEMS",
            "Optional extras",
            p,
        )
    )
    lines.extend(_card(36, 452, 250, 44, "Databases & Arrow", "SQL, cache & Arrow IPC", p))
    lines.extend(_card(305, 452, 250, 44, "Object Storage", "S3, GCS, Azure, R2, OCI, local", p))
    lines.extend(_card(574, 452, 250, 44, "Bundler & SSR", "Oxc / Rolldown & QuickJS pool", p))

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_circuit_breaker(p: DiagramPalette) -> str:
    """Render the circuit breaker state machine diagram."""
    lines = _svg_header(860, 272, "Circuit Breaker Lifecycle", p)

    lines.append(f'  <circle cx="24" cy="136" r="5" fill="{p.arrow}"/>')
    lines.append(_arrow("M 29 136 L 54 136", p.arrow, "arrow-muted"))

    lines.extend(
        _card(
            58,
            96,
            212,
            80,
            "Closed",
            "Normal traffic · counts errors",
            p,
            bg=p.green_bg,
            border=p.green_border,
            title_color=p.green_text,
            large=True,
        )
    )
    lines.extend(
        _card(
            344,
            96,
            204,
            80,
            "Open",
            "Fast-fail 429 · skips workers",
            p,
            bg=p.rose_bg,
            border=p.rose_border,
            title_color=p.rose_text,
            large=True,
        )
    )
    lines.extend(
        _card(
            622,
            96,
            216,
            80,
            "Half-Open",
            "Allows limited probe traffic",
            p,
            bg=p.gold_bg,
            border=p.gold_border,
            title_color=p.gold_text,
            large=True,
        )
    )

    lines.append(_arrow("M 270 136 L 340 136", p.rose_border, "arrow-rose"))
    lines.append(
        f'  <text x="305" y="114" text-anchor="middle" class="sans label" fill="{p.rose_text}">error rate</text>'
    )
    lines.append(f'  <text x="305" y="128" text-anchor="middle" class="sans sub" fill="{p.muted}">≥ threshold</text>')

    lines.append(_arrow("M 548 136 L 618 136", p.gold_border, "arrow-gold"))
    lines.append(f'  <text x="583" y="114" text-anchor="middle" class="sans label" fill="{p.gold_text}">window</text>')
    lines.append(f'  <text x="583" y="128" text-anchor="middle" class="sans sub" fill="{p.muted}">elapsed</text>')

    lines.append(
        _arrow(
            "M 730 96 L 730 44 Q 730 32 718 32 L 176 32 Q 164 32 164 44 L 164 92",
            p.green_border,
            "arrow-green",
        )
    )
    lines.extend(_pill(447, 32, "N consecutive probes succeed", p, color=p.green_text, bg=p.green_bg))

    lines.append(
        _arrow(
            "M 730 176 L 730 224 Q 730 236 718 236 L 458 236 Q 446 236 446 224 L 446 180",
            p.rose_border,
            "arrow-rose",
        )
    )
    lines.extend(_pill(588, 236, "any 5xx probe fails", p, color=p.rose_text, bg=p.rose_bg))

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_shm_broker(p: DiagramPalette) -> str:
    """Render the shared-memory database connection broker fan-in diagram."""
    lines = _svg_header(860, 316, "Shared-Memory Connection Broker", p)

    worker_ys = (24, 112, 200)
    for idx, wy in enumerate(worker_ys):
        label = f"Worker Process {idx + 1}" if idx < 2 else "Worker Process N"
        lines.extend(_card(20, wy, 186, 62, label, "SQLSpec / SQLAlchemy", p))
        cy = wy + 31
        lines.append(_arrow(f"M 206 {cy} L 258 {cy}", p.arrow, "arrow-muted"))

    lines.extend(
        _group_box(
            262,
            24,
            184,
            238,
            "SHARED MEMORY",
            "",
            p,
            border=p.gold_border,
            kicker_color=p.gold_text,
        )
    )
    lines.extend(
        _card(
            278,
            62,
            152,
            82,
            "Lease Table",
            "Connection handoff",
            p,
            bg=p.gold_bg,
            border=p.gold_border,
            title_color=p.gold_text,
        )
    )
    lines.extend(
        _card(
            278,
            160,
            152,
            82,
            "Statement Cache",
            "Shared prepared plans",
            p,
        )
    )

    lines.append(_arrow("M 446 143 L 486 143", p.blue_border, "arrow-blue"))

    lines.extend(
        _group_box(
            490,
            34,
            192,
            218,
            "BROKER PROCESS",
            "",
            p,
            border=p.blue_border,
            kicker_color=p.blue_text,
        )
    )
    lines.extend(
        _card(
            506,
            72,
            160,
            58,
            "BrokerHost",
            "vtb broker serve",
            p,
            subtitle_mono=True,
        )
    )
    lines.append(_arrow("M 586 130 L 586 156", p.arrow, "arrow-muted"))
    lines.extend(
        _card(
            506,
            160,
            160,
            72,
            "1 Bounded Pool",
            "Shared by all workers",
            p,
            bg=p.blue_bg,
            border=p.blue_border,
            title_color=p.blue_text,
        )
    )

    lines.append(_arrow("M 682 196 L 718 196", p.green_border, "arrow-green"))

    lines.extend(
        _card(
            722,
            152,
            118,
            88,
            "Database",
            "Bounded conns",
            p,
            bg=p.green_bg,
            border=p.green_border,
            title_color=p.green_text,
        )
    )

    lines.append(
        f'  <text x="430" y="298" text-anchor="middle" class="sans caption" fill="{p.muted}">'
        "N worker processes share 1 connection pool instead of opening N separate pools.</text>"
    )

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_cloud_dialers(p: DiagramPalette) -> str:
    """Render the Cloud SQL and AlloyDB control-plane vs data-plane dialer diagram."""
    lines = _svg_header(860, 294, "Cloud SQL and AlloyDB Direct Dialer", p)

    lines.extend(
        _group_box(
            20,
            24,
            262,
            246,
            "VERTEBRA WORKER",
            "In-process",
            p,
            border=p.blue_border,
            kicker_color=p.blue_text,
        )
    )
    lines.extend(
        _card(
            40,
            62,
            222,
            74,
            "GCP Token Cache",
            "ADC + background refresh",
            p,
            bg=p.gold_bg,
            border=p.gold_border,
            title_color=p.gold_text,
        )
    )
    lines.append(_arrow("M 151 136 L 151 172", p.arrow, "arrow-muted"))
    lines.append(f'  <text x="161" y="158" class="sans sub" fill="{p.muted}">cert + IAM token</text>')
    lines.extend(
        _card(
            40,
            176,
            222,
            74,
            "Rust mTLS Dialer",
            "TLS 1.3 + IAM auth",
            p,
            bg=p.blue_bg,
            border=p.blue_border,
            title_color=p.blue_text,
        )
    )

    lines.extend(
        _card(
            548,
            46,
            292,
            92,
            "Cloud SQL / AlloyDB API",
            "Issues ephemeral X.509 client cert",
            p,
            bg=p.gold_bg,
            border=p.gold_border,
            title_color=p.gold_text,
            badge=("CONTROL PLANE", p.pill_bg, p.gold_text),
        )
    )
    lines.extend(
        _card(
            548,
            166,
            292,
            92,
            "Cloud SQL / AlloyDB Instance",
            "Direct mTLS on :3307 or :5433",
            p,
            bg=p.green_bg,
            border=p.green_border,
            title_color=p.green_text,
            badge=("DATA PLANE", p.pill_bg, p.green_text),
        )
    )

    lines.append(
        _arrow(
            "M 266 96 L 544 96",
            p.gold_border,
            "arrow-gold",
            dashed=True,
            start_marker="arrow-gold",
        )
    )
    lines.append(
        f'  <text x="405" y="84" text-anchor="middle" class="sans label" fill="{p.gold_text}">'
        "1. Fetch ephemeral cert (cached)</text>"
    )

    lines.append(_arrow("M 262 213 L 544 213", p.green_border, "arrow-green", width=2.0))
    lines.append(
        f'  <text x="405" y="201" text-anchor="middle" class="sans label" fill="{p.green_text}">'
        "2. Direct mTLS connection</text>"
    )
    lines.extend(_pill(405, 236, "No Auth Proxy sidecar", p, color=p.muted, bg=p.pill_bg))

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


DIAGRAM_SPECS: tuple[tuple[str, Callable[[DiagramPalette], str]], ...] = (
    ("runtime-architecture", render_runtime_architecture),
    ("circuit-breaker", render_circuit_breaker),
    ("shm-broker", render_shm_broker),
    ("cloud-dialers", render_cloud_dialers),
)


def render_all_diagrams(output_dir: Path, *, check: bool = False) -> list[str]:
    """Write or verify all light and dark SVG diagram files under output_dir."""
    drift: list[str] = []
    if not check:
        output_dir.mkdir(parents=True, exist_ok=True)

    for stem, renderer in DIAGRAM_SPECS:
        for palette in (LIGHT_PALETTE, DARK_PALETTE):
            target = output_dir / f"{stem}-{palette.name}.svg"
            expected = renderer(palette)
            if check:
                if not target.is_file() or target.read_text(encoding="utf-8") != expected:
                    drift.append(target.as_posix())
            else:
                target.write_text(expected, encoding="utf-8")
    return drift


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for generating or checking documentation SVG diagrams."""
    parser = argparse.ArgumentParser(description="Generate or verify Vertebra documentation SVG diagrams.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("docs/assets/diagrams"),
        help="Destination directory for rendered light and dark SVGs.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated SVG diagrams differ from committed files.",
    )
    args = parser.parse_args(argv)

    drift = render_all_diagrams(args.output_dir, check=args.check)
    if args.check:
        if drift:
            sys.stderr.write("Diagram SVGs are out of date:\n")
            for item in drift:
                sys.stderr.write(f"  - {item}\n")
            sys.stderr.write("Run 'uv run --no-sync python -m tools.docs.render_diagrams' to regenerate.\n")
            return 1
        sys.stdout.write("Documentation SVG diagrams are up to date.\n")
        return 0

    sys.stdout.write(f"Generated {len(DIAGRAM_SPECS) * 2} SVG diagrams in {args.output_dir}.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
