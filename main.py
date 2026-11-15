"""
SpeedFinder Mobile (Kivy) - Continuous Real-Time Mbps & MB/s Mapper
===================================================================
A minimal, mobile-first Python (Kivy) GUI app for Android (Pydroid 3 / APK)
and Windows 11 that saturates your connection continuously with 4 parallel
HTTPS streams so both the app and your phone's built-in speed meter show
your true full bandwidth in real time.

How it works:
- 4 worker threads keep HTTPS streams open continuously (no 1.5s dead gaps
  and no repeated TLS handshakes every second, which also prevents HTTP 429s).
- Every 1.0 second, exact bytes received across all 4 streams are divided by
  exact elapsed time to give live Mbps and MB/s.
- Tapping 'Pause Live Test' immediately closes all streams to stop data use.
"""

from __future__ import annotations

import socket
import ssl
import statistics
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

from kivy.app import App
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.graphics import Color, Ellipse, Line, Rectangle
from kivy.metrics import dp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.widget import Widget

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
NUM_STREAMS = 4
TICK_SEC = 1.0
SAMPLES_PER_SPOT = 3
MAX_SAMPLE_FAILURES = 4
COOLDOWN_429_SEC = 60.0

# ---------------------------------------------------------------------------
# Theme Colors (Obsidian & Warm Amber)
# ---------------------------------------------------------------------------
BG_DARK = (0.059, 0.067, 0.082, 1)      # #0F1115
BG_CARD = (0.094, 0.106, 0.133, 1)      # #181B22
BG_CARD_ALT = (0.125, 0.141, 0.180, 1)  # #20242E
ACCENT_AMBER = (0.851, 0.510, 0.259, 1) # #D98242
TEXT_PRIMARY = (0.953, 0.957, 0.965, 1) # #F3F4F6
TEXT_MUTED = (0.612, 0.639, 0.686, 1)   # #9CA3AF

COLOR_GOOD = (0.133, 0.773, 0.369, 1)   # >= 20 Mbps (#22C55E)
COLOR_FAIR = (0.961, 0.620, 0.043, 1)   # 5 - 19.9 Mbps (#F59E0B)
COLOR_POOR = (0.937, 0.267, 0.267, 1)   # < 5 Mbps (#EF4444)
COLOR_OFF = (0.200, 0.224, 0.278, 1)    # Inactive arc


def _make_ssl_context() -> ssl.SSLContext:
    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


# ---------------------------------------------------------------------------
# Continuous 4-Stream Speed & Ping Engine
# ---------------------------------------------------------------------------
class SpeedEngine:
    """Keeps 4 parallel HTTPS download streams open for true full-speed readings."""

    ENDPOINTS = [
        "https://speed.cloudflare.com/__down?bytes=25000000",
        "https://sgp-ping.vultr.com/vultr.com.100MB.bin",
        "https://proof.ovh.net/files/10Mb.dat",
        "https://hel1-speed.hetzner.com/100MB.bin",
        "https://ash-speed.hetzner.com/100MB.bin",
    ]
    PING_TARGETS = [("1.1.1.1", 443), ("8.8.8.8", 443)]

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window_bytes: int = 0
        self._total_bytes: int = 0
        self._cooldowns: Dict[str, float] = {}
        self._ssl_ctx = _make_ssl_context()
        self._stream_stop = threading.Event()
        self._workers: List[threading.Thread] = []
        self._active = False
        self._last_ping: Optional[float] = None

    @property
    def total_bytes_used(self) -> int:
        with self._lock:
            return self._total_bytes

    def _add_bytes(self, n: int) -> None:
        if n > 0:
            with self._lock:
                self._window_bytes += n
                self._total_bytes += n

    def pop_window_bytes(self) -> int:
        with self._lock:
            val = self._window_bytes
            self._window_bytes = 0
            return val

    def _active_endpoints(self, offset: int = 0) -> List[str]:
        now = time.monotonic()
        with self._lock:
            ready = [u for u in self.ENDPOINTS if self._cooldowns.get(u, 0.0) <= now]
        pool = ready if ready else list(self.ENDPOINTS)
        shift = offset % len(pool)
        return pool[shift:] + pool[:shift]

    def _note_error(self, url: str, exc: BaseException) -> None:
        if isinstance(exc, urllib.error.HTTPError) and exc.code in (403, 429, 503):
            with self._lock:
                self._cooldowns[url] = time.monotonic() + COOLDOWN_429_SEC

    def _open_stream(self, url: str, timeout: float = 5.0):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 14)",
                    "Cache-Control": "no-cache",
                    "Accept-Encoding": "identity",
                },
            )
            return urllib.request.urlopen(req, timeout=timeout, context=self._ssl_ctx)
        except Exception as exc:
            self._note_error(url, exc)
            return None

    def _stream_worker(self, idx: int) -> None:
        attempt = 0
        while not self._stream_stop.is_set():
            resp = None
            endpoints = self._active_endpoints(offset=idx + attempt)
            used_url = endpoints[0]
            for url in endpoints:
                if self._stream_stop.is_set():
                    return
                resp = self._open_stream(url)
                if resp is not None:
                    used_url = url
                    break

            if resp is None:
                attempt += 1
                self._stream_stop.wait(1.0)
                continue

            try:
                read = getattr(resp, "read1", resp.read)
                while not self._stream_stop.is_set():
                    chunk = read(65536)
                    if not chunk:
                        break
                    self._add_bytes(len(chunk))
            except Exception as exc:
                self._note_error(used_url, exc)
            finally:
                try:
                    resp.close()
                except Exception:
                    pass
            attempt += 1

    def measure_ping_ms(self, timeout: float = 1.0) -> Optional[float]:
        best: Optional[float] = None
        for host, port in self.PING_TARGETS:
            start = time.perf_counter()
            try:
                with socket.create_connection((host, port), timeout=timeout):
                    rtt = (time.perf_counter() - start) * 1000.0
                    if best is None or rtt < best:
                        best = rtt
            except Exception:
                continue
        self._last_ping = round(best, 1) if best is not None else None
        return self._last_ping

    def start_streams(self) -> None:
        if self._active:
            return
        self._active = True
        self._stream_stop.clear()
        self.pop_window_bytes()
        self._workers = [
            threading.Thread(target=self._stream_worker, args=(i,), daemon=True)
            for i in range(NUM_STREAMS)
        ]
        for w in self._workers:
            w.start()

    def stop_streams(self) -> None:
        if not self._active:
            return
        self._active = False
        self._stream_stop.set()
        self._workers = []


# ---------------------------------------------------------------------------
# Dynamic Antenna Icon Widget (Kivy Canvas)
# ---------------------------------------------------------------------------
class AntennaIcon(Widget):
    """Draws a dynamic antenna tower with 4 signal arcs based on Mbps."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.mbps: float = 0.0
        self.active: bool = False
        self.bind(pos=self._redraw, size=self._redraw)

    @staticmethod
    def color_for_mbps(mbps: Optional[float]) -> tuple:
        if mbps is None or mbps <= 0:
            return COLOR_OFF
        if mbps >= 20.0:
            return COLOR_GOOD
        if mbps >= 5.0:
            return COLOR_FAIR
        return COLOR_POOR

    def set_speed(self, mbps: Optional[float]) -> None:
        self.mbps = mbps or 0.0
        self.active = mbps is not None and mbps > 0
        self._redraw()

    def _redraw(self, *args) -> None:
        self.canvas.clear()
        cx = self.x + self.width * 0.5
        cy = self.y + self.height * 0.48
        s = min(self.width, self.height)

        color = self.color_for_mbps(self.mbps if self.active else 0.0)
        if not self.active:
            lit_arcs = 0
        elif self.mbps >= 50.0:
            lit_arcs = 4
        elif self.mbps >= 20.0:
            lit_arcs = 3
        elif self.mbps >= 5.0:
            lit_arcs = 2
        else:
            lit_arcs = 1

        with self.canvas:
            Color(0.08, 0.09, 0.11, 1)
            r_bg = s * 0.45
            Ellipse(pos=(cx - r_bg, cy - r_bg), size=(r_bg * 2, r_bg * 2))

            mast_top = cy + s * 0.04
            mast_bot = cy - s * 0.28
            half_w = s * 0.08
            Color(*(TEXT_PRIMARY if self.active else TEXT_MUTED))
            Line(
                points=[cx, mast_top, cx - half_w, mast_bot, cx + half_w, mast_bot, cx, mast_top],
                width=dp(2.0),
            )
            Line(
                points=[cx - half_w * 0.55, cy - s * 0.12, cx + half_w * 0.55, cy - s * 0.12],
                width=dp(1.6),
            )

            Color(*color)
            dot_r = dp(5)
            Ellipse(pos=(cx - dot_r, mast_top - dot_r), size=(dot_r * 2, dot_r * 2))

            for idx, mult in enumerate((0.12, 0.20, 0.28, 0.36)):
                arc_r = s * mult
                is_lit = idx < lit_arcs
                Color(*(color if is_lit else COLOR_OFF))
                w = dp(2.8) if is_lit else dp(1.8)
                Line(circle=(cx, mast_top, arc_r, 45, 135), width=w)
                Line(circle=(cx, mast_top, arc_r, 225, 315), width=w)


# ---------------------------------------------------------------------------
# Styled Card Helper
# ---------------------------------------------------------------------------
class CardBox(BoxLayout):
    def __init__(self, bg=BG_CARD, **kwargs) -> None:
        super().__init__(**kwargs)
        with self.canvas.before:
            self._bg_color = Color(*bg)
            self._rect = Rectangle(pos=self.pos, size=self.size)
        self.bind(pos=self._update_rect, size=self._update_rect)

    def _update_rect(self, *args) -> None:
        self._rect.pos = self.pos
        self._rect.size = self.size


def _aligned_label(**kwargs) -> Label:
    lbl = Label(**kwargs)
    lbl.bind(size=lbl.setter("text_size"))
    return lbl


EMPTY_SPOTS_TEXT = (
    "Stand at any spot in your room, type a name (optional), and tap "
    "'Measure Spot' to save & rank."
)


# ---------------------------------------------------------------------------
# Main Kivy Application
# ---------------------------------------------------------------------------
class SpeedFinderMobileApp(App):
    def build(self) -> BoxLayout:
        self.title = "SpeedFinder — Room Mbps & MB/s Mapper"
        Window.clearcolor = BG_DARK

        self.engine = SpeedEngine()
        self.running = True
        self.bg_paused = False
        self.live_enabled = True
        self.wake = threading.Event()

        self.spot_counter = 1
        self.corner_records: Dict[str, Dict] = {}
        self._ctx: Tuple[int, Optional[str]] = (0, None)
        self.sampling_auto = False
        self.sample_failures = 0
        self.corner_samples: List[float] = []
        self.corner_pings: List[float] = []

        root = BoxLayout(orientation="vertical", padding=dp(14), spacing=dp(10))

        # 1. Header Bar
        header = BoxLayout(size_hint_y=None, height=dp(36))
        header.add_widget(
            _aligned_label(
                text="SPEEDFINDER",
                color=ACCENT_AMBER,
                font_size=dp(18),
                bold=True,
                halign="left",
                valign="middle",
            )
        )
        self.lbl_status = _aligned_label(
            text="LIVE (4-STREAM)",
            color=COLOR_GOOD,
            font_size=dp(11),
            bold=True,
            size_hint_x=None,
            width=dp(120),
            halign="right",
            valign="middle",
        )
        header.add_widget(self.lbl_status)
        root.add_widget(header)

        # 2. Main Speed + Antenna Card
        top_card = CardBox(
            orientation="horizontal",
            padding=dp(12),
            spacing=dp(12),
            size_hint_y=None,
            height=dp(175),
        )

        self.antenna = AntennaIcon(size_hint_x=0.40)
        top_card.add_widget(self.antenna)

        readout_col = BoxLayout(orientation="vertical", size_hint_x=0.60, spacing=dp(2))

        speed_row = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(54), spacing=dp(6))
        self.lbl_mbps = _aligned_label(
            text="--",
            color=TEXT_PRIMARY,
            font_size=dp(38),
            bold=True,
            halign="left",
            valign="bottom",
            size_hint_x=None,
            width=dp(105),
        )
        speed_row.add_widget(self.lbl_mbps)

        self.lbl_mbs = _aligned_label(
            text="(-- MB/s)",
            color=ACCENT_AMBER,
            font_size=dp(16),
            bold=True,
            halign="left",
            valign="bottom",
        )
        speed_row.add_widget(self.lbl_mbs)
        readout_col.add_widget(speed_row)

        self.lbl_unit = _aligned_label(
            text="Mbps  (Connecting...)",
            color=TEXT_MUTED,
            font_size=dp(13),
            bold=True,
            halign="left",
            valign="top",
            size_hint_y=None,
            height=dp(24),
        )
        readout_col.add_widget(self.lbl_unit)

        self.lbl_ping = _aligned_label(
            text="Ping: -- ms  |  Data: 0.0 MB",
            color=TEXT_MUTED,
            font_size=dp(12),
            halign="left",
            valign="middle",
            size_hint_y=None,
            height=dp(26),
        )
        readout_col.add_widget(self.lbl_ping)

        self.btn_toggle = Button(
            text="Pause Live Test",
            background_normal="",
            background_color=BG_CARD_ALT,
            color=TEXT_PRIMARY,
            font_size=dp(12),
            bold=True,
            size_hint_y=None,
            height=dp(36),
            on_press=self._toggle_live,
        )
        readout_col.add_widget(self.btn_toggle)
        top_card.add_widget(readout_col)
        root.add_widget(top_card)

        # 3. Room Spot Finder Card
        corner_card = CardBox(orientation="vertical", padding=dp(12), spacing=dp(8))

        corner_hdr = BoxLayout(size_hint_y=None, height=dp(26))
        corner_hdr.add_widget(
            _aligned_label(
                text="ROOM SPOT FINDER",
                color=ACCENT_AMBER,
                font_size=dp(12),
                bold=True,
                halign="left",
                valign="middle",
                size_hint_x=0.4,
            )
        )
        self.lbl_best = _aligned_label(
            text="Best: --",
            color=COLOR_GOOD,
            font_size=dp(12),
            bold=True,
            halign="right",
            valign="middle",
            size_hint_x=0.6,
            shorten=True,
            shorten_from="right",
        )
        corner_hdr.add_widget(self.lbl_best)
        corner_card.add_widget(corner_hdr)

        ctrl_row = BoxLayout(size_hint_y=None, height=dp(44), spacing=dp(8))
        self.input_spot = TextInput(
            text="",
            hint_text="Spot name (optional)",
            multiline=False,
            background_normal="",
            background_active="",
            background_color=BG_CARD_ALT,
            foreground_color=TEXT_PRIMARY,
            hint_text_color=TEXT_MUTED,
            cursor_color=ACCENT_AMBER,
            font_size=dp(13),
            padding=[dp(10), dp(10), dp(10), dp(10)],
            size_hint_x=0.58,
        )
        ctrl_row.add_widget(self.input_spot)

        self.btn_sample = Button(
            text="Measure Spot",
            background_normal="",
            background_color=ACCENT_AMBER,
            color=(0.07, 0.07, 0.07, 1),
            font_size=dp(13),
            bold=True,
            size_hint_x=0.42,
            on_press=self._start_corner_test,
        )
        ctrl_row.add_widget(self.btn_sample)
        corner_card.add_widget(ctrl_row)

        scroll = ScrollView()
        self.lbl_corners = Label(
            text=EMPTY_SPOTS_TEXT,
            color=TEXT_PRIMARY,
            font_size=dp(13),
            halign="left",
            valign="top",
            size_hint_y=None,
        )
        self.lbl_corners.bind(
            width=lambda *x: setattr(self.lbl_corners, "text_size", (self.lbl_corners.width, None)),
            texture_size=lambda *x: setattr(
                self.lbl_corners, "height", max(dp(80), self.lbl_corners.texture_size[1])
            ),
        )
        scroll.add_widget(self.lbl_corners)
        corner_card.add_widget(scroll)

        btn_clear = Button(
            text="Clear All Spots",
            background_normal="",
            background_color=BG_CARD_ALT,
            color=TEXT_MUTED,
            font_size=dp(12),
            size_hint_y=None,
            height=dp(32),
            on_press=self._clear_corners,
        )
        corner_card.add_widget(btn_clear)
        root.add_widget(corner_card)

        threading.Thread(target=self._poll_loop, daemon=True).start()
        return root

    def _toggle_live(self, *args) -> None:
        self.live_enabled = not self.live_enabled
        if self.live_enabled:
            self.btn_toggle.text = "Pause Live Test"
            self.lbl_status.text = "LIVE (4-STREAM)"
            self.lbl_status.color = COLOR_GOOD
        else:
            self.btn_toggle.text = "Resume Live Test"
            self.lbl_status.text = "PAUSED"
            self.lbl_status.color = TEXT_MUTED
            if self._ctx[1] is None:
                self.engine.stop_streams()
        self.wake.set()

    def _start_corner_test(self, *args) -> None:
        if self._ctx[1] is not None:
            return

        custom_name = self.input_spot.text.strip()
        if custom_name:
            spot = custom_name
            self.sampling_auto = False
        else:
            while f"Spot {self.spot_counter}" in self.corner_records:
                self.spot_counter += 1
            spot = f"Spot {self.spot_counter}"
            self.spot_counter += 1
            self.sampling_auto = True

        self.input_spot.text = ""
        self.corner_samples = []
        self.corner_pings = []
        self.sample_failures = 0
        self._ctx = (self._ctx[0] + 1, spot)
        self.btn_sample.disabled = True
        self.btn_sample.text = f"Sampling 1/{SAMPLES_PER_SPOT}..."
        self.wake.set()

    def _clear_corners(self, *args) -> None:
        if self._ctx[1] is not None:
            self._ctx = (self._ctx[0] + 1, None)
        self.btn_sample.disabled = False
        self.btn_sample.text = "Measure Spot"
        self.corner_records.clear()
        self.spot_counter = 1
        self.lbl_best.text = "Best: --"
        self.lbl_corners.text = EMPTY_SPOTS_TEXT

    def _finish_sampling(self, spot: str) -> None:
        samples, pings = self.corner_samples, self.corner_pings
        auto = self.sampling_auto
        self._ctx = (self._ctx[0], None)
        self.btn_sample.disabled = False
        self.btn_sample.text = "Measure Spot"

        if not self.live_enabled:
            self.engine.stop_streams()

        if not samples:
            if auto:
                self.spot_counter = max(1, self.spot_counter - 1)
            return

        avg_mbps = round(statistics.median(samples), 2)
        avg_ping = round(statistics.median(pings)) if pings else None
        self.corner_records[spot] = {"mbps": avg_mbps, "ping": avg_ping}
        self._render_corners()

    def _poll_loop(self) -> None:
        last_t = time.perf_counter()
        ping_tick = 0
        while self.running:
            sid, spot = self._ctx
            should_run = (not self.bg_paused) and (self.live_enabled or spot is not None)

            if not should_run:
                self.engine.stop_streams()
                self.wake.wait(0.4)
                self.wake.clear()
                last_t = time.perf_counter()
                continue

            if not self.engine._active:
                self.engine.measure_ping_ms()
                self.engine.start_streams()
                last_t = time.perf_counter()
                self.wake.wait(TICK_SEC)
                self.wake.clear()
                continue

            now = time.perf_counter()
            dt = max(0.2, now - last_t)
            last_t = now

            received = self.engine.pop_window_bytes()
            mbps: Optional[float] = round((received * 8.0) / (dt * 1_000_000.0), 2) if received > 0 else None

            ping_tick += 1
            if ping_tick % 5 == 0:
                ping_ms = self.engine.measure_ping_ms()
            else:
                ping_ms = self.engine._last_ping

            used_mb = self.engine.total_bytes_used / (1024 * 1024)
            Clock.schedule_once(
                lambda _dt, m=mbps, p=ping_ms, u=used_mb, s=sid: self._update_ui(m, p, u, s)
            )
            self.wake.wait(TICK_SEC)
            self.wake.clear()

    def _update_ui(self, mbps: Optional[float], ping_ms: Optional[float], used_mb: float, sid: int) -> None:
        color = AntennaIcon.color_for_mbps(mbps)
        self.antenna.set_speed(mbps)

        if mbps is not None:
            mbs = mbps / 8.0
            self.lbl_mbps.text = f"{mbps:.0f}" if mbps >= 100 else f"{mbps:.1f}"
            self.lbl_mbps.color = color
            self.lbl_mbs.text = f"({mbs:.1f} MB/s)"
            tier = "Fast" if mbps >= 20 else ("Moderate" if mbps >= 5 else "Slow")
            self.lbl_unit.text = f"Mbps  ({tier})"
        else:
            self.lbl_mbps.text = "0.0"
            self.lbl_mbps.color = COLOR_POOR
            self.lbl_mbs.text = "(0.0 MB/s)"
            self.lbl_unit.text = "Mbps  (No Internet)" if ping_ms is None else "Mbps  (Connecting...)"

        ping_str = f"{ping_ms:.0f} ms" if ping_ms is not None else "Timeout"
        self.lbl_ping.text = f"Ping: {ping_str}  |  Data: {used_mb:.1f} MB"

        cur_sid, spot = self._ctx
        if spot is None or sid != cur_sid:
            return

        if mbps is not None:
            self.corner_samples.append(mbps)
            if ping_ms is not None:
                self.corner_pings.append(ping_ms)
        else:
            self.sample_failures += 1

        done = len(self.corner_samples)
        if done >= SAMPLES_PER_SPOT or self.sample_failures >= MAX_SAMPLE_FAILURES:
            self._finish_sampling(spot)
        else:
            self.btn_sample.text = f"Sampling {done + 1}/{SAMPLES_PER_SPOT}..."

    def _render_corners(self) -> None:
        if not self.corner_records:
            return
        ranked = sorted(
            self.corner_records.items(),
            key=lambda kv: (
                -round(kv[1]["mbps"], 1),
                kv[1]["ping"] if kv[1]["ping"] is not None else float("inf"),
            ),
        )
        best_name, best_data = ranked[0]
        best_mbs = best_data["mbps"] / 8.0
        self.lbl_best.text = f"Best: {best_name} ({best_data['mbps']:.1f} Mbps | {best_mbs:.1f} MB/s)"

        lines = []
        for idx, (name, data) in enumerate(ranked, start=1):
            badge = "  * BEST" if idx == 1 else ""
            mbs = data["mbps"] / 8.0
            p_txt = f"{data['ping']:.0f} ms" if data["ping"] is not None else "--"
            lines.append(f"{idx}. {name} — {data['mbps']:.1f} Mbps ({mbs:.1f} MB/s) | Ping: {p_txt}{badge}")
        self.lbl_corners.text = "\n".join(lines)

    def on_pause(self) -> bool:
        self.bg_paused = True
        self.engine.stop_streams()
        return True

    def on_resume(self) -> None:
        self.bg_paused = False
        self.wake.set()

    def on_stop(self) -> None:
        self.running = False
        self.engine.stop_streams()
        self.wake.set()


if __name__ == "__main__":
    SpeedFinderMobileApp().run()