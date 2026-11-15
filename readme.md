# SpeedFinder

**SpeedFinder** is a lightweight Python (Kivy) mobile GUI application that measures real-time Wi-Fi and 4G/5G download speeds in **Mbps** and **MB/s** as you walk around a room, helping you find the spot with the fastest, most reliable internet connection.

## Features

- **Real-Time Dual Speed Readout:** Displays live download speed in both **Mbps** (Megabits/sec) and **MB/s** (Megabytes/sec) side-by-side.
- **Continuous 4-Stream Engine:** Keeps 4 parallel HTTPS download streams open for accurate full-bandwidth measurement, with automatic `HTTP 429` cooldown and multi-CDN endpoint rotation.
- **Dynamic Antenna UI:** Custom-drawn antenna icon that changes signal arcs and color (`Green` ≥ 20 Mbps, `Amber` 5–19.9 Mbps, `Red` < 5 Mbps) in real time.
- **Room Spot Finder:** Label any spot in your room (or auto-number `Spot 1`, `Spot 2`, etc.), capture a 3-sample median reading, and automatically rank all spots to highlight the **Best** location.
- **Live Ping & Data Counter:** Shows TCP latency (`ms`) and total session data used (`MB`), with a **Pause Live Test** toggle to stop data usage anytime.

## Running the App

### On Android (via Pydroid 3)
1. Install **Pydroid 3** from the Google Play Store.
2. Open `main.py` in Pydroid 3 and tap **Run**.

### On Windows / macOS / Linux
```bash
pip install kivy certifi
python main.py
```

## Building an Android `.apk` (GitHub Actions)

1. Push `main.py`, `buildozer.spec`, and `.github/workflows/build.yml` to your GitHub repository.
2. Open the **Actions** tab and wait for the **Build Android APK** workflow to finish.
3. Download the **`SpeedFinder-APK`** artifact, unzip it, and install the `.apk` on your Android device.
