[app]
title = SpeedFinder
package.name = speedfinder
package.domain = org.speedfinder
source.dir = .
source.include_exts = py,png,jpg,kv,atlas
version = 1.0
requirements = python3,kivy==2.3.0,certifi
orientation = portrait
fullscreen = 0
android.permissions = INTERNET
android.archs = arm64-v8a
android.ndk = 25b
android.accept_sdk_license = True
p4a.branch = release-2024.01.21

[buildozer]
log_level = 2
warn_on_root = 0
