# Static FFmpeg Builds for Embedding — Research Notes

**Research date:** 2026-08-24 · **Requirement:** static, self-contained ffmpeg 7.x GPL binaries suitable for embedding in a Go program (Linux x86_64, Linux arm64, Windows x86_64, macOS).

## TL;DR

| Target | Best source | 7.x available? | sha256 published | License |
|---|---|---|---|---|
| Linux x86_64 | [BtbN/FFmpeg-Builds](https://github.com/BtbN/FFmpeg-Builds) | ⚠️ ended 2026-08-17; last is `autobuild-2026-08-16-13-00` | ✅ `checksums.sha256` per release | GPLv3 (binary) |
| Linux arm64 | BtbN/FFmpeg-Builds | ⚠️ same as above | ✅ same | GPLv3 |
| Windows x86_64 | BtbN/FFmpeg-Builds | ⚠️ same as above | ✅ same | GPLv3 |
| Windows arm64 | BtbN/FFmpeg-Builds | ⚠️ same as above (exists!) | ✅ same | GPLv3 |
| macOS Intel | evermeet.cx | ✅ 7.1 / 7.1.1 archived | ❌ GPG sig only | GPL (`--enable-gpl --enable-version3`) |
| macOS arm64 | osxexperts.net | ❌ arm64 only as 9.0 | ✅ sha256 on page (verified) | GPL (`--enable-gpl`) |

**Headline finding:** BtbN dropped the 7.1 series from its build matrix on 2026-08-17 (replaced by FFmpeg 9.0; current series are master, 8.1, 9.0). Upstream, FFmpeg 7.1.5 is still a maintained branch ([ffmpeg.org](https://www.ffmpeg.org/download.html) lists it; newest release is 9.0.1 "Lei"). If 7.x is a hard requirement, pin the last BtbN 7.1 build below and **vendor/mirror the assets** — BtbN's retention policy deletes old daily releases. There is **no static GPL arm64 macOS 7.x build from any primary source**; the macOS arm64 options are FFmpeg 9.0 (osxexperts) or Rosetta-emulated Intel builds (evermeet).

---

## 1. Linux x86_64 & arm64 — BtbN/FFmpeg-Builds

Repo: https://github.com/BtbN/FFmpeg-Builds · Releases: https://github.com/BtbN/FFmpeg-Builds/releases

### Release channels and current state (verified via GitHub API)

- `GET https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest` resolves to the **floating `latest` tag** — a master-snapshot channel republished daily ("Latest Auto-Build (2026-08-23 13:03)", `target_commitish: master`). This is NOT the stable channel.
- Stable releases are tagged `autobuild-YYYY-MM-DD-HH-MM` (daily). Newest stable tag verified: **`autobuild-2026-08-23-13-03`** ("Auto-Build 2026-08-23 13:03", published 2026-08-23T13:04:04Z, 49 assets) via `https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=30`.
- Current stable series (asset prefixes): master (`ffmpeg-N-126247-gb79d4c4c0a-*`), **n8.1.2** and **n9.0.1**. **The 7.1 series is gone.** Scanning daily tags via the API: `autobuild-2026-08-16-13-00` still had series `{N, n7.1.5, n8.1.2}`; `autobuild-2026-08-17-13-05` has `{N, n8.1.2, n9.0.1}`. 7.1 was rotated out when FFmpeg 9.0 landed.
- Floating `latest` tag assets are named `ffmpeg-master-latest-linux64-gpl.tar.xz`, `ffmpeg-n8.1-latest-linux64-gpl-8.1.tar.xz`, `ffmpeg-n9.0-latest-linux64-gpl-9.0.tar.xz` (+ arm64/win variants) — verified via `https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/tags/latest`. The historical `ffmpeg-n7.1-latest-linux64-gpl-7.1.tar.xz` URL now returns **404** (verified with an HTTP range request against `releases/download/latest/…`).

### Naming pattern

`ffmpeg-<describe>-<os><bits>-<variant>[-shared]-<branch>.tar.xz`, e.g.
`ffmpeg-n7.1.5-16-g9a4bb2c579-linux64-gpl-7.1.tar.xz`.
`<describe>` is the git describe of the built branch (`n7.1.5` tag + 16 commits + short hash). The trailing `-7.1` is the ffmpeg release branch the build tracks. `-shared` variants bundle libav* shared libs instead of pure-static binaries (not suitable for embedding). `gpl` = full dependency set including GPL-only libs (x264/x265); `lgpl` lacks them. (README: https://github.com/BtbN/FFmpeg-Builds — "variants" and "addins" sections; note the README's addin list still says `4.4`–`7.1` while the builds now also cover 8.1/9.0.)

### linux-arm64 GPL builds — confirmed

Present in every release, e.g. in the last 7.1 release (`autobuild-2026-08-16-13-00`, verified via `https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/tags/autobuild-2026-08-16-13-00`):

- `ffmpeg-n7.1.5-16-g9a4bb2c579-linuxarm64-gpl-7.1.tar.xz` — 102,880,044 bytes (98.1 MiB)
- download URL: `https://github.com/BtbN/FFmpeg-Builds/releases/download/autobuild-2026-08-16-13-00/ffmpeg-n7.1.5-16-g9a4bb2c579-linuxarm64-gpl-7.1.tar.xz`

README caveats for linuxarm64: no `davs2`/`xavs2` (broken on aarch64), no `libmfx`/`libva` (Intel QSV, x86-only). Minimum platform: glibc ≥ 2.28 / kernel ≥ 4.18 (RHEL/CentOS 8+).

### Checksums

One `checksums.sha256` manifest per release (not per-asset `.sha256` files), covering all 48 archive assets in standard `sha256sum` format. Verified by downloading the current one:

- URL pattern: `https://github.com/BtbN/FFmpeg-Builds/releases/download/<tag>/checksums.sha256`
- Example: https://github.com/BtbN/FFmpeg-Builds/releases/download/autobuild-2026-08-23-13-03/checksums.sha256 (5,712 bytes, 48 lines, e.g. `18bd59b528f2…  ffmpeg-N-126247-gb79d4c4c0a-win64-gpl-shared.zip`)
- Present in older releases too (verified for `autobuild-2026-07-31-14-10`).

### Sizes

**Measured** (downloaded `ffmpeg-n7.1.5-16-g9a4bb2c579-linux64-gpl-7.1.tar.xz` from `autobuild-2026-08-16-13-00` and extracted):

| Asset | Compressed | Uncompressed `ffmpeg` binary |
|---|---|---|
| linux64-gpl-7.1.tar.xz | 119,819,996 B ≈ 114.3 MiB | 139,880,168 B ≈ 133.4 MiB |
| linuxarm64-gpl-7.1.tar.xz | 102,880,044 B ≈ 98.1 MiB (API) | ~120 MiB (**estimate**: archive size ratio × measured linux64 binary) |

Notes: archives also contain `ffprobe` (~139.7 MiB) and `ffplay` (~139.7 MiB) plus ~25 MiB of HTML docs; extract only `bin/ffmpeg` (+ `bin/ffprobe`) when embedding. Binary is a stripped statically-linked ffmpeg (ELF, GNU/Linux 4.18+, no libav* shared deps). Sizes for the 8.1/9.0 series are essentially identical (linux64-gpl: 125.8 MiB / 126.6 MiB compressed per the GitHub API for `autobuild-2026-08-23-13-03`).

### License

- Repo/build scripts: MIT (verified via `https://api.github.com/repos/BtbN/FFmpeg-Builds/license`).
- **The `-gpl-` binaries ship with `LICENSE.txt` containing the full GNU GPL version 3 text** (verified inside the downloaded linux64 archive and listed in the win64 zip's central directory). Distribute accordingly (GPLv3 terms, source offer required if you redistribute the binary).

### Retention policy (matters for pinning!)

README: *"The last build of each month is kept for two years. The last 14 daily builds are kept. The special 'latest' build floats."* Verified against the releases list: only month-end tags survive older than ~14 days. Consequence: `autobuild-2026-08-16-13-00` (a daily, and the last 7.1 build) **will be deleted** by this policy within days; the month-end **`autobuild-2026-07-31-14-10`** is retained ~2 years (until ~2028-07) and also contains 7.1.

### Pinned-for-embedding recommendation (Linux)

- Strict 7.x: pin **`autobuild-2026-07-31-14-10`**, assets `ffmpeg-n7.1.5-12-g1fdbca85aa-linux64-gpl-7.1.tar.xz` (119,007,364 B) and `ffmpeg-n7.1.5-12-g1fdbca85aa-linuxarm64-gpl-7.1.tar.xz` (101,894,512 B), e.g.
  `https://github.com/BtbN/FFmpeg-Builds/releases/download/autobuild-2026-07-31-14-10/ffmpeg-n7.1.5-12-g1fdbca85aa-linux64-gpl-7.1.tar.xz` — verify against that release's `checksums.sha256`. (The absolute newest 7.1 snapshot is `autobuild-2026-08-16-13-00`/`n7.1.5-16-g9a4bb2c579`, but it is retention-prone to deletion.)
- Regardless of tag, **vendor the extracted binary into your own storage/artifact registry**; do not hot-link GitHub release URLs at runtime.
- If 7.x is not actually mandatory: prefer the current **8.1 series** (`ffmpeg-n8.1.2-44-g7c533d0f86-linux64-gpl-8.1.tar.xz` in `autobuild-2026-08-23-13-03`) — 8.1 is still an actively maintained FFmpeg branch; 7.1 builds are now frozen forever.

---

## 2. Windows x86_64 — BtbN win64-gpl

Same repo/release machinery as Linux. Asset naming: `ffmpeg-<describe>-win64-gpl-7.1.zip` (note `.zip`, not `.tar.xz`). Last 7.1 example (verified via release API):

- `ffmpeg-n7.1.5-16-g9a4bb2c579-win64-gpl-7.1.zip` — 159,542,614 bytes (152.2 MiB)
- URL: `https://github.com/BtbN/FFmpeg-Builds/releases/download/autobuild-2026-08-16-13-00/ffmpeg-n7.1.5-16-g9a4bb2c579-win64-gpl-7.1.zip`
- Month-end alternative (retained): `ffmpeg-n7.1.5-12-g1fdbca85aa-win64-gpl-7.1.zip` (158,697,002 B) in `autobuild-2026-07-31-14-10`.

**Contents & sizes** — verified by parsing the zip's central directory via an HTTP range request (full download exceeded this network's patience): contains `bin/ffmpeg.exe` **138,577,920 B ≈ 132.2 MiB uncompressed** (50.1 MiB compressed in-zip), `bin/ffplay.exe` (132.0 MiB), `bin/ffprobe.exe` (132.0 MiB), `LICENSE.txt` (GPLv3), `doc/`, `presets/`. So yes, `ffmpeg.exe` is inside, single self-contained PE.

**Checksums:** same per-release `checksums.sha256` manifest (covers the win assets too — verified content).

**Windows arm64 — yes, it exists now.** Every current series ships `winarm64` variants (verified in asset lists): last 7.1 example `ffmpeg-n7.1.5-16-g9a4bb2c579-winarm64-gpl-7.1.zip` (112,076,219 B); current 9.0 example `ffmpeg-n9.0.1-6-g9d4ca21220-winarm64-gpl-9.0.zip` (115,071,381 B). README says auto-builds run for "win(arm)64 and linux(arm)64" (the README's *targets* list itself only names `win64`/`win32`/`linux64`/`linuxarm64`; the release assets are the authoritative evidence for winarm64). No win32 auto-builds. Windows floor: Windows 10 22H2 (README).

**Pinned-for-embedding recommendation (Windows):** same tags as Linux — `autobuild-2026-07-31-14-10` for 7.1 (or `autobuild-2026-08-16-13-00` if you mirror immediately), else 8.1/9.0 series from the newest stable tag. Vendor the binary; verify against `checksums.sha256`.

---

## 3. macOS — investigated sources

`https://www.ffmpeg.org/download.html` (fetched 2026-08-24) lists **exactly one** macOS provider: evermeet.cx ("Static builds for macOS 64-bit"). The previous second entry (osxexperts.net used to be listed there historically) is no longer on the page; nothing else on the page covers macOS.

### 3.1 evermeet.cx — still publishing in 2026, but Intel-only

Site: https://evermeet.cx/ffmpeg/ (fetched live 2026-08-24)

- **Versions:** current release build **FFmpeg 9.0.1** (`ffmpeg-9.0.1.zip`/`.7z`, compile of the n9.0.1 release) and a **master snapshot** `ffmpeg-126221-g96d82d90c3f` (compiled 2026-08-20); same for ffprobe/ffplay. Machine-readable: https://evermeet.cx/ffmpeg/info/ffmpeg/release → `{"version":"9.0.1","size":80776328,"download":{"7z":{"size":18068408},"zip":{"size":26172529}}}` and `…/info/ffmpeg/snapshot` (binary 80,912,840 B, zip 26,243,096 B).
- **Arch:** **x86_64 only, no Apple Silicon.** The maintainer's page https://evermeet.cx/ffmpeg/apple-silicon-arm states: *"I do not plan to provide native ffmpeg binaries for Apple Silicon ARM"* (reasons: no ARM hardware, libs' ARM support, x265 multi-bit on ARM) and claims the Intel binaries run on ARM via Rosetta "without a performance hit". It also warns *"I might stop providing binaries altogether."* (Binary-level arch confirmation not performed — the server's download rate from this network was ~1 Mb/min and timed out; the statement above is the primary source.)
- **Checksums:** no sha256. Archives are **GPG-signed** (`.sig` next to each file; key `0x476C4B611A660874`, fingerprint `20F6 EA3E 0CFD 6B4C 5344 7A73 476C 4B61 1A66 0874`, downloadable at https://evermeet.cx/ffmpeg/0x1A660874.asc — see the page's Remarks section).
- **License:** built with `--enable-gpl … --enable-version3` and static (`--pkg-config-flags=--static`) — the build configuration is printed on the page itself. GPL therefore (GPLv2+ upgraded by version3 clause); no bundled LICENSE file observed.
- **Pinning:** versioned archives are kept at https://evermeet.cx/pub/ffmpeg/ — verified listing includes `ffmpeg-9.0.1.zip`, `ffmpeg-9.0.zip`, `ffmpeg-8.1.2.zip` … back through **`ffmpeg-7.1.zip` and `ffmpeg-7.1.1.zip`** to 3.x (7.1.5 was never archived; release builds only track the then-latest release). Snapshots archive: https://evermeet.cx/pub/ffmpeg/snapshots/. There is also a download API: `https://evermeet.cx/ffmpeg/get[/release]/(ffmpeg|ffprobe|ffplay)/(7z|zip)` (floating, latest-only). Binaries target macOS 10.13+ (Remarks, 2026-03-29).
- **Sizes:** ffmpeg binary ≈ 80.8 MiB; zip ≈ 26.2 MiB; 7z ≈ 18.1 MiB (from the info API, release 9.0.1).

**7.x macOS pin via evermeet (Intel only):** `https://evermeet.cx/pub/ffmpeg/ffmpeg-7.1.1.zip` — stable versioned URL, GPG-verifiable, but x86_64-only (Rosetta on Apple Silicon).

### 3.2 osxexperts.net — the only arm64 option, with caveats

Site: https://osxexperts.net/ (fetched live 2026-08-24). Offers static builds:

| File | Label | Arch (verified) | True version (verified) | zip size | binary size |
|---|---|---|---|---|---|
| `https://www.osxexperts.net/ffmpeg9arm.zip` | ffmpeg 9.0 (Apple Silicon) | Mach-O arm64 | FFmpeg 9.0 (embedded string "FFmpeg version 9.0", libavcodec 63.1) | 22,608,364 B | 52,070,376 B (49.7 MiB) |
| `https://www.osxexperts.net/ffmpeg80intel.zip` | ffmpeg 8.0 (Intel) | Mach-O x86_64 | FFmpeg 8.0 (libavcodec 62.3) | 26,155,398 B | 78,290,848 B (74.7 MiB) |

Also ffprobe (`ffprobe9arm.zip` / `ffprobe80intel.zip`) and ffplay variants.

- **Checksums: yes, published on the page and verified.** The page lists SHA256 "of FFmpeg file" — this is the hash of the **binary inside the zip**, not the zip. Both downloaded binaries matched exactly: arm64 `591260c945d0eef150e3bf82b0ef988bd36a9cecc18ff05d6679617159f0a95e`, Intel `df3f1e3facdc1ae0ad0bd898cdfb072fbc9641bf47b11f172844525a05db8d11` (`shasum -a 256`).
- **License:** binaries built `--enable-gpl` (Intel also `--enable-version3`) — verified from the embedded `configuration:` string in each binary; the configure command line is also published on the page. Static (`--pkg-config-flags=--static`). No corresponding-source offer on the site, and the page says *"The provided ffmpeg, ffplay and ffprobe files are for educational purposes only."* Their `license.html` is just Apache-2.0 boilerplate unrelated to the binaries.
- **Provenance anomaly:** the page's "Source used to compile" links for the arm builds point to FFmpeg `release/6.1`, which contradicts the verified 9.0 binary — the links are stale template leftovers, not the actual source. Flagged.
- **Pinning:** URLs are **floating** (`ffmpeg9arm.zip` gets overwritten on each update); no versioned archive. Installation requires `xattr -dr com.apple.quarantine` and ad-hoc signing on arm64 (`codesign -s -`), per the site's own instructions.

**macOS arm64 pin (pragmatic):** download `ffmpeg9arm.zip` (and `ffprobe9arm.zip`) once, verify the sha256 against the page value, and **vendor the binaries in your own storage**. It is FFmpeg 9.0, not 7.x — no 7.x arm64 build exists from this source.

### 3.3 Others checked

- **ffmpeg.martin-riedl.de** (formerly referenced macOS static-build project): **unreachable** — connection failure (curl exit 000) on 2026-08-24. Treat as dead.
- **[eugeneware/ffmpeg-static](https://github.com/eugeneware/ffmpeg-static)** (GitHub; powers the npm `ffmpeg-static` package): does ship `darwin-arm64` static binaries with per-platform LICENSE files — latest release **b6.1.1** (published 2025-11-14): `ffmpeg-darwin-arm64` 45,568,216 B (43.5 MiB), gz 18.4 MiB (verified via `https://api.github.com/repos/eugeneware/ffmpeg-static/releases/latest`). Stuck on FFmpeg 6.1.1 → does not satisfy a 7.x requirement, noted as a fallback.
- Homebrew ffmpeg: not static (dynamically linked against cellar libs) — not suitable; osxexperts' own page makes the same point.

### macOS bottom line

- **Intel (x86_64):** evermeet is solid and pinnable (`/pub/ffmpeg/ffmpeg-7.1.1.zip` for 7.x), GPG-verified, but the maintainer is openly disengaged — mirror anything you depend on.
- **arm64:** only osxexperts (9.0) or ffmpeg-static (6.1.1). No GPL static 7.x arm64 macOS build exists from any verified source as of 2026-08-24. If the Go program's remux-only ffmpeg usage tolerates a version split across platforms, arm64-macOS = osxexperts 9.0 (vendored), everything else = BtbN 7.1.5. Otherwise build arm64 macOS ffmpeg 7.1 from source (osxexperts publishes a full static-build script for Apple Silicon on its homepage that could be retargeted at release/7.1).

---

## Open items / could not verify

- evermeet binary arch confirmed only by maintainer statement (download too slow to complete from this network).
- linuxarm64 uncompressed binary size is an estimate (no arm64 hardware here to extract-check quickly); compressed size is exact from the GitHub API.
- osxexperts ffprobe/ffplay zips not downloaded (checksums listed on the page; assumed same pattern).
- Whether BtbN will keep honoring the 2-year month-end retention for `autobuild-2026-07-31-14-10` — policy is stated in the README, not contractual; vendoring removes the risk.
