# 01 — 纯 Go 的 FLV→MP4 转封装（可选功能，留痕待办）

**Status:** ready-for-human

**来源：** 2026-08-24 设计访谈（"收尾改为纯 Go FLV 合并"）的附带决定。当时
的核心需求是"把一场会话的 FLV 分段合并成一个文件"，已用纯 Go 合并器
（`internal/data/merge.go`）实现；"最终产物为 MP4"被降级为可选功能，
并明确**不走 ffmpeg 内嵌路线**，本文件留痕。

**要做什么：** 会话收尾在合并出单个 FLV 之后（或替代之），把 H.264/AAC
流拷贝封装进 MP4 容器（stream copy，不重编码），使产物在 QuickTime、
电视盒子等不认 FLV 的播放器上可播。

**方向（访谈共识）：优先纯 Go 实现**——
- 复用现有 `internal/data/flv/` 的标签级解析；
- 用 go-mp4（abema）或 mp4ff（Eyevinn）之类的库写 MP4 封装：从 AVC/AAC
  sequence header 提取 avcC/AudioSpecificConfig，按采样构建
  moov/mdat，处理 ctts（B 帧）与 edit list；
- 收益：与合并器共用一套解析、零外部二进制、无二进制分发与 GPL 问题。

**为什么不走 ffmpeg 内嵌：** 见 `.scratch/ffmpeg-static-builds/research.md`
——5 平台静态构建的来源碎片化（macOS arm64 无 7.x GPL 静态版）、
构建期下载与校验成本、二进制体积（每个平台 100MB+）与 GPL 分发义务，
复杂度与"可选功能"的定位严重不成比例。

**注意：** 本票是用户明确表示"以后再提"的备忘，无人认领前不要动手。

## Comments
