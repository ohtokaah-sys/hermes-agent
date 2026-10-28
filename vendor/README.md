# vendor/ — 本地重打包的 browser-harness wheel

## 这份文件是什么

`browser_harness-0.1.13.post1-py3-none-any.whl` 是上游 `browser-harness==0.1.13`
**同一份源码**的重打包 wheel，只改了两处元数据：

1. `Requires-Dist: pillow==12.3.0` → `pillow>=12.3.0`
   （上游硬钉与本项目 Pillow 13.0.0 冲突；`uv pip check` 读已安装 dist-info，
   `override-dependencies` 管不到这一层）
2. `Version: 0.1.13` → `0.1.13.post1`
   （避免与索引上的 0.1.13 混淆；`.postN` 是 PEP 440 合法的本地补丁位）

**业务代码一行未动。**

## 为什么不用「安装后再修」

`uv pip check` 的输入是已安装的 dist-info，而 `override-dependencies` 只影响解析阶段。
所以靠后置脚本改 metadata 属于「装完再补」，换机器/换 PM 代次就失效。改成 path source
后，**装出来就是合规的**，任何环境的 `uv pip check` 都过。

## 完整性校验（必做）

本地 path source 不支持 hash 锁定，因此哈希单独记账：

```bash
cd vendor && sha256sum -c SHA256SUMS
```

`SHA256SUMS` 里是本仓库认可的唯一哈希。wheel 被替换/误提交时，这条命令会 FAIL。
升级或重打包后必须同步更新 `SHA256SUMS`，否则视为篡改。

**机械判据（打包/CI/每日巡检统一调用这一个）：**

```bash
python3 ~/.hermes/scripts/verify_vendor_wheels.py --repo ~/.hermes/hermes-agent
# exit 0 = wheel 哈希 & uv.sources 路径全部一致；exit 1 = 逐条列出问题
```

已接线的三处：① `.github/workflows/vendor-integrity.yml`（push/PR 改到 `vendor/**`、
`pyproject.toml`、`uv.lock` 时跑）；② 每日 13:00 `hermes-upgrade-watch.sh`（发现问题
进微信投递，校验器自身丢失也会报）；③ 打包脚本必须显式调用（不要自己再写一份 `sha256sum -c`，
判据只有一份，避免两把尺子）。

## 迁移性警告

`tool.uv.sources` 是 **uv 专有**字段，且**不写进构建产物的 metadata**：

- `uv sync` / `uv pip install` + 仓库内 `vendor/` → 正常
- `pip install .`、`uv pip install --no-sources`、或缺 `vendor/` 的 sdist 二次构建 → 会因
  `0.1.13.post1` 不在任何索引而**解析失败**（大声报错，不会静默装成别的 0.1.13 变体）

所以打包/CI 脚本必须固定走 `uv` + 仓库根目录。这是有意的「宁可失败，不装错」。

## 何时可以整块删除

上游放宽 `pillow` 约束并发布新版本后：

1. `pyproject.toml`：`browser-harness==0.1.13.post1` 改回上游新版本号
2. 删 `[tool.uv.sources]` 整段
3. 删 `override-dependencies` 里的 `"pillow==13.0.0"`
4. 删 `vendor/` 整个目录
5. `hermes pm lock` 重新生成本地 lock（上游动作）
