# Progressive Disclosure for AI Agents

> How we shrunk our agent's system prompt from 239KB to 34KB without losing a single behavioral constraint.
> 
> **Stateless compression doesn't work for agent constitutions. Here's what does.**

---

## The Problem

AI agents accumulate rules. Over months of operation, our agent's behavioral constitution grew to:

| File | Size | Lines |
|------|------|-------|
| SOUL.md (rules & identity) | 103KB | 1,398 |
| MEMORY.md (context & preferences) | 61KB | 356 |
| USER.md (user profile) | 75KB | 477 |
| **Total per session** | **239KB** | **2,231** |

Every new conversation injected all of it. Our DeepSeek API bill hit ¥100/day during development spikes — not from output, but from **cache-miss tokens**: when the system prompt prefix doesn't match the cache, you pay 120× more (¥3/M vs ¥0.025/M).

The obvious answer — "just summarize with an LLM" — has a fatal flaw: **LLMs silently drop constraints**. A missing rule doesn't throw an error. The agent just behaves wrong, and you discover it days later.

We needed compression that never regresses.

---

## The Architecture

Three files, three strategies, one invariant: **mechanical verification guards every LLM-generated summary**.

```
                    ┌─────────────────────────────┐
                    │     System Prompt (34KB)     │
                    │  ┌───────────────────────┐   │
                    │  │ SOUL.md v0.7.1 (7.3KB) │   │  ← Manually curated compression
                    │  │ "Converged Edition"    │   │    Rules grouped, examples stripped,
                    │  └───────────────────────┘   │    cross-references preserved
                    │  ┌───────────────────────┐   │
                    │  │ MEMORY_SUMMARY (22KB)  │   │  ← LLM-generated summary
                    │  │ Regenerated on MEMORY   │   │    Guard block appended mechanically
                    │  │ change + per-/new check │   │
                    │  └───────────────────────┘   │
                    │  ┌───────────────────────┐   │
                    │  │ USER_SUMMARY (4.6KB)   │   │  ← LLM-generated summary
                    │  │ Regenerated on USER write │   │    YAML constraint guard enforces
                    │  │ + per-/new check         │   │
                    │  └───────────────────────┘   │
                    └─────────────────────────────┘

    Fallback: if any summary is missing or stale (>24h), inject the full version.
    Guard:   if any constraint is dropped, the mechanical guard appends it back.
```

### Layer 1: SOUL.md — Curated Compression

The agent's behavioral rules are too important to trust to an LLM summary. We manually maintain a "converged edition" (~108 lines, 7.3KB) that preserves every rule reference while stripping examples and rationale.

**Key insight**: SOUL rules are a closed set — they grow slowly and deliberately. A human-curated compression is cheaper and more reliable than an LLM one.

```python
# prompt_builder.py — priority path
soul_path = get_hermes_home() / "memories" / "SOUL.md"   # Compressed (7KB)
if not soul_path.exists():
    soul_path = get_hermes_home() / "SOUL.md"              # Full (103KB) fallback
```

A file watcher (`soul_change_detector.sh`) monitors the full version and alerts when it changes, so the compressed version stays in sync.

### Layer 2: MEMORY.md — LLM Summary + Mechanical Guard

MEMORY grows organically — new facts, preferences, and context are added daily. We use DeepSeek V3 to generate a summary (~22KB) on every MEMORY write and on `/new` session refresh, but we don't trust it blindly.

**The Governance Decay Guard** is the key innovation:

```
WRITE-TIME (plugin hook)              READ-TIME (memory_tool.py)
┌──────────────────────┐              ┌──────────────────────┐
│ MEMORY.md write       │              │ Summary injection     │
│        ↓              │              │        ↓              │
│ 1. Write to disk      │              │ 1. Load summary       │
│ 2. Hook triggers      │              │ 2. grep YAML keys     │
│ 3. Regenerate summary │              │ 3. Append missing     │
│    via DS V3          │              │    constraints        │
│ 4. Append guard block │              │ 4. Inject             │
│    (55 must-have      │              │                       │
│     constraints)      │              │                       │
└──────────────────────┘              └──────────────────────┘
```

The 55 must-have constraints live in an external file (`MEMORY_CONSTRAINTS.txt`). After every LLM summary generation, a mechanical grep checks all 55 keys exist. Missing ones are appended.

This achieves **94% constraint coverage** in the summary — the LLM handles 94% correctly, and the mechanical guard catches the remaining 6%. Combined, zero constraints are lost.

### Layer 3: USER.md — YAML Constraint Validation

Same pattern, different enforcement mechanism. `USER_CONSTRAINTS.yaml` defines 38 required fields. The guard validates the summary against this schema on every injection.

All summaries have a 24-hour freshness window. If stale, the full version is injected instead — the system **fails open**, never silently degrading.

---

## Design Principles

### 1. Mechanical Guard > LLM Trust

> "The enforcer and the violator cannot be the same agent."

LLMs are excellent compressors and terrible auditors. Every LLM-generated output in this system has a **mechanical, grep-based validator** that runs independently. The guard lives in:
- A plugin hook (write-time)
- The injection path (read-time)  
- A cron script (periodic audit)

No single point of LLM trust. No silent regression.

### 2. Fail Open, Never Silent

If a summary is missing, stale, or corrupted, the system injects the full version. It costs more but preserves correctness. **Never optimize cost at the expense of constraint integrity.**

### 3. Externalize Constraints

The list of things that must never be dropped lives in plain text files (`MEMORY_CONSTRAINTS.txt`, `USER_CONSTRAINTS.yaml`). Adding a new constraint is one line. No code changes, no deployment, no LLM retraining.

### 4. Prefix Cache Stability

A long-lived conversation reuses a cached system prompt prefix every turn. If the prompt changes mid-conversation, the cache invalidates. Our injection code freezes the snapshot at session start and never touches it again — preserving cache across all turns.

---

## Cost Impact

| Metric | Before | After |
|--------|--------|-------|
| System prompt per session | 239KB | 34KB |
| Cache-hit rate (stable usage) | 97.8% | TBD (observing) |
| Daily DS API cost (baseline) | ~¥33 | TBD (observing) |

The real savings aren't just in smaller prompts — they're in **cache stability**. A stable, smaller system prompt means more turns share the same prefix, which means more cache hits, which means 120× cheaper input tokens.

---

## How to Adapt for Your Agent

### Prerequisites

- Hermes Agent or any agent framework with pluggable system prompt injection
- A model with prefix caching (DeepSeek, Anthropic Claude, OpenAI)
- Willingness to maintain a curated rule summary

### Step 1: Audit Your Injection

```bash
# Find your current injection size
wc -c ~/.hermes/SOUL.md ~/.hermes/memories/MEMORY.md ~/.hermes/memories/USER.md
```

### Step 2: Curate Your SOUL Compression

Don't use an LLM for this. Your behavioral rules are a closed set — group related rules, strip examples, preserve all rule IDs and cross-references. Target: **keep every rule, drop every example**.

### Step 3: Set Up Summary Generation

Create a cron job that:
1. Reads your full MEMORY.md / USER.md
2. Calls your preferred model for summarization
3. Writes to MEMORY_SUMMARY.md / USER_SUMMARY.md
4. Applies the mechanical guard

### Step 4: Wire the Injection Path

Modify your system prompt builder to:
1. Try the summary file first
2. Check freshness (<24h)
3. Apply constraint guard
4. Fall back to full version if anything fails

### Step 5: Add the Mechanical Guard

Create a constraints file listing every rule/constraint that MUST survive compression. The guard script greps for each key and appends missing ones to the summary.

---

## Files in This Release

| Path | Purpose |
|------|---------|
| `agent/prompt_builder.py` | SOUL.md priority path injection |
| `tools/memory_tool.py` | MEMORY + USER summary injection with guards |
| `agent/system_prompt.py` | System prompt assembly order |
| `cron/scheduler.py` | Cron post-hook for summary regeneration |

Supporting files (constraint lists, guard scripts) are in `references/`.

---

## Known Limitations

- **LLM summary quality varies by model.** DeepSeek V3 works well for factual content but occasionally drops nuanced preferences. This is why the mechanical guard exists — it catches what the LLM misses.
- **Curated SOUL compression requires maintenance.** When rules change, the compressed version must be updated. The file watcher (`soul_change_detector.sh`) alerts but doesn't auto-sync — by design, since no LLM should be trusted to rewrite behavioral rules.
- **Summary freshness is a gate, not a schedule.** Summaries are regenerated on MEMORY/USER write (plugin hook) and on every `/new` session refresh — not on a fixed cron schedule. The 24h check is a freshness threshold for using an existing summary, not the regeneration cadence.
- **Not a general-purpose compressor.** This is designed for agent constitutions — structured, rule-dense text. It won't help with general conversation context.
- **External enforcement is essential, not optional.** The Governance Decay Guard depends on cron scripts, plugin hooks, and file watchers running independently from the agent. If the host goes down, mechanical guards go down with it. We run these on a dedicated always-on machine with [3-2-1 backup](https://en.wikipedia.org/wiki/Backup#The_3-2-1_rule) for disaster recovery.

---

## Lessons Learned

1. **Don't optimize what you haven't measured.** We spent two weeks building progressive disclosure before realizing our SOUL.md was being injected from the wrong path. Always verify the injection path first.

2. **Cache-miss is the real cost driver, not total tokens.** A 103KB prompt with 98% cache-hit is cheaper than a 34KB prompt with 90% cache-hit. Optimize for cache stability, not just size.

3. **LLMs cannot be trusted to audit themselves.** Every guard in this system is a grep command. The LLM generates; the script validates. Never the same agent for both.

4. **Externalized constraints are a superpower.** Adding "never drop Rule X from the summary" is one line in a text file. No deployment, no testing, no risk.

---

## License & Attribution

Part of the Hermes Agent ecosystem. See repository LICENSE for terms.

Built by [ohtokaah-sys](https://github.com/ohtokaah-sys). If you adapt this pattern, I'd love to hear about it — open an issue or DM me.

---

> *"The best compression algorithm for agent constitutions isn't a model — it's a model with a bouncer standing behind it holding a checklist."*

---

---

# AI Agent 渐进式披露系统

> 我们如何将 Agent 的系统提示从 239KB 压缩到 34KB，一条行为约束都没丢。
> 
> **对 Agent 的行为宪法来说，无状态的压缩不起作用。以下是我的方案。**

---

## 问题

AI Agent 会不断积累规则。运行几个月后，我们的 Agent 行为宪法膨胀到了：

| 文件 | 大小 | 行数 |
|------|------|------|
| SOUL.md（规则与身份） | 103KB | 1,398 |
| MEMORY.md（上下文与偏好） | 61KB | 356 |
| USER.md（用户画像） | 75KB | 477 |
| **每次会话总计** | **239KB** | **2,231** |

每次新会话全部注入。我们的 DeepSeek API 账单在开发高峰日冲到 ¥100/天——不是因为输出 token，而是 **cache-miss token**：当系统提示前缀和缓存不匹配时，你要付 120 倍的价钱（¥3/M vs ¥0.025/M）。

最直观的答案——"用 LLM 摘要一下不就行了"——有一个致命缺陷：**LLM 会悄悄丢掉约束**。丢掉的规则不会报错，Agent 只是行为错了，你几天后才发现。

我们要的是永远不会退行的压缩。

---

## 架构

三个文件，三种策略，一个不变式：**机械验证守卫每一个 LLM 生成的摘要**。

```
                    ┌─────────────────────────────┐
                    │      System Prompt (34KB)    │
                    │  ┌───────────────────────┐   │
                    │  │ SOUL.md v0.7.1 (7.3KB) │   │  ← 人工维护的收敛版
                    │  │ "收敛版"               │   │    规则归类，案例剥离，
                    │  └───────────────────────┘   │    交叉引用保留
                    │  ┌───────────────────────┐   │
                    │  │ MEMORY_SUMMARY (22KB)  │   │  ← LLM 生成摘要
                    │  │ MEMORY 写入触发         │   │    机械 Guard 追加约束块
                    │  │ + 每次 /new 按需刷新     │   │
                    │  └───────────────────────┘   │
                    │  ┌───────────────────────┐   │
                    │  │ USER_SUMMARY (4.6KB)   │   │  ← LLM 生成摘要
                    │  │ USER 写入触发           │   │    YAML 约束 Guard 校验
                    │  │ + 每次 /new 按需刷新     │   │
                    │  └───────────────────────┘   │
                    └─────────────────────────────┘

    回退：摘要缺失或过期(>24h) → 自动注入全量版。
    Guard：任何约束被丢弃 → 机械追加回去。
```

### 第一层：SOUL.md — 人工维护的收敛版

Agent 的行为规则太重要，不能交给 LLM 压缩。我们手动维护了一份"收敛版"（~108 行，7.3KB），保留全部规则引用，剥离案例和推理过程。

**关键洞察**：SOUL 规则是一个闭合集合——增长缓慢且刻意。人工压缩比 LLM 压缩更便宜也更可靠。

```python
# prompt_builder.py — 优先路径
soul_path = get_hermes_home() / "memories" / "SOUL.md"   # 压缩版 (7KB)
if not soul_path.exists():
    soul_path = get_hermes_home() / "SOUL.md"              # 全量版 (103KB) 回退
```

`soul_change_detector.sh` 监听全量版变更，一旦改动立即通知，确保压缩版同步。

### 第二层：MEMORY.md — LLM 摘要 + 机械 Guard

MEMORY 是持续增长的——新事实、偏好、上下文每天追加。我们用 DeepSeek V3 在每次 MEMORY 写入时重新生成摘要（~22KB），并在每次 `/new` 时按需刷新，但绝不盲目信任。

**Governance Decay Guard（治理退行守卫）** 是核心创新：

```
WRITE-TIME（写入时，插件 hook）       READ-TIME（读取时，memory_tool.py）
┌──────────────────────┐              ┌──────────────────────┐
│ MEMORY.md 写入        │              │ 摘要注入 system prompt │
│        ↓              │              │        ↓              │
│ 1. 落盘               │              │ 1. 加载摘要            │
│ 2. Hook 触发          │              │ 2. grep 扫描 YAML 键  │
│ 3. DS V3 重新生成摘要  │              │ 3. 追加缺失约束        │
│ 4. 追加 Guard 约束块   │              │ 4. 注入                │
│    (55 条必须保留的    │              │                        │
│     约束)             │              │                        │
└──────────────────────┘              └──────────────────────┘
```

55 条必须保留的约束存储在外部文件 `MEMORY_CONSTRAINTS.txt` 中。每次 LLM 生成摘要后，机械 grep 检查全部 55 个键是否存在，缺失的直接追加。

实现了 **94% 的约束覆盖率**——LLM 正确处理 94%，机械 Guard 兜底剩余 6%。两者合力，零丢失。

### 第三层：USER.md — YAML 约束校验

同样的模式，不同的执法机制。`USER_CONSTRAINTS.yaml` 定义 38 个必填字段，Guard 在每次注入时校验摘要是否覆盖全部字段。

所有摘要都有 24 小时新鲜度窗口。过期自动回退全量版——系统 **fail open**，绝不悄悄降级。

---

## 设计原则

### 1. 机械执法 > LLM 信任

> "执法者和违规者不能是同一个 Agent。"

LLM 是优秀的压缩器、糟糕的审计师。这个系统里每一个 LLM 生成的输出，都有一个独立的、基于 grep 的机械校验器。Guard 分布在：
- 插件 hook（写入时）
- 注入路径（读取时）
- cron 脚本（周期性审计）

没有单点 LLM 信任。没有静默退行。

### 2. Fail Open，绝不静默

摘要缺失、过期或损坏 → 注入全量版。成本高一点，但正确性不丢。**永远不要为了省钱牺牲约束完整性。**

### 3. 约束外置

"绝对不能丢的东西"清单放在纯文本文件里（`MEMORY_CONSTRAINTS.txt`、`USER_CONSTRAINTS.yaml`）。新增一条约束只需一行。不改代码、不部署、不重训模型。

### 4. 前缀缓存稳定性

一个长会话每轮都会复用缓存的系统提示前缀。如果系统提示在会话中途变化，缓存失效。我们的注入代码在会话开始时冻结快照，之后永不触碰——保证跨越全部轮次的前缀缓存稳定。

---

## 成本影响

| 指标 | 之前 | 之后 |
|------|------|------|
| 每次会话系统提示 | 239KB | 34KB |
| 缓存命中率（稳定使用） | 97.8% | 观察中 |
| DS API 日均费用（基线） | ~¥33 | 观察中 |

真正的节省不只在于提示变小——在于**缓存稳定性**。更小更稳定的系统提示意味着更多轮次共享同一前缀，更多 cache hit，120 倍便宜。

---

## 适配你自己的 Agent

### 前置条件

- Hermes Agent 或任何支持系统提示注入的 Agent 框架
- 支持前缀缓存的模型（DeepSeek、Anthropic Claude、OpenAI）
- 愿意维护一份人工整理的规则摘要

### 第一步：审计当前的注入量

```bash
wc -c ~/.hermes/SOUL.md ~/.hermes/memories/MEMORY.md ~/.hermes/memories/USER.md
```

### 第二步：整理 SOUL 压缩版

不要用 LLM 做这件事。你的行为规则是闭合集合——归类相关规则、剥离案例、保留全部规则 ID 和交叉引用。目标：**保留每一条规则，删除每一个案例**。

### 第三步：搭建摘要生成

创建一个 cron 任务：
1. 读取全量 MEMORY.md / USER.md
2. 调用你选择的模型生成摘要
3. 写入 MEMORY_SUMMARY.md / USER_SUMMARY.md
4. 执行机械 Guard

### 第四步：接入注入路径

修改系统提示构建器：
1. 优先尝试摘要文件
2. 检查新鲜度（<24h）
3. 执行约束 Guard
4. 任何失败回退全量版

### 第五步：搭建机械 Guard

创建约束清单文件，列出所有必须存活过压缩的规则/约束。Guard 脚本逐条 grep，缺失的直接追加到摘要末尾。

---

## 本次发布包含的文件

| 路径 | 用途 |
|------|------|
| `agent/prompt_builder.py` | SOUL.md 优先路径注入 |
| `tools/memory_tool.py` | MEMORY + USER 摘要注入 + Guard |
| `agent/system_prompt.py` | 系统提示组装顺序 |
| `cron/scheduler.py` | Cron post-hook 摘要重新生成 |

辅助文件（约束清单、Guard 脚本）在 `references/` 目录。

---

## 已知局限

- **不同模型摘要质量不同。** DeepSeek V3 对事实性内容效果好，但偶尔会丢细微偏好。这正是机械 Guard 存在的理由——它兜底 LLM 漏掉的部分。
- **SOUL 压缩版需要人工维护。** 规则变更时压缩版必须同步更新。`soul_change_detector.sh` 会通知但不会自动同步——这是刻意的：行为规则不该交给任何 LLM 自动改写。
- **摘要新鲜度是门限，不是周期。** 摘要在 MEMORY/USER 写入时（插件 hook）和每次 `/new` 会话刷新时重新生成——不是按固定 cron 排程。24h 只是决定是否使用已有摘要的新鲜度阈值，不是生成频率。
- **不是通用压缩器。** 这套系统专为 Agent 行为宪法设计——结构化、规则密集的文本。不适用于普通对话上下文。
- **外部执法者是必需品，不是可选项。** Governance Decay Guard 依赖独立于 Agent 运行的 cron 脚本、插件 hook 和文件监听器。宿主机宕机，机械 Guard 也停。我们运行在专用的常开机器上，配有 [3-2-1 备份](https://en.wikipedia.org/wiki/Backup#The_3-2-1_rule) 机制保障灾难恢复。

---

## 教训

1. **没度量的东西别优化。** 我们花了两周搭建渐进式披露，然后发现 SOUL.md 一直在从错误的路径注入。永远先验证注入路径。

2. **cache-miss 才是真正的成本驱动者，不是 token 总量。** 103KB 提示 + 98% 缓存命中，比 34KB 提示 + 90% 缓存命中更便宜。优化对象是缓存稳定性，不是文本大小。

3. **LLM 不能用来审计自己。** 这个系统里的每一个 Guard 都是 grep 命令。LLM 负责生成，脚本负责校验。生成和校验永远不是同一个 Agent。

4. **外置约束是超能力。** "不要把 Rule X 从摘要里丢掉"就是文本文件里的一行。不改代码、不测试、零风险。

---

## 许可与署名

Hermes Agent 生态的一部分。详见仓库 LICENSE。

由 [ohtokaah-sys](https://github.com/ohtokaah-sys) 构建。如果你适配了这个方案，我很想听听——提 issue 或直接 DM。

---

> *"Agent 行为宪法的最好压缩算法不是模型——是一个模型加一个站在后面拿着检查清单的保安。"*
