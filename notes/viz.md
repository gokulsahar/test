# Build Prompt: Deterministic Python Code Review Visualizer

## Goal

Build a Flask + React web app that lets a developer visually explore and review a single Python file — either a static file on a branch, or a file changed in a PR — without reading it top to bottom. All analysis is static (`ast`-based), deterministic, and reproducible. **No LLM is used anywhere in the pipeline** — not for summarization, not for review, not for explanations. Every fact shown in the UI must be traceable to a static-analysis rule.

The core interaction: view a file as an interactive call graph, click any node to inspect it, and click any import/call that points into another file to drill into that file too — building a navigable, cross-file exploration of the codebase instead of a flat script reading.

---

## Two Entry Modes (shared engine, different inputs)

The app always analyzes **exactly one file at a time**. There are two ways to select that file:

1. **Branch Mode** — paste a GitHub file URL, or pick repo + branch/ref + path. Analyzes that file as a single snapshot at that ref.
2. **PR Mode** — paste a PR URL. App fetches PR metadata and shows a picker of every changed `.py` file (with +/- line counts, sorted by change size). Reviewer selects one file, and the app shows a **base-vs-head structural diff** for that file instead of a static snapshot.

Both modes feed the same `ast` analysis engine and the same graph/panel/checklist UI. The only difference is single-snapshot vs. before/after diff.

**Cross-file drill-down works identically in both modes.** When a reviewer clicks into an imported file:
- In Branch Mode, the target file is fetched at the same branch/ref as the current file.
- In PR Mode, the target file is fetched at the **PR's head SHA** (i.e., the codebase as it looks *after* the PR's changes) — and from that point on, the reviewer is browsing in ordinary single-snapshot Branch Mode for that file, since only the originally-selected PR file gets a base-vs-head diff view.

---

## Core Analysis Engine (Python `ast` only — no third-party AI/LLM libraries)

For any given file content, deterministically extract:

1. **Structure** — classes (base classes, decorators), functions/methods (decorators, args, return type hints), nested functions, module-level constants, imports (split stdlib / third-party / internal).
2. **Call graph** — which function calls which, within the file. Resolve `self.method()` calls. Tag calls to stdlib/external functions separately from internal calls.
3. **Entry points** — functions never called elsewhere in the file (public API candidates), using naming conventions, framework decorators (e.g. `@app.route`), and `if __name__ == "__main__"` as signals.
4. **Side-effect tags** — file I/O, network calls, subprocess/OS execution, DB access, env var reads, non-determinism sources (`random`, `datetime.now`, `uuid`) — matched deterministically against import usage.
5. **Complexity** — line count, cyclomatic complexity, max nesting depth, argument count, per function.
6. **Exception surface** — what's raised and caught per function, flagging bare `except:`.
7. **Dead code candidates** — functions/classes defined but never referenced in the file.
8. **Docstring / type-hint coverage** — per function and class.
9. **Import resolution** — for every import, resolve to either an internal repo-relative file path (handling relative imports, absolute imports, and `__init__.py` package boundaries) or mark as external. Unresolvable internal-looking imports fall back to being treated as external/opaque rather than erroring.

---

## Cross-File Navigation

- Clicking a node/edge that resolves to an internal file not yet loaded fetches and parses it on demand (same engine, same caching), then pushes it onto a **navigation stack**.
- A breadcrumb bar (`file_a.py > utils.py > db.py`) and Back button let the reviewer retrace steps, preserving each file's graph state (zoom, filters, expanded nodes).
- Cross-file edges are visually distinct from intra-file edges (different color/style, file-path shown on hover before clicking).
- Optional **merged view**: combine all files visited in the session into one graph with subtle per-file grouping, for reviewers who want the full cross-file chain at once.
- Guardrails: only resolve files within the same repo + ref as the current session; never crawl into other repos; cap auto-fetch depth and require explicit clicks to drill further rather than auto-expanding everything.

---

## PR Mode Specifics

1. **PR ingestion** — given a PR URL or `org/repo#123`, pull title, description, author, base SHA, head SHA, and the full changed-files list via the GitHub API.
2. **File picker (required step)** — list changed files, `.py` files clickable, others shown but disabled. Each row shows path, +/- line counts, and status (added/modified/removed). Default sort: most lines changed first. Reviewer picks exactly one file.
3. **Structural diff** — fetch the file at base SHA and head SHA (exact commits, not moving branch tips), run the `ast` engine on both, and diff the structures: added / removed / modified (signature, body, complexity, or side-effects changed) / unchanged functions and classes.
4. **Visualization** — same graph as Branch Mode, with change-state coloring layered on top (added = green outline, removed = red/ghosted, modified = amber, unchanged = default). Side panel for modified nodes shows before/after source side-by-side. Checklist adapts to PR context (e.g. "removed function still referenced elsewhere at head").
5. **Cross-PR-file flags** — if a changed file references another file also changed in the same PR, flag it as a badge in the picker and a note in the side panel. Clicking it switches to that file's own graph via the file switcher — it does not merge the two into one view.
6. A lightweight **file switcher** (not the full picker) stays visible once inside a file's graph for one-click swaps between changed files in the same PR.

---

## Interactive Visualization

- **Graph** (Cytoscape.js): one node per function/class, edges = calls. Node color = side-effect category, node size = complexity, edge style distinguishes internal vs. cross-file vs. external/stdlib calls.
- **Click a node** → side panel with syntax-highlighted source, callers/callees (clickable), side-effect tags, complexity, docstring/type-hint status, exceptions, and (if applicable) an "Open file" button for cross-file drill-down.
- **Hover** → highlight direct neighbors, dim the rest.
- **Filters** → hide stdlib/external calls, isolate subgraph from a chosen entry point, highlight top-N complexity, highlight missing docstrings, highlight side-effects without error handling, search by name.
- **Class view toggle** → UML-style diagram for class-heavy files.
- Large graphs (100+ functions) stay smooth: canvas rendering, collapse-by-default with expand-on-demand rather than rendering everything at once.

---

## Review Aids (rule-based, not generated text)

- **Auto-checklist**, computed from the analysis engine, e.g.: functions with no docstring, functions over N lines, side-effect calls with no error handling, high-complexity functions, dead code candidates, bare `except:` clauses. Each item links to its graph node.
- **Import/dependency panel** — stdlib / third-party / internal, grouped with usage counts.
- **Complexity leaderboard** — sortable table, click-through to graph.
- **Export** — static HTML or JSON snapshot of the current analysis (and, in merged-view sessions, all files visited), shareable without the live app.

---

## Visual Design & UX (required, not a later pass)

- Dark-mode-first, three-pane layout: left = navigation tree/breadcrumbs, center = graph canvas, right = collapsible detail panel.
- Smooth animated graph transitions on re-layout, filter, or navigation; zoom/pan with inertia; minimap for large graphs.
- Consistent color language for side-effect categories and a complexity gradient (green → yellow → red), reused across graph, checklist, and filters — paired with icons/labels, not color alone.
- Transparent loading states ("fetching file_b.py…", "parsing 340 lines…"); a "⚡ cached" badge on cache hits.
- Syntax-highlighted code snippets (Shiki or Prism) in the side panel; collapsible caller/callee/exception sections.
- Checklist and complexity items are clickable and pulse-highlight the relevant graph node on jump.
- Breadcrumb navigation styled like browser tabs/history.
- Clean, modern aesthetic (Linear/Vercel/GitHub-style) — generous whitespace, monospace for code, sans-serif for UI chrome, subtle depth rather than heavy borders.

---

## Tech Stack

- **Backend:** Python 3.11+, Flask
- **Analysis:** Python's built-in `ast` module only — no LLM or AI libraries anywhere
- **Frontend:** React (Vite) + Tailwind CSS, Cytoscape.js for the graph, Shiki or Prism for syntax highlighting
- **Storage:** SQLite or filesystem JSON cache, keyed by `repo + ref + path + commit_sha`
- **GitHub access:** REST API (Contents API + Pulls API), PAT stored server-side only, never exposed to the frontend

---

## API Endpoints

- `POST /api/fetch` — `{repo, ref, path}` or `{raw_code}` → file content + commit SHA
- `POST /api/analyze` — `{content, cache_key}` → `{nodes, edges, checklist, imports, complexity_table}`
- `POST /api/resolve-import` — `{repo, ref, current_path, import_statement}` → internal file path or external tag
- `POST /api/pr` — `{repo, pr_number}` → PR metadata + changed `.py` files list
- `POST /api/pr/analyze-file` — `{repo, pr_number, path}` → base/head analysis + structural diff
- `GET /api/cache/:key` — cached analysis if present
- `GET /api/export/:key` — static HTML/JSON export

---

## Non-Goals

- No LLM calls anywhere in the pipeline (source docstrings can be displayed, never generated or paraphrased).
- Python only for v1 (`ast`-based); no other languages.
- No commenting/collaboration system for v1.
- No generic arbitrary-ref diff tool — PR Mode only needs the PR's own base/head SHAs.

---

## Deliverables

1. Flask app implementing the endpoints above.
2. React frontend implementing the graph, panels, filters, checklist, and PR picker/switcher described above.
3. `README.md` covering setup (PAT env var), architecture, and how the deterministic analysis works.
4. Test suite for the `ast` analysis engine — fixture files with known structure, asserting exact expected output (structure, call graph, checklist).
5. Test suite for import resolution — fixture package with relative/absolute imports, `__init__.py` boundaries, and one unresolvable case, asserting correct internal/external classification.
6. Test suite for PR structural diffing — fixture base/head file pairs with known added/removed/modified functions, asserting exact diff output.
