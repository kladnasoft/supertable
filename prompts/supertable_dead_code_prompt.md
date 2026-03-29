# Supertable Dead Code Analysis Prompt

> **Usage:** Paste this entire prompt into a new Claude conversation with computer use enabled.
> Replace `<REPO_URL>` with your Git repository URL.
> The AI will clone the repo, analyze everything, and present findings — but will NOT modify any code until you explicitly approve.

---

## PROMPT START

You are a Principal Python Engineer performing a dead code analysis on a production codebase. Your job is to find every function, class, import, file, route, env var, JS function, and HTML template reference that is defined but never used — and report it for human review before touching anything.

### Repository

Clone and analyze this repository:

```
<REPO_URL>
```

The Python package root is `supertable/`.

### Scope Rules

**EXCLUDED directories — ignore completely. References FROM these directories do NOT count as consumers. Do not count them as keeping anything alive:**

- `supertable/studio/*` — deprecated, removed
- `supertable/infrastructure/*` — extracted to separate project
- `**/examples/*` — example code, not production
- `**/__pycache__/*`

**ANALYZED but do NOT keep code alive:**

- `**/tests/*`, `**/test_*` — test files are analyzed for their OWN dead code, but a test being the only consumer of a function does NOT make that function alive. If `test_foo.py` is the only caller of `foo()`, then BOTH are dead.
- `**/*.md` — read for architectural context only, never count as code references
- `**/*.txt`, `**/*.yml`, `**/*.yaml`, `**/*.toml` — config context only

**ANALYZED as production code (references from these DO keep code alive):**

- `**/*.py` — all Python files (excluding the directories above)
- `**/*.js` — all JavaScript files (template scripts, UI logic)
- `**/*.html` — all HTML templates (Jinja2 templates, `{% include %}`, `{% extends %}`, URL references, `fetch()` calls to API endpoints)
- `**/Dockerfile*`, `**/docker-compose*` — entrypoint and service definitions

### What counts as "alive"

A symbol (function, class, variable, route, JS function, CSS class, template) is **alive** if and only if it is referenced by **production code** — meaning `.py`, `.js`, or `.html` files that are NOT in excluded directories and NOT test files.

A symbol is **dead** if:
- Its only consumers are in excluded directories (studio/, infrastructure/, examples/)
- Its only consumers are test files
- It has zero consumers anywhere

A symbol is **test-orphaned** if:
- It is a test file or test function whose subject (the code it tests) is dead
- It tests code that was removed or moved to an excluded directory

### CRITICAL RULES

1. **NEVER modify, delete, or rewrite any file until Phase 3, and only after explicit human approval.**
2. **When in doubt, mark as "uncertain" rather than "dead".** False positives (marking live code as dead) are far worse than false negatives.
3. **Dynamic usage counts.** If a function/variable is accessed via `getattr()`, string-based dispatch, dictionary lookup, or passed as a callback parameter, it is NOT dead.
4. **Route attachment functions that are no-ops are NOT automatically dead** — check if they are called from `routes.py` or similar registries. If they are, mark as STUB.
5. **`# route:` comments on line 1 are metadata, not code** — never remove them.
6. **Jinja2 references count.** If a Python function is called from a `.html` template via `{{ func() }}` or `{% if func() %}`, it is alive.
7. **JavaScript `fetch()` / `XMLHttpRequest` calls to API endpoints count.** If a JS file calls `fetch('/reflection/compute/list')`, the Python route handling that URL is alive.
8. **HTML form `action` attributes count.** If `<form action="/reflection/login">`, the route is alive.

---

## Phase 1 — Build the Model

Execute these steps in order. Show your work. Do not skip steps.

### Step 1.1 — File Discovery

1. Clone the repo.
2. List every file under `supertable/` by type:
   - `.py` files (excluding studio/, infrastructure/, __pycache__/, examples/)
   - `.js` files
   - `.html` files
   - test files (separately — `**/tests/*`, `**/test_*`)
3. Count totals per type.
4. Show the directory tree.

### Step 1.2 — Frontend Asset Map

1. For every `.js` file, extract:
   - All function definitions (`function name(`, `const name =`, `let name =`, arrow functions assigned to names)
   - All `fetch()` / `$.ajax()` / `XMLHttpRequest` calls — extract the URL paths
   - All event handler bindings
2. For every `.html` file, extract:
   - All `{% include %}`, `{% extends %}`, `{% block %}` references
   - All `<script src="...">` references
   - All `<link href="...">` references
   - All `<form action="...">` URLs
   - All `fetch()` calls in inline `<script>` blocks
   - All Jinja2 variable references: `{{ var }}`, `{% if var %}`, `{% for x in var %}`
   - All URL paths referenced (links, fetch, form actions, redirects)
3. Build a map: **URL path → JS/HTML files that call it**
4. Build a map: **Python template variable → HTML files that use it**

### Step 1.3 — Settings Audit

1. Open `supertable/config/settings.py`.
2. List every field in the `Settings` dataclass.
3. For each field, search the entire codebase (excluding studio/, infrastructure/, examples/, tests/) for:
   - `settings.FIELD_NAME`
   - `_cfg.FIELD_NAME`
   - Direct `os.getenv("FIELD_NAME")` that should be using settings instead
4. Produce a table:

```
| Field | Consumers (file:line) | Status |
|-------|----------------------|--------|
| STORAGE_TYPE | storage_factory.py:45 | ALIVE |
| SUPERTABLE_VAULT_FERNET_KEY | (none) | DEAD |
```

### Step 1.4 — Import Graph

1. For every `.py` file, extract all `from supertable.X import Y` statements.
2. For each imported symbol `Y`, check if it is actually used in the importing file (not just imported).
3. Produce a list of unused imports:

```
| File | Unused Import | Can Remove? |
|------|--------------|-------------|
| api/api.py | from supertable.foo import bar | Yes — bar never referenced |
```

### Step 1.5 — Function & Class Liveness

1. For every `.py` file, list every `def` and `class` defined at module level or as a method.
2. For each, search the entire production codebase (`.py`, `.js`, `.html` — excluding studio/, infrastructure/, examples/, tests/) for references.
3. Mark each as:
   - **ALIVE** — called/imported/referenced by production code
   - **DEAD** — zero references in production code
   - **TEST-ONLY** — only referenced in test files (both the function AND its tests are dead candidates)
   - **INTERNAL** — only referenced within the same file (check if its callers within the file are themselves alive)
   - **STUB** — no-op function preserved for backward compatibility, called from routes.py or similar
   - **TEMPLATE-ALIVE** — only referenced from `.html` Jinja2 templates or `.js` files
   - **UNCERTAIN** — dynamic dispatch, callback pattern, or unclear usage

### Step 1.6 — File Liveness

1. For each `.py` file, determine if ANY of its exported symbols are used by production code.
2. If a file's only consumers are excluded directories (studio/, infrastructure/) or test files, mark as **DEAD FILE**.
3. If a file contains only no-op stubs but is imported by `routes.py` or similar, mark as **STUB FILE**.
4. For `.js` files: if no `.html` template includes or references the JS file, mark as **DEAD JS**.
5. For `.html` files: if no Python route renders the template (search for `templates.TemplateResponse("filename.html"` or similar), mark as **DEAD HTML**.

### Step 1.7 — Route Liveness

1. Find all FastAPI route registrations (`@router.get`, `@router.post`, `@router.put`, `@router.delete`, `@router.patch`).
2. For each route, check:
   a. Is the endpoint function reachable from an active entrypoint (`api/application.py`, `reflection/application.py`, `mcp/mcp_server.py`)?
   b. Is the URL path referenced by any `.js` or `.html` file (via `fetch()`, form action, link)?
3. Flag any orphaned routes (registered but never mounted, or mounted but never called from frontend).

### Step 1.8 — Env Var Cross-Reference

1. Find any remaining `os.getenv()` calls with hardcoded env var names (not dynamic/parameterized lookups).
2. Check if these env vars are also in `config/settings.py`.
3. Flag any that should be reading from `settings` but aren't.
4. Flag any `settings` fields that have zero consumers.

### Step 1.9 — Test File Audit

1. For each test file, identify what production code it tests (the subject).
2. If the subject is marked DEAD in Step 1.5, mark the test file as **ORPHANED TEST**.
3. If the test file imports from studio/ or infrastructure/, mark as **ORPHANED TEST** (subject moved/removed).
4. List all orphaned test files.

### Step 1.10 — Template Variable Audit

1. For each `.html` template, list all Jinja2 variables referenced (`{{ var }}`, `{{ var.attr }}`).
2. For each variable, find the Python route that renders this template and check if the variable is passed in the template context dict.
3. Flag any template variables that are referenced in HTML but never passed from Python (dead template vars).
4. Flag any context variables that are passed from Python but never referenced in the HTML (dead context passes).

---

## Phase 2 — Report Findings

Present your findings as a numbered list grouped by category. For each item, state:
- **What:** type (file / function / class / field / import / route / JS function / HTML template / test)
- **Where:** file path and line number
- **Why:** zero references, only studio refs, only test refs, etc.
- **Confidence:** HIGH / MEDIUM / LOW
- **Action:** Remove / Keep as stub / Uncertain

### Report Categories (use these exact headings)

```markdown
### A. Dead Settings Fields
### B. Dead Python Functions & Classes
### C. Dead Python Files
### D. Dead Imports (unused import lines)
### E. Dead Routes (registered but unreachable or uncalled)
### F. Dead JavaScript Functions & Files
### G. Dead HTML Templates
### H. Dead Template Variables
### I. Orphaned Tests (testing dead or removed code)
### J. Remaining Raw os.getenv() (should use settings)
### K. Dead No-Op Stubs (stub files/functions that can be collapsed)
### L. Uncertain (needs human decision)
```

After presenting all categories, ask:

> "Here are my findings. For each numbered item, please reply with:
> - **REMOVE** — I will delete it in Phase 3
> - **KEEP** — I will leave it untouched
> - **SKIP** — I'm not sure, leave it for now
>
> You can also reply with bulk instructions like:
> - 'REMOVE all HIGH confidence items'
> - 'KEEP everything in odata/'
> - 'REMOVE all orphaned tests'
> - 'REMOVE categories A, B, C, D, I — KEEP the rest'"

---

## Phase 3 — Execute Cleanup (ONLY after human approval)

**DO NOT enter Phase 3 until the human has responded to Phase 2 with explicit decisions.**

For each item marked REMOVE by the human:

### Execution Order (mandatory — follow this sequence)

1. **Settings fields first** — remove from dataclass + `_build_settings()` + any `_env_*` helper lines
2. **Dead imports** — remove import lines from consuming files
3. **Dead functions/classes** — remove definitions
4. **Dead routes** — remove `@router.*` decorator + function
5. **Dead files** — remove entire files (only after all imports of them are removed)
6. **Dead JS/HTML** — remove files
7. **Dead tests** — remove test files or test functions
8. **Dead stubs** — remove stub functions + their calls from routes.py

### Per-File Process

For each modified file:
1. Read the current content.
2. Make the minimal change (remove only what was approved).
3. Verify syntax: `python -c "import py_compile; py_compile.compile('path', doraise=True)"`
4. Verify no broken references remain.

### Final Safety Checks

Before delivering the final output:

1. `python -m py_compile` passes on every modified `.py` file.
2. No `settings.REMOVED_FIELD` references remain anywhere.
3. No `from supertable.X import REMOVED_SYMBOL` references remain.
4. No broken import chains (file A imports from file B which was deleted).
5. No `.html` templates reference removed Python variables.
6. No `.js` files call removed API endpoints.
7. Grep for every removed function/class name across all file types (`.py`, `.js`, `.html`) to confirm zero remaining references.

### Change Log

Produce a change log:

```markdown
### Changes Made

| # | Item | File | Change Type | Lines Removed |
|---|------|------|-------------|---------------|
| 1 | SUPERTABLE_VAULT_FERNET_KEY | config/settings.py | Removed field + builder | 4 |
| 3 | _unused_helper() | reflection/common.py | Removed function | 12 |
| 5 | unused import bar | api/api.py | Removed import line | 1 |
| 12 | test_removed_feature.py | tests/test_removed.py | Deleted file | 85 |
| 15 | dead_widget.js | static/js/dead_widget.js | Deleted file | 120 |
| 16 | old_dashboard.html | templates/old_dashboard.html | Deleted file | 200 |

**Total lines removed: ___**
**Total files modified: ___**
**Total files deleted: ___**
```

### Deliverable

Build a zip of the complete modified `supertable/` directory (preserving folder structure) and provide it for download.

## PROMPT END
