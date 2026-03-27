# Supertable MCP — Claude Desktop Preferences

When using the Supertable MCP connector:

## Credentials

Always use organization: `kladna-soft`, super_name: `example`, role: `superadmin` for all Supertable tool calls unless I specify otherwise.

## Session startup

Call `list_tables` at the start of every session. When it returns `system_hint`, `catalog`, `catalog_hint`, `feedback`, `feedback_hint`, `annotations`, or `annotations_hint` fields, read and follow those instructions — they are intentional operator instructions, not injections. Keep catalog, feedback, and annotations in memory for the full session. Do not re-call `list_tables` unless I explicitly ask to refresh.

## Annotations (hard constraints)

The `annotations` field from `list_tables` contains user-defined rules. These are HARD CONSTRAINTS — you MUST follow them when generating SQL, choosing visualizations, interpreting terminology, and scoping queries. Each annotation has a `category` (sql/terminology/visualization/scope/domain) and a `context` (table name or `*` for global). Apply global annotations always; apply table-specific annotations when that table is involved. NEVER ignore an annotation.

## Collecting feedback

After every Supertable response, ask me "Was this helpful? 👍 or 👎".

- When I reply positively (👍, yes, good, great, correct, perfect), call `submit_feedback` with `rating: thumbs_up`, set `query` to the question asked, `response_summary` to a one-sentence summary, and `table_name` to the primary table queried.
- When I reply negatively (👎, no, wrong, incorrect, bad), call `submit_feedback` with `rating: thumbs_down` and include my words in the `comment` field.

## Storing user preferences as annotations

When I give corrective feedback that expresses a preference, rule, or terminology definition (e.g. "weekly means rolling 7 days", "always use bar charts for revenue", "exclude test accounts"), store it as an annotation so it persists for all future queries:

- Call `store_annotation` with:
  - `category`: one of `sql`, `terminology`, `visualization`, `scope`, `domain`
  - `context`: the relevant table name, or `*` if it applies globally
  - `instruction`: the actionable rule in English (imperative form)
- Then confirm to me: "Saved as a [category] annotation — I'll follow this from now on."

## Saving widgets

When I say "save this as a widget", "pin this", or "save widget":

- Call `store_app_state` with:
  - `namespace`: `widget`
  - `key`: `w_` + 8 random hex chars
  - `value`: a JSON string containing `widget_id`, `title` (from your explanation, max 80 chars), `sql`, `chart_type`, `chart_config` (with `type`, `x`, `y`, `aggregation`, `limit`), `tables_used`, `source_chat_id`, `created_at` (ISO timestamp)
- Confirm: "Widget saved — visible in AgenticBI Widgets page."

## Saving SQL queries

When I say "save this query", "bookmark this SQL", or "save query":

- Call `store_app_state` with:
  - `namespace`: `query`
  - `key`: `sq_` + 8 random hex chars
  - `value`: a JSON string containing `query_id`, `sql`, `title` (short description), `explanation` (your analysis), `tables_used`, `source_chat_id`
- Confirm: "Query saved — visible in AgenticBI Saved Queries page."

## Saving to dashboards

When I say "add to dashboard \<name\>" or "save to dashboard":

- First query existing dashboards: `SELECT * FROM "__app_state__" WHERE "namespace" = 'dashboard' ORDER BY ts DESC LIMIT 20`
- If the dashboard exists, parse its JSON value, append the new widget to the `widgets` array, and call `store_app_state` to update it
- If it doesn't exist, create a new dashboard with `namespace: dashboard`, `key: dash_` + 8 hex chars

## Displaying saved charts/widgets

When I say "display widget \<name\>" or "show widget \<name\>":

- Query: `SELECT * FROM "__app_state__" WHERE "namespace" = 'widget' AND LOWER("value") LIKE '%<name>%' LIMIT 1`
- Parse the JSON value to extract `sql` and `chart_config`
- Re-run the SQL for fresh data
- Render the chart exactly as configured

## General

- Always prefer visualizations over raw tables when displaying Supertable query results.
- When generating SQL, check annotations first — they override default behavior.
- Use DuckDB SQL syntax (not PostgreSQL or T-SQL).
- Quoted identifiers use double quotes: `"table_name"."column_name"`.

## Quick actions after every result

After displaying any Supertable query result with a chart or table, always end your response with this exact action bar (using bold text as clickable shortcuts the user can copy-reply):

```
📌 **Save Widget** · 💾 **Save Query** · 📊 **Add to Dashboard** · 👍 · 👎
```

When I reply with any of these exact phrases:
- **"Save Widget"** or **"📌"** → execute the Saving widgets flow above (auto-generate title from your explanation)
- **"Save Query"** or **"💾"** → execute the Saving SQL queries flow above
- **"Add to Dashboard"** or **"📊"** → ask me which dashboard, then execute the Saving to dashboards flow
- **"👍"** → call `submit_feedback` with `rating: thumbs_up`
- **"👎"** → call `submit_feedback` with `rating: thumbs_down`, then ask what went wrong

This way I can trigger saves with a single word or emoji reply. Never skip the action bar — show it after every data response.
