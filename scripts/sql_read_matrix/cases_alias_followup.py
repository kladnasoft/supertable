from collections import Counter
from dataclasses import replace

from .cases_temporal import build_cases as temporal_cases
from .models import QueryCase


def build_cases(data):
    cases = []
    for case in temporal_cases(data):
        if case.case_id not in {"temporal_date_aggregate_year", "temporal_timestamp_aggregate_year"}:
            continue
        name = "date" if "_date_" in case.case_id else "timestamp"
        column = "event_date" if name == "date" else "event_time"
        for variant, expression in (("expression", f"EXTRACT(year FROM {column})"), ("ordinal", "1")):
            cases.append(replace(case, case_id=f"alias_{name}_{variant}", category="group_alias_followup",
                sql=case.sql.replace("GROUP BY yr", "GROUP BY " + expression)))
        cases.append(replace(case, case_id=f"alias_{name}_quoted", category="group_alias_followup",
            sql=case.sql.replace(" AS yr", ' AS "Calendar Year"').replace("BY yr", 'BY "Calendar Year"'),
            columns=("Calendar Year", *case.columns[1:])))
    counts = Counter(row["grp"] for row in data["numbers"])
    for name, expression, shift in (("direct", "grp", 0), ("expression", "grp + 1", 1)):
        cases.append(QueryCase(f"alias_number_{name}", "group_alias_followup",
            f"SELECT {expression} AS category, COUNT(*) AS n FROM numbers GROUP BY category ORDER BY category",
            ("category", "n"), [(group + shift, n) for group, n in sorted(counts.items())], ordered=True))
    cases.append(QueryCase("alias_number_rollup", "group_alias_followup",
        "SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY ROLLUP(category) ORDER BY category NULLS LAST",
        ("category", "n"), [*sorted(counts.items()), (None, sum(counts.values()))], ordered=True))
    cases.append(QueryCase("alias_number_physical_collision", "group_alias_followup",
        "SELECT grp + 1 AS grp, COUNT(*) AS n FROM numbers GROUP BY grp ORDER BY grp",
        ("grp", "n"), [(group + 1, n) for group, n in sorted(counts.items())], ordered=True))
    return cases
