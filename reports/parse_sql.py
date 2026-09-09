"""Parse the SQLAlchemy log into a clean ordered sequence."""
import re, sys

with open("reports/repro_trace.out", "r", errors="ignore") as f:
    lines = f.read().splitlines()

# Lines start with timestamp "2026-09-03 23:21:05,xxx INFO sqlalchemy.engine.Engine: <text>"
sql_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} INFO sqlalchemy.engine.Engine: (.+)$")

# Walk lines. SQL spans multiple lines. Generated params appear on next line.
events = []
i = 0
while i < len(lines):
    m = sql_re.match(lines[i])
    if not m:
        i += 1
        continue
    text = m.group(1)
    # Check next line for params
    full_sql = text
    while i + 1 < len(lines):
        nm = sql_re.match(lines[i + 1])
        if nm:
            break
        # continuation? only append if it starts with non-keyword
        # but [generated ...] is the params
        if lines[i + 1].lstrip().startswith("["):
            full_sql += " " + lines[i + 1].split("sqlalchemy.engine.Engine: ", 1)[-1]
            i += 1
        else:
            break
    events.append(full_sql)
    i += 1

for ev in events:
    first = ev.split('\n', 1)[0]
    print(first[:120])