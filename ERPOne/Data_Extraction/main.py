import requests
import pandas as pd
import time
import json
from datetime import datetime

# 🔐 Token & Base URL
TOKEN = "perm-S293c2FseWFmUmFuZ2FuYXRoYW4=.NDYtNw==.haJ24t87toIKzOy98mwp2wxcT8k3hX"
BASE_URL = "https://youtrack24.onedatasoftware.com/api/issues"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

# 🧠 Helper function to safely extract single field values
def get_value(field_list, field_name):
    """Return the string or numeric value of a custom field."""
    for f in field_list:
        if f.get("name") == field_name:
            val = f.get("value")
            if isinstance(val, dict):
                return val.get("name") or val.get("value") or val.get("presentation")
            return val
    return None

# 🧠 Helper to extract multiple sprints as JSON-style array string
def get_sprints_array(field_list):
    """Return sprints as a JSON array string (e.g. '["Sprint 12", "Sprint 13"]')."""
    for f in field_list:
        if f.get("name") == "Sprints":  # YouTrack field name is 'Sprints'
            val = f.get("value")
            if isinstance(val, list):
                sprints = [v.get("name") for v in val if isinstance(v, dict) and v.get("name")]
                return json.dumps(sprints, ensure_ascii=False)
            elif isinstance(val, dict):
                return json.dumps([val.get("name")], ensure_ascii=False)
    return json.dumps([])

# 🧩 Pagination setup
limit = 100
skip = 0
max_pages = 2000
all_issues = []
seen_ids = set()

print("⏳ Fetching all ERPOne issues from YouTrack...")

while True:
    url = (
        f"{BASE_URL}?query=project:{{ERPOne}}"
        f"&fields=id,idReadable,summary,description,created,updated,resolved,"
        f"project(id,name,shortName),"
        f"reporter(login,fullName),"
        f"assignee(login,fullName),"
        f"customFields(name,value(name,value,presentation,minutes)),"
        f"tags(name),"
        f"links(direction,linkType(name),issues(idReadable,summary)),"
        f"workItems(author(login,fullName),duration(minutes,presentation),text,created)"
        f"&$top={limit}&$skip={skip}"
    )

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    if not data:
        print("✅ No more issues found — stopping pagination.")
        break

    new_issues = [d for d in data if d.get("id") not in seen_ids]
    if not new_issues:
        print("⚠️ Duplicate page detected — stopping to prevent infinite loop.")
        break

    for issue in new_issues:
        seen_ids.add(issue.get("id"))

    all_issues.extend(new_issues)
    skip += limit

    print(f"📦 Total fetched so far: {len(all_issues)}")

    if len(data) < limit:
        print("✅ Final page reached.")
        break

    if skip / limit > max_pages:
        print("⚠️ Safety stop — too many pages (possible pagination bug).")
        break

    time.sleep(0.2)

print(f"\n✅ Finished! Total ERPOne issues fetched: {len(all_issues)}")


# 🧮 Flatten JSON to tabular rows
rows = []
for issue in all_issues:
    custom_fields = issue.get("customFields", [])

    # 🔗 Link info
    link_directions, link_types, link_ids, link_summaries = [], [], [], []
    for link in issue.get("links", []):
        direction = link.get("direction", "")
        link_type = link.get("linkType", {}).get("name", "")
        for li in link.get("issues", []):
            link_directions.append(direction)
            link_types.append(link_type)
            link_ids.append(li.get("idReadable", ""))
            link_summaries.append(li.get("summary", ""))

    # 🕓 Work items (spent time)
    work_items_data = []
    total_minutes = 0
    for wi in issue.get("workItems", []):
        author = wi.get("author", {}).get("fullName", "")
        minutes = wi.get("duration", {}).get("minutes", 0)
        total_minutes += minutes
        text = wi.get("text", "")
        work_items_data.append(f"{author} ({minutes} min) - {text}")

    # 🧩 Construct each row
    row = {
        "Issue ID": issue.get("idReadable"),
        "Summary": issue.get("summary"),
        "Description": issue.get("description"),
        "Created": issue.get("created"),
        "Updated": issue.get("updated"),
        "Resolved": issue.get("resolved"),
        "Project": issue.get("project", {}).get("name"),
        "Reporter": issue.get("reporter", {}).get("fullName"),
        "Assignee": issue.get("assignee", {}).get("fullName"),
        # 🧩 Custom fields
        "State": get_value(custom_fields, "State"),
        "Type": get_value(custom_fields, "Type"),
        "Priority": get_value(custom_fields, "Priority"),
        "Estimation": get_value(custom_fields, "Estimation"),
        "Sprints": get_sprints_array(custom_fields),  # JSON array format
        "Work Type": get_value(custom_fields, "Work Type"),
        "Area Type": get_value(custom_fields, "Area Type"),
        "Tags": ", ".join([t.get("name") for t in issue.get("tags", []) if t.get("name")]),  # Plain text
        # 🕓 Work logging
        "Work Logged (min)": total_minutes,
        "Spent Time (hrs)": round(total_minutes / 60, 2),
        "Work Items Detail": " | ".join(work_items_data),
        # 🔗 Link details (array format)
        "Link Direction": json.dumps(link_directions),
        "Link Type": json.dumps(link_types),
        "Linked Issue ID": json.dumps(link_ids),
        "Linked Issue Summary": json.dumps(link_summaries),
    }

    rows.append(row)


# 🧾 Convert to DataFrame
df = pd.DataFrame(rows)

print(f"🧩 Columns found in DataFrame: {list(df.columns)}")

# 🕒 Convert timestamps (YouTrack uses milliseconds)
for col in ["Created", "Updated", "Resolved"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")

print("\n📋 Data preview:")
print(df.head())

# 💾 Save results
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_name = f"youtrack_erpo_full_data_{timestamp}.csv"

df.to_csv(csv_name, index=False, encoding="utf-8-sig")
print(f"✅ All ERPOne issue data saved to '{csv_name}'")
