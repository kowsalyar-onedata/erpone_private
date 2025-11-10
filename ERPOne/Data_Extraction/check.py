import requests
import pandas as pd
import time
from datetime import datetime

# 🔐 Token & Base URL
TOKEN = "perm-S293c2FseWFmUmFuZ2FuYXRoYW4=.NDYtNw==.haJ24t87toIKzOy98mwp2wxcT8k3hX"
BASE_URL = "https://youtrack24.onedatasoftware.com/api/issues"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

# 🧠 Helper function to safely extract values
def get_value(field_list, field_name):
    """Extract value of a custom field by name."""
    for f in field_list:
        if f.get("name") == field_name:
            val = f.get("value")
            if isinstance(val, dict):
                return val.get("name") or val.get("presentation") or val.get("value")
            return val
    return None


# 🧩 Pagination setup
limit = 100
skip = 0
max_pages = 2000  # safety cap (~200k issues)
all_issues = []
seen_ids = set()

print("⏳ Fetching issues from YouTrack...")

while True:
    # Build URL with correct fields (includes duration presentation)
    url = (
        f"{BASE_URL}?query=project:{{ERPOne}}"
        f"&fields=id,idReadable,summary,description,created,updated,resolved,"
        f"project(id,name,shortName),"
        f"reporter(fullName,login,email),"
        f"assignee(fullName,login,email),"
        f"customFields(name,value(name,value,presentation,minutes)),"
        f"tags(name),"
        f"links(direction,linkType(name),issues(idReadable,summary)),"
        f"workItems(author(fullName),duration(minutes,presentation),text,created)"
        f"&$top={limit}&$skip={skip}"
    )

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    if not data:
        print("No more issues found — stopping pagination.")
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

    time.sleep(0.2)  # Gentle rate limit pause


print(f"\n✅ Finished! Total ERPOne issues fetched: {len(all_issues)}")

# 🧮 Flatten JSON into rows
rows = []
for issue in all_issues:
    work_items = issue.get("workItems", [])

    total_minutes = sum([
        wi.get("duration", {}).get("minutes", 0) or 0
        for wi in work_items
    ])

    total_presentation = ", ".join([
        wi.get("duration", {}).get("presentation", "")
        for wi in work_items if wi.get("duration", {}).get("presentation")
    ])

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
        "State": get_value(issue.get("customFields", []), "State"),
        "Type": get_value(issue.get("customFields", []), "Type"),
        "Priority": get_value(issue.get("customFields", []), "Priority"),
        "Estimation": get_value(issue.get("customFields", []), "Estimation"),
        "Sprint": get_value(issue.get("customFields", []), "Sprint"),
        "Work Type": get_value(issue.get("customFields", []), "Work Type"),
        "Tags": ", ".join([
            t.get("name", "") for t in issue.get("tags", []) if t.get("name")
        ]),
        "Comments": " | ".join([
            f"{c['author']['fullName']}: {c.get('text', '').strip()}"
            for c in issue.get("comments", []) if c.get("text")
        ]),
        "Attachments": ", ".join([
            a.get("url", "") for a in issue.get("attachments", []) if a.get("url")
        ]),
        "Work Logged (min)": total_minutes,
        "Spent Time (formatted)": total_presentation if total_presentation else f"{total_minutes} min"
    }
    rows.append(row)

# 🧾 Convert to DataFrame
df = pd.DataFrame(rows)

# 🕒 Convert timestamps
for col in ["Created", "Updated", "Resolved"]:
    df[col] = pd.to_datetime(df[col], errors="coerce")

print("\n📋 Data preview:")
print(df.head())

# 💾 Save to CSV
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_name = f"youtrack_erpo_data_{timestamp}.csv"

df.to_csv(csv_name, index=False, encoding="utf-8-sig")
print(f"✅ All ERPOne issues saved to '{csv_name}'")
