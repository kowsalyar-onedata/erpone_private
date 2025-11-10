import requests
import pandas as pd
import time
import json
from datetime import datetime
 
# 🔐 YouTrack Token & Base URL
TOKEN = "perm-S293c2FseWFmUmFuZ2FuYXRoYW4=.NDYtNw==.haJ24t87toIKzOy98mwp2wxcT8k3hX"
BASE_URL = "https://youtrack24.onedatasoftware.com/api/issues"
 
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}
 
# 🧠 Helper: Extract field value safely
def get_value(field_list, field_name):
    for f in field_list:
        if f.get("name") == field_name:
            val = f.get("value")
            if val is None:
                return None
            if isinstance(val, dict):
                return (
                    val.get("name")
                    or val.get("value")
                    or val.get("presentation")
                    or val.get("minutes")
                )
            if isinstance(val, list):
                names = [v.get("name") for v in val if isinstance(v, dict) and v.get("name")]
                return ", ".join(names)
            return val
    return None
 
# 🧠 Helper: Extract sprints as JSON array string
def get_sprints_array(field_list):
    for f in field_list:
        if f.get("name") == "Sprints":
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
        f"assignee(login,fullName),"  # Root-level assignee
        f"customFields(name,value(name,value,presentation,minutes)),"
        f"tags(name),"
        f"links(direction,linkType(name),issues(idReadable,summary))"
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
 
    if len(data) < limit or skip / limit > max_pages:
        print("✅ Final page reached.")
        break
 
    time.sleep(0.2)
 
print(f"\n✅ Finished! Total ERPOne issues fetched: {len(all_issues)}")
 
# 🧮 Flatten JSON to tabular rows
rows = []
for issue in all_issues:
    custom_fields = issue.get("customFields", [])
 
    # 🔗 Links (all types)
    link_directions, link_types, link_ids, link_summaries = [], [], [], []
    for link in issue.get("links", []):
        direction = link.get("direction", "")
        link_type = link.get("linkType", {}).get("name", "")
        for li in link.get("issues", []):
            link_directions.append(direction)
            link_types.append(link_type)
            link_ids.append(li.get("idReadable", ""))
            link_summaries.append(li.get("summary", ""))
 
    # ✅ Assignee handling (root-level OR custom field)
    assignee_name = issue.get("assignee", {}).get("fullName")
    if not assignee_name:
        assignee_name = get_value(custom_fields, "Assignee")
 
    # 🧩 Build Data Row
    row = {
        "Issue_ID": issue.get("idReadable"),
        "Summary": issue.get("summary"),
        "Description": issue.get("description"),
        "Created": issue.get("created"),
        "Updated": issue.get("updated"),
        "Resolved": issue.get("resolved"),
        "Project": issue.get("project", {}).get("name"),
        "Reporter": issue.get("reporter", {}).get("fullName"),
        "Assignee": assignee_name,  # ✅ Updated logic
 
        # Custom Fields
        "State": get_value(custom_fields, "State"),
        "Type": get_value(custom_fields, "Type"),
        "Priority": get_value(custom_fields, "Priority"),
        "Reviewer": get_value(custom_fields, "Reviewer"),
        "Assigned_By": get_value(custom_fields, "Assigned By"),
        "Assistance": get_value(custom_fields, "Assistance"),
        "Prioritised_By": get_value(custom_fields, "Prioritised by"),
        "Due_Date": get_value(custom_fields, "Due Date"),
        "Output_Link": get_value(custom_fields, "Output Link"),
        "Ideal_Days": get_value(custom_fields, "Ideal days"),
        "Original_Estimation": get_value(custom_fields, "Original estimation"),
        "Screenshot": get_value(custom_fields, "Screenshot"),
        "Story_Points": get_value(custom_fields, "Story points"),
        "Estimation": get_value(custom_fields, "Estimation"),
        "Sprints": get_sprints_array(custom_fields),
        "Work_Types": get_value(custom_fields, "Work Types"),
        "Area_Used": get_value(custom_fields, "Area Used"),
        "Spent_Time": get_value(custom_fields, "Spent time"),
        "Tags": ", ".join([t.get("name") for t in issue.get("tags", []) if t.get("name")]),
 
        # 🔗 Linked Issues
        "Link_Direction": json.dumps(link_directions),
        "Link_Type": json.dumps(link_types),
        "Linked_Issue_ID": json.dumps(link_ids),
        "Linked_Issue_Summary": json.dumps(link_summaries),
    }
 
    rows.append(row)
 
# 🧾 Convert to DataFrame
df = pd.DataFrame(rows)
 
# 🕒 Convert timestamps (YouTrack uses milliseconds)
for col in ["Created", "Updated", "Resolved", "Due_Date"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")
 
# 💾 Save results
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_name = f"YouTrack_ERPOne_Data_{timestamp}.csv"
df.to_csv(csv_name, index=False, encoding="utf-8-sig")
 
print("\n✅ All ERPOne issue data saved to:")
print(csv_name)