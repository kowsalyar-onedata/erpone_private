import requests
import pandas as pd
import time
import json
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# =======================
# 🔐 YouTrack Configuration
# =======================
TOKEN = "perm-S293c2FseWFmUmFuZ2FuYXRoYW4=.NDYtNw==.haJ24t87toIKzOy98mwp2wxcT8k3hX"
BASE_URL = "https://youtrack24.onedatasoftware.com/api/issues"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

# =======================
# 🧠 Helper Functions
# =======================
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

# =======================
# 🧩 Fetch Issues with Pagination
# =======================
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

# =======================
# 🧮 Flatten JSON to DataFrame
# =======================
rows = []
for issue in all_issues:
    custom_fields = issue.get("customFields", [])

    # 🔗 Links
    link_directions, link_types, link_ids, link_summaries = [], [], [], []
    for link in issue.get("links", []):
        direction = link.get("direction", "")
        link_type = link.get("linkType", {}).get("name", "")
        for li in link.get("issues", []):
            link_directions.append(direction)
            link_types.append(link_type)
            link_ids.append(li.get("idReadable", ""))
            link_summaries.append(li.get("summary", ""))

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
        # Custom Fields
        "State": get_value(custom_fields, "State"),
        "Type": get_value(custom_fields, "Type"),
        "Priority": get_value(custom_fields, "Priority"),
        "Reviewer": get_value(custom_fields, "Reviewer"),
        "Assigned By": get_value(custom_fields, "Assigned By"),
        "Assistance": get_value(custom_fields, "Assistance"),
        "Prioritised by": get_value(custom_fields, "Prioritised by"),
        "Due Date": get_value(custom_fields, "Due Date"),
        "Output Link": get_value(custom_fields, "Output Link"),
        "Ideal days": get_value(custom_fields, "Ideal days"),
        "Original estimation": get_value(custom_fields, "Original estimation"),
        "Screenshot": get_value(custom_fields, "Screenshot"),
        "Story points": get_value(custom_fields, "Story points"),
        "Estimation": get_value(custom_fields, "Estimation"),
        "Sprints": get_sprints_array(custom_fields),
        "Work Types": get_value(custom_fields, "Work Types"),
        "Area Used": get_value(custom_fields, "Area Used"),
        "Spent Time": get_value(custom_fields, "Spent time"),
        "Tags": ", ".join([t.get("name") for t in issue.get("tags", []) if t.get("name")]),
        # Linked Issues
        "Link Direction": json.dumps(link_directions),
        "Link Type": json.dumps(link_types),
        "Linked Issue ID": json.dumps(link_ids),
        "Linked Issue Summary": json.dumps(link_summaries),
    }

    rows.append(row)

df = pd.DataFrame(rows)

# Convert timestamps
for col in ["Created", "Updated", "Resolved", "Due Date"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")

# =======================
# 💾 Save CSV Locally
# =======================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_name = f"youtrack_erpo_full_data_{timestamp}.csv"
df.to_csv(csv_name, index=False, encoding="utf-8-sig")
print(f"\n✅ CSV saved locally: {csv_name}")

# =======================
# ☁️ Upload CSV to AWS S3
# =======================
AWS_ACCESS_KEY = "YOUR_ACCESS_KEY_ID"
AWS_SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"
BUCKET_NAME = "my-youtrack-data"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def upload_to_s3(local_file, s3_file):
    try:
        s3.upload_file(local_file, BUCKET_NAME, s3_file)
        print(f"✅ File uploaded to S3: {s3_file}")
    except FileNotFoundError:
        print("❌ The file was not found")
    except NoCredentialsError:
        print("❌ AWS credentials not available")

# Upload automatically after CSV creation
s3_key = f"youtrack_exports/{csv_name}"
upload_to_s3(csv_name, s3_key)
