import requests
import pandas as pd
import time
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

# =============================================================================
# 🔐 CONFIGURATION
# =============================================================================
TOKEN = "perm-S293c2FseWFmUmFuZ2FuYXRoYW4=.NDYtNw==.haJ24t87toIKzOy98mwp2wxcT8k3hX"
BASE_URL = "https://youtrack24.onedatasoftware.com/api/issues"
PROJECT = "ERPOne"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
}

PAGE_SIZE = 100
MAX_PAGES = 2000
SLEEP_BETWEEN_CALLS = 0.2  # seconds


# =============================================================================
# 🧩 UTILITY FUNCTIONS
# =============================================================================
def get_value(fields: List[Dict[str, Any]], name: str) -> Optional[Any]:
    """Safely extract a named value from YouTrack's custom fields list."""
    for field in fields:
        if field.get("name") == name:
            val = field.get("value")
            if val is None:
                return None

            # Single object
            if isinstance(val, dict):
                return (
                    val.get("name")
                    or val.get("value")
                    or val.get("presentation")
                    or val.get("minutes")
                )

            # Multiple selections
            if isinstance(val, list):
                return ", ".join(
                    v.get("name") for v in val if isinstance(v, dict) and v.get("name")
                )

            return val
    return None


def get_sprints_array(fields: List[Dict[str, Any]]) -> str:
    """Return Sprints field as a JSON array string."""
    for field in fields:
        if field.get("name") == "Sprints":
            val = field.get("value")
            if isinstance(val, list):
                return json.dumps(
                    [v.get("name") for v in val if v.get("name")],
                    ensure_ascii=False,
                )
            elif isinstance(val, dict):
                return json.dumps([val.get("name")], ensure_ascii=False)
    return json.dumps([])


def extract_links(links_data: List[Dict[str, Any]]) -> Dict[str, str]:
    """Extract all issue link details into parallel lists."""
    link_directions, link_types, link_ids, link_summaries = [], [], [], []

    for link in links_data or []:
        direction = link.get("direction", "")
        link_type = link.get("linkType", {}).get("name", "")
        for linked in link.get("issues", []):
            link_directions.append(direction)
            link_types.append(link_type)
            link_ids.append(linked.get("idReadable", ""))
            link_summaries.append(linked.get("summary", ""))

    return {
        "Link_Direction": json.dumps(link_directions, ensure_ascii=False),
        "Link_Type": json.dumps(link_types, ensure_ascii=False),
        "Linked_Issue_ID": json.dumps(link_ids, ensure_ascii=False),
        "Linked_Issue_Summary": json.dumps(link_summaries, ensure_ascii=False),
    }


def extract_workitems(workitems: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract work item details and compute billable totals."""
    all_authors, all_durations, all_texts, all_types, all_created = [], [], [], [], []
    total_minutes = 0
    billable_minutes = 0

    for wi in workitems or []:
        duration = wi.get("duration", {}).get("minutes") or 0
        total_minutes += duration

        # Detect billable work items
        attributes = wi.get("attributes", [])
        is_billable = any(
            (attr.get("name") == "Billable" and attr.get("value", {}).get("name") == "Yes")
            or (attr.get("value", {}).get("name") == "Billable")
            for attr in attributes
        )
        if is_billable:
            billable_minutes += duration

        all_authors.append(wi.get("author", {}).get("fullName", ""))
        all_durations.append(duration)
        all_texts.append(wi.get("text", ""))
        all_types.append(wi.get("type", {}).get("name", ""))
        all_created.append(wi.get("created"))

    return {
        "WorkItem_Authors": json.dumps(all_authors, ensure_ascii=False),
        "WorkItem_Types": json.dumps(all_types, ensure_ascii=False),
        "WorkItem_Texts": json.dumps(all_texts, ensure_ascii=False),
        "WorkItem_Durations_Minutes": json.dumps(all_durations),
        "WorkItem_Created": json.dumps(all_created),
        "Total_WorkItem_Minutes": total_minutes,
        "Billable_WorkItem_Minutes": billable_minutes,
    }


# =============================================================================
# 🔄 FETCHING ISSUES
# =============================================================================
def fetch_all_issues() -> List[Dict[str, Any]]:
    print(f"⏳ Fetching all {PROJECT} issues from YouTrack...")

    all_issues, seen_ids = [], set()
    skip = 0

    while True:
        url = (
            f"{BASE_URL}?query=project:{{{PROJECT}}}"
            f"&fields=id,idReadable,summary,description,created,updated,resolved,"
            f"project(id,name,shortName),"
            f"reporter(login,fullName),"
            f"assignee(login,fullName),"
            f"customFields(name,value(name,value,presentation,minutes)),"
            f"tags(name),"
            f"links(direction,linkType(name),issues(idReadable,summary)),"
            f"workItems(author(login,fullName),duration(minutes,presentation),text,created,type(name),attributes(name,value(name)))"
            f"&$top={PAGE_SIZE}&$skip={skip}"
        )

        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"❌ Error fetching page {skip // PAGE_SIZE + 1}: {e}")
            break

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

        print(f"📦 Total fetched so far: {len(all_issues)}")

        if len(data) < PAGE_SIZE or skip / PAGE_SIZE > MAX_PAGES:
            print("✅ Final page reached.")
            break

        skip += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"\n✅ Finished! Total {PROJECT} issues fetched: {len(all_issues)}")
    return all_issues


# =============================================================================
# 🧮 TRANSFORMATION
# =============================================================================
def transform_issues(issues: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten the list of issues into a clean DataFrame."""
    rows = []

    for issue in issues:
        custom = issue.get("customFields", [])
        assignee = issue.get("assignee", {}).get("fullName") or get_value(custom, "Assignee")

        links = extract_links(issue.get("links"))
        workinfo = extract_workitems(issue.get("workItems"))

        row = {
            "Issue_ID": issue.get("idReadable"),
            "Summary": issue.get("summary"),
            "Description": issue.get("description"),
            "Created": issue.get("created"),
            "Updated": issue.get("updated"),
            "Resolved": issue.get("resolved"),
            "Project": issue.get("project", {}).get("name"),
            "Reporter": issue.get("reporter", {}).get("fullName"),
            "Assignee": assignee,

            # Custom fields
            "State": get_value(custom, "State"),
            "Type": get_value(custom, "Type"),
            "Priority": get_value(custom, "Priority"),
            "Reviewer": get_value(custom, "Reviewer"),
            "Assigned_By": get_value(custom, "Assigned By"),
            "Assistance": get_value(custom, "Assistance"),
            "Prioritised_By": get_value(custom, "Prioritised by"),
            "Due_Date": get_value(custom, "Due Date"),
            "Output_Link": get_value(custom, "Output Link"),
            "Ideal_Days": get_value(custom, "Ideal days"),
            "Original_Estimation": get_value(custom, "Original estimation"),
            "Screenshot": get_value(custom, "Screenshot"),
            "Story_Points": get_value(custom, "Story points"),
            "Estimation": get_value(custom, "Estimation"),
            "Sprints": get_sprints_array(custom),
            "Work_Types": get_value(custom, "Work Types"),
            "Area_Used": get_value(custom, "Area Used"),
            "Spent_Time": get_value(custom, "Spent time"),
            "Tags": ", ".join(
                t.get("name") for t in issue.get("tags", []) if t.get("name")
            ),
            **links,
            **workinfo,
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    # Convert timestamps (YouTrack uses milliseconds)
    for col in ["Created", "Updated", "Resolved", "Due_Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")

    return df


# =============================================================================
# 💾 MAIN EXECUTION
# =============================================================================
def main():
    issues = fetch_all_issues()
    df = transform_issues(issues)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"YouTrack_{PROJECT}_Data_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")

    print(f"\n✅ Export complete! Saved to: {filename}")


# =============================================================================
# 🚀 ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    main()
