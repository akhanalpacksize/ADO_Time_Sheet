import base64
import csv
from config.env import Devops_token
from utils import safe_request


# -----------------------------------------------------
# Fetch transition dates (Doing/Done)
# -----------------------------------------------------
def get_transition_dates(work_item_id, project, organization, headers):
    url = (
        f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/"
        f"{work_item_id}/revisions?api-version=7.0"
    )

    resp = safe_request("GET", url, headers=headers)
    revisions = resp.json().get("value", [])

    doing_date = None
    done_date = None

    for rev in revisions:
        fields = rev.get("fields", {})
        state = fields.get("System.State")
        changed_date = fields.get("System.ChangedDate")

        if changed_date:
            changed_date = changed_date[:10]

        if state == "Doing" and doing_date is None:
            doing_date = changed_date

        if state == "Done" and done_date is None:
            done_date = changed_date

    return doing_date, done_date


# -----------------------------------------------------
# Batch fetch fields for WI IDs
# -----------------------------------------------------
def get_work_items(ids, project, organization, headers):
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemsbatch?api-version=7.0"

    payload = {
        "ids": ids,
        "fields": [
            "System.Id",
            "System.Title",
            "System.State",
            "System.AssignedTo",
            "System.WorkItemType",
            "Custom.ProductType",
            "Microsoft.VSTS.Scheduling.TargetDate",
        ],
    }

    resp = safe_request("POST", url, headers=headers, json=payload)
    return resp.json().get("value", [])


# -----------------------------------------------------
# Main Export Function
# -----------------------------------------------------
def export_work_items_to_csv(output_file):
    api_token = Devops_token
    organization = "packsize"

    headers = {
        "Authorization": "Basic "
        + base64.b64encode(f":{api_token}".encode()).decode(),
        "Content-Type": "application/json",
    }

    # Team → (Project, Area Path)
    team_area_paths = {
        "Team_NABU_RnD_HW_Sustaining": ("Packsize Product Development", "Team_NABU_RnD_HW_Sustaining"),
        "Team_RnD_Design": ("Packsize Product Development", "Team_RnD_Design"),
        "Team_PLC": ("Packsize Product Development", "Team_PLC"),
        "Team_PLC_TechDebt": ("Packsize Product Development", "Team_PLC_TechDebt"),
        "RnD_X5N": ("RnD_X5N", "RnD_X5N"),  # entire project
    }

    all_details = []

    # -------------------------------------------------
    # Loop all teams
    # -------------------------------------------------
    for team, (proj, area_path) in team_area_paths.items():

        # Validate team settings except for X5N
        if proj != "RnD_X5N":
            team_url = (
                f"https://dev.azure.com/{organization}/{proj}/{team}/"
                "_apis/work/teamsettings?api-version=7.0"
            )
            try:
                safe_request("GET", team_url, headers=headers)
            except Exception as e:
                print(f"Team settings error for {team}: {e}")
                continue

        # -------------------------------------------------
        # Build WIQL
        # -------------------------------------------------
        if proj == "RnD_X5N":
            wiql = {
                "query": f"SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = '{proj}'"
            }
            wiql_url = f"https://dev.azure.com/{organization}/_apis/wit/wiql?api-version=7.0"

        else:
            wiql = {
                "query": f"""
                    SELECT [System.Id] FROM WorkItems
                    WHERE [System.TeamProject] = '{proj}'
                    AND [System.AreaPath] UNDER '{proj}\\{area_path}'
                """
            }
            wiql_url = f"https://dev.azure.com/{organization}/{proj}/_apis/wit/wiql?api-version=7.0"

        # -------------------------------------------------
        # Execute WIQL
        # -------------------------------------------------
        response = None
        try:
            response = safe_request("POST", wiql_url, headers=headers, json=wiql)
            work_item_ids = [item["id"] for item in response.json().get("workItems", [])]
        except Exception as e:
            error_text = response.text if response else "<no response>"
            print(f"\nWIQL failed for {proj}: {e}\nResponse: {error_text}\n")
            continue

        print(f"{team}: Found {len(work_item_ids)} work items")

        # -------------------------------------------------
        # Batch fetch work items
        # -------------------------------------------------
        details = []
        for i in range(0, len(work_item_ids), 200):
            batch = work_item_ids[i : i + 200]
            details.extend(get_work_items(batch, proj, organization, headers))

        # -------------------------------------------------
        # Compute transition dates & apply filters
        # -------------------------------------------------
        for item in details:
            wid = item["id"]

            doing, done = get_transition_dates(wid, proj, organization, headers)

            item["DoingDate"] = doing
            item["DoneDate"] = done
            item["Team"] = team

            state = item["fields"].get("System.State")
            done_date = done

            # Skip migration junk
            if done_date:
                if (state == "Done" and done_date == "2025-09-10") or (done_date < "2025-01-01"):
                    continue

            all_details.append(item)

    # -----------------------------------------------------
    # Write CSV
    # -----------------------------------------------------
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ID",
                "Title",
                "AssignedTo",
                "State",
                "Type",
                "ProductType",
                "TargetDate",
                "DoingDate",
                "DoneDate",
                "Team",
            ],
        )
        writer.writeheader()

        for item in all_details:
            fields = item["fields"]
            assigned = fields.get("System.AssignedTo", {})
            assigned_name = assigned.get("displayName") if isinstance(assigned, dict) else ""

            target = fields.get("Microsoft.VSTS.Scheduling.TargetDate")
            target = target[:10] if target else ""

            writer.writerow({
                "ID": fields["System.Id"],
                "Title": fields["System.Title"],
                "AssignedTo": assigned_name,
                "State": fields["System.State"],
                "Type": fields["System.WorkItemType"],
                "ProductType": fields.get("Custom.ProductType", ""),
                "TargetDate": target,
                "DoingDate": item.get("DoingDate", ""),
                "DoneDate": item.get("DoneDate", ""),
                "Team": item.get("Team", ""),
            })

    print(f"\n✅ CSV exported successfully → {output_file}")


# Run
# export_work_items_to_csv("work_items.csv")
