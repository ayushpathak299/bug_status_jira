import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from dateutil import parser
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
load_dotenv()
# ----------------------------
# Configurations
# ----------------------------

JIRA_URL = os.getenv("JIRA_URL")
JIRA_USER = os.getenv("JIRA_USER")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
PROJECT_KEY = os.getenv("PROJECT_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

PG_CONN = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# ----------------------------
# Get last ETL run timestamp
# ----------------------------
cursor = PG_CONN.cursor()
cursor.execute("SELECT last_run FROM etl_metadata WHERE etl_name = 'status_etl'")
last_run = cursor.fetchone()[0]

buffered_start_time = datetime.now(timezone.utc) - timedelta(days=2)

now_utc = datetime.now(timezone.utc)

print(f"ETL running from: {buffered_start_time} to {now_utc}")

# ----------------------------
# Tracked Statuses
# ----------------------------
tracked_statuses = [
    "Open", "Moved To Product", "Engg Review In Progress",
    "PST Review In Progress", "Moved To Engg", "Needs More Info"
]

# ----------------------------
# Run ETL
# ----------------------------
start_at = 0
while True:
    jql_time = buffered_start_time.strftime("%Y-%m-%d %H:%M")
    jql = f"project={PROJECT_KEY} AND updated >= '{jql_time}' AND issuetype = 'Bug'"
    url = f"{JIRA_URL}/rest/api/3/search?expand=changelog"
    params = {"jql": jql, "startAt": start_at, "maxResults": 50}
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = HTTPBasicAuth(JIRA_USER, JIRA_API_TOKEN)

    response = requests.get(url, headers=headers, params=params, auth=auth)
    data = response.json()
    issues = data.get("issues", [])
    if not issues:
        break

    for issue in issues:
        issue_key = issue["key"]
        issue_created = parser.parse(issue["fields"]["created"])
        changelogs = sorted(issue.get("changelog", {}).get("histories", []), key=lambda x: parser.parse(x["created"]))

        for history in changelogs:
            change_time = parser.parse(history["created"])
            if not (buffered_start_time <= change_time <= now_utc):
                continue

            for item in history.get("items", []):
                if item.get("field") != "status":
                    continue

                from_status = item.get("fromString")
                to_status = item.get("toString")

                # Step 1: Update exit for from_status
                if from_status in tracked_statuses:
                    cursor.execute("""
                        SELECT id, entered_at FROM issue_status_history
                        WHERE issue_key = %s AND status = %s AND exited_at IS NULL
                        ORDER BY entered_at DESC LIMIT 1
                    """, (issue_key, from_status))
                    result = cursor.fetchone()

                    # Handle missing "Open" entry by using created date
                    if from_status == "Open" and not result:
                        cursor.execute("""
                            INSERT INTO issue_status_history (issue_key, status, entered_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (issue_key, status, entered_at) DO NOTHING
                        """, (issue_key, "Open", issue_created))
                        cursor.execute("""
                            SELECT id, entered_at FROM issue_status_history
                            WHERE issue_key = %s AND status = %s AND exited_at IS NULL
                            ORDER BY entered_at DESC LIMIT 1
                        """, (issue_key, "Open"))
                        result = cursor.fetchone()

                    if result:
                        record_id, entered_at = result
                        if change_time > entered_at.replace(tzinfo=timezone.utc):
                            cursor.execute("""
                                UPDATE issue_status_history
                                SET exited_at = %s,
                                    duration_minutes = EXTRACT(EPOCH FROM (%s - entered_at)) / 60
                                WHERE id = %s
                            """, (change_time, change_time, record_id))

                # Step 2: Insert entry for to_status
                if to_status in tracked_statuses:
                    cursor.execute("""
                        INSERT INTO issue_status_history (issue_key, status, entered_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (issue_key, status, entered_at) DO NOTHING
                    """, (issue_key, to_status, change_time))

        PG_CONN.commit()

    start_at += 50
    if start_at >= data.get("total", 0):
        break

# ----------------------------
# Step 3: Update ETL timestamp
# ----------------------------
cursor.execute("UPDATE etl_metadata SET last_run = %s WHERE etl_name = 'status_etl'", (now_utc,))
PG_CONN.commit()

cursor.close()
PG_CONN.close()

print("ETL completed successfully.")
