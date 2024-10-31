import os
import json
import logging
import csv
from datetime import datetime, timedelta
import pytz
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)


class GoogleCalendarAPI:
    def __init__(
        self,
        calendar_id,
        time_zone,
        scopes,
        token_json_file,
        credentials_json_file,
        max_results=2500,
        dry_run=False,
    ):
        self.calendar_id = calendar_id
        self.time_zone = time_zone
        self.scopes = scopes
        self.token_json_file = token_json_file
        self.credentials_json_file = credentials_json_file
        self.max_results = max_results
        self.dry_run = dry_run
        if not dry_run:
            self.service = self.authenticate()

    def authenticate(self):
        creds = None
        if os.path.exists(self.token_json_file):
            creds = Credentials.from_authorized_user_file(
                self.token_json_file, self.scopes
            )

        # Handle token refresh or re-authentication
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    os.remove(self.token_json_file)  # Remove invalid token
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_json_file, self.scopes
                )
                creds = flow.run_local_server(port=0)
                with open(self.token_json_file, "w") as token:
                    token.write(creds.to_json())

        logger.info("Authenticated with Google Calendar API.")
        return build("calendar", "v3", credentials=creds)

    def fetch_events(self, start_date, end_date):
        if self.dry_run:
            logger.info("Dry run mode: Not fetching remote events.")
            return []

        logger.info(f"Fetching events between {start_date} and {end_date}")
        events_result = (
            self.service.events()
            .list(
                calendarId=self.calendar_id,
                timeMin=start_date.isoformat(),
                timeMax=end_date.isoformat(),
                maxResults=self.max_results,
                singleEvents=True,
                orderBy="startTime",
                timeZone=self.time_zone,
            )
            .execute()
        )
        return events_result.get("items", [])
    
    def prepare_event_data(self, event):
        try:
            # Convert timestamp to date
            date_timestamp = event["date"]
            date = datetime.fromtimestamp(date_timestamp / 1000, pytz.utc)

            # Combine date with start and end times
            start_time = datetime.strptime(event["start_time"], "%H:%M:%S").time()
            end_time = datetime.strptime(event["end_time"], "%H:%M:%S").time()

            # Localize the datetime objects
            local_tz = pytz.timezone(self.time_zone)
            start_datetime = local_tz.localize(datetime.combine(date.date(), start_time))
            end_datetime = local_tz.localize(datetime.combine(date.date(), end_time))

            # Extract course, lecturer, and other details from raw_details
            raw_details = event.get("raw_details", [])
            if not raw_details:
                raise ValueError("raw_details is empty.")

            # Assuming the first element is the course name
            course = raw_details[0].strip()

            # Assuming the second element is the lecturer's name
            lecturer = raw_details[1].strip() if len(raw_details) > 1 else "Unknown Lecturer"

            # Combine any additional details into the description
            additional_details = ", ".join([detail.strip() for detail in raw_details[2:]]) if len(raw_details) > 2 else ""

            # Combine summary and additional details
            summary = f"{course}" + (f" - {additional_details}" if additional_details else "")

            return {
                "summary": summary,
                "location": raw_details[2].strip() if len(raw_details) > 2 else "",
                "start": {
                    "dateTime": start_datetime.isoformat(),
                    "timeZone": self.time_zone,
                },
                "end": {
                    "dateTime": end_datetime.isoformat(),
                    "timeZone": self.time_zone,
                },
                "description": lecturer,
            }
        except Exception as e:
            logger.error(f"Error preparing event data: {event} - {e}")
            return None

    def create_event(self, event):
        event_data = self.prepare_event_data(event)
        if event_data:
            if self.dry_run:
                logger.info(f"Dry run mode: Prepared event data: {event_data}")
                return event_data
            else:
                created_event = (
                    self.service.events()
                    .insert(calendarId=self.calendar_id, body=event_data)
                    .execute()
                )
                logger.info(f'Event created: {created_event["summary"]}')
                return created_event
        else:
            logger.error("Failed to create event due to preparation error.")

    def delete_event(self, event_id):
        if self.dry_run:
            logger.info(
                f"Dry run mode: Would delete event with ID: {event_id}"
            )
        else:
            self.service.events().delete(
                calendarId=self.calendar_id, eventId=event_id
            ).execute()
            logger.info(f"Deleted event with ID: {event_id}")


def create_all_events(calendar_api, local_events):
    created_events = []
    for event in local_events:
        created_event = calendar_api.create_event(event)
        if created_event:
            created_events.append(created_event)
    return created_events


def delete_all_events(calendar_api, time_zone):
    start_date = datetime.now(pytz.timezone(time_zone)) - timedelta(days=300)
    end_date = datetime.now(pytz.timezone(time_zone)) + timedelta(days=300)
    logger.info(f"Fetching events between {start_date} and {end_date}")

    remote_events = calendar_api.fetch_events(start_date, end_date)
    for event in remote_events:
        calendar_api.delete_event(event["id"])
    logger.info("All events deleted successfully.")


def save_events_to_csv(events, filename):
    if not events:
        logger.warning("No events to save.")
        return

    keys = events[0].keys()
    with open(filename, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(events)

    logger.info(f"Events saved to {filename}.")


def main(
    calendar_id,
    time_zone,
    scopes,
    token_json_file,
    credentials_json_file,
    max_results=2500,
    dry_run=False,
):
    calendar_api = GoogleCalendarAPI(
        calendar_id,
        time_zone,
        scopes,
        token_json_file,
        credentials_json_file,
        max_results,
        dry_run,
    )

    # Load local events from JSON file
    try:
        with open("/workspaces/py.hsbi-timetable_2.0/output/Stundenplan WS_2024_2025_ELM 3_Stand 2024-10-11_events.json", "r") as file:
            local_events = json.load(file)
            logger.info(f"Found {len(local_events)} events in the timetable.")
    except json.JSONDecodeError as e:
        logger.error(f"Error reading final_events.json: {e}")
        return

    if dry_run:
        created_events = create_all_events(calendar_api, local_events)
        save_events_to_csv(created_events, "/workspaces/py.hsbi-timetable_2.0/output/created_events.csv")
    else:
        delete_all_events(calendar_api, time_zone)
        create_all_events(calendar_api, local_events)


if __name__ == "__main__":
    # Example inputs, replace with actual values
    calendar_id = "a0fd5d4d46978655a3a840648665285da64e2a08e761c5a9b0800fd5730d2024@group.calendar.google.com"
    time_zone = "Europe/Berlin"
    api_url = ["https://www.googleapis.com/auth/calendar"]
    token_json_file = "/workspaces/py.hsbi-timetable_2.0/config/token.json"
    credential_json_file = "/workspaces/py.hsbi-timetable_2.0/config/client_secret.json"
    max_results = 2500
    dry_run = False
    main(
        calendar_id,
        time_zone,
        api_url,
        token_json_file,
        credential_json_file,
        max_results,
        dry_run,
    )
