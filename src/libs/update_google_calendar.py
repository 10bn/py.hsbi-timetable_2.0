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
from logger import setup_logger  # Set up the logger

# Set up logging
setup_logger()
logger = logging.getLogger(__name__)

# Constants for authentication and calendar settings
SCOPES = ["https://www.googleapis.com/auth/calendar"]
TOKEN_JSON_FILE = "config/token.json"
CREDENTIALS_JSON_FILE = "config/client_secret.json"
CALENDAR_ID_ELM3 = "9a901e48af79cd47cb67c184c642400a25fc301ad3bacf45ae6e003672174209@group.calendar.google.com"
CALENDAR_ID_ELM5 = "dd947828be3d70701a1373643ece5ef5bee930d44c14fa2bf7ba3dc9004d603e@group.calendar.google.com"
TIME_ZONE = "Europe/Berlin"
MAX_RESULTS = 2500


class GoogleCalendarAPI:
    def __init__(self, calendar_id, time_zone, dry_run=False):
        self.calendar_id = calendar_id
        self.time_zone = time_zone
        self.dry_run = dry_run
        if not dry_run:
            self.service = self.authenticate()

    def authenticate(self):
        creds = None
        if os.path.exists(TOKEN_JSON_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_JSON_FILE, SCOPES)
        
        # Handle token refresh or re-authentication
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    os.remove(TOKEN_JSON_FILE)  # Remove invalid token
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_JSON_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                with open(TOKEN_JSON_FILE, "w") as token:
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
                maxResults=MAX_RESULTS,
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
            local_tz = pytz.timezone(TIME_ZONE)
            start_datetime = local_tz.localize(datetime.combine(date.date(), start_time))
            end_datetime = local_tz.localize(datetime.combine(date.date(), end_time))

            # Ensure lecturer is a list
            lecturer_field = event["lecturer"]
            lecturer_list = json.loads(lecturer_field.replace("'", '"')) if isinstance(lecturer_field, str) else lecturer_field

            # Get event details, ensure it's not None
            details = event.get("details", "")

            # Combine summary and details
            summary = f"{event['course']}, {details}" if details else event['course']

            return {
                "summary": summary,
                "location": event.get("location", ""),
                "start": {
                    "dateTime": start_datetime.isoformat(),
                    "timeZone": TIME_ZONE,
                },
                "end": {
                    "dateTime": end_datetime.isoformat(),
                    "timeZone": TIME_ZONE,
                },
                "description": ", ".join(lecturer_list),
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
                created_event = self.service.events().insert(calendarId=self.calendar_id, body=event_data).execute()
                logger.info(f'Event created: {created_event["summary"]}')
                return created_event
        else:
            logger.error("Failed to create event due to preparation error.")

    def delete_event(self, event_id):
        if self.dry_run:
            logger.info(f"Dry run mode: Would delete event with ID: {event_id}")
        else:
            self.service.events().delete(calendarId=self.calendar_id, eventId=event_id).execute()
            logger.info(f"Deleted event with ID: {event_id}")


def create_all_events(calendar_api, local_events):
    created_events = []
    for event in local_events:
        created_event = calendar_api.create_event(event)
        if created_event:
            created_events.append(created_event)
    return created_events


def delete_all_events(calendar_api):
    start_date = datetime.now(pytz.timezone(TIME_ZONE)) - timedelta(days=300)
    end_date = datetime.now(pytz.timezone(TIME_ZONE)) + timedelta(days=300)
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


def main(dry_run=False):
    calendar_api = GoogleCalendarAPI(CALENDAR_ID_ELM3, TIME_ZONE, dry_run)

    # Load local events from JSON file
    try:
        with open("output/final_events.json", "r") as file:
            local_events = json.load(file)
            logger.info(f"Found {len(local_events)} events in the timetable.")
    except json.JSONDecodeError as e:
        logger.error(f"Error reading final_events.json: {e}")
        return

    if dry_run:
        created_events = create_all_events(calendar_api, local_events)
        save_events_to_csv(created_events, "output/dry_run_output.csv")
    else:
        delete_all_events(calendar_api)
        create_all_events(calendar_api, local_events)


if __name__ == "__main__":
    dry_run = False  # Set to True for a dry run, False for actual operation
    main(dry_run)