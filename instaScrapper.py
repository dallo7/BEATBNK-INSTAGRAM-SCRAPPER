import os
import requests
import pprint
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import time
import random

load_dotenv()
TOKEN = os.getenv('EnsembleApi')

# --- CONFIGURATION ---
# 1. List of places to iterate through
PLACES_TO_SCRAPE = [
    "Kalamatake",
    "quiverlounge_kilimani",
    "Platinum7dnakuru",
    "naxvegasclublife",
    "1824"
]

# 2. How often to check for new data (in seconds)
SCRAPE_INTERVAL = 60  # 2 minutes

# --- DATABASE CONNECTION PARAMETERS ---
db_connection_params = {
    "dbname": "beatbnk_db",
    "user": "user",
    "password": "X1SOrzeSrk",
    "host": "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
    "port": "5432"
}

EVENT_KEYWORDS = [
    'event', 'concert', 'show', 'performance', 'party', 'celebration',
    'festival', 'workshop', 'seminar', 'conference', 'meetup', 'launch',
    'opening', 'closing', 'anniversary', 'birthday', 'wedding', 'gala'
]


def _format_as_event(profile_data: dict, event_post: dict, creator_id: str) -> dict:
    if event_post is None: event_post = {}
    caption_edges = event_post.get('edge_media_to_caption', {}).get('edges', [])
    caption = caption_edges[0]['node']['text'] if caption_edges else profile_data.get('biography', '')
    location_obj = event_post.get('location')
    venue_id = location_obj.get('id') if location_obj else None

    # Use the Instagram profile ID as the primary key for the record
    return {
        'id': random.randint(100, 99999),
        'performerId': None,
        'eventName': profile_data.get('full_name', ''),
        'description': caption,
        'minAmount': None,
        'eventDate': datetime.now(),
        'posterUrl': event_post.get('display_url', profile_data.get('profile_pic_url', '')),
        'createdBy': 289,
        'deletedAt': None,
        'createdAt': datetime.now(),
        'updatedAt': datetime.now(),
        'isPaid': False,
        'ticketingURL': profile_data.get('external_url', ''),
        'eventQRCode': None,
        'eventStatus': 'UNPUBLISHED',
        'previousEventDate': None,
        'previousStartTime': None,
        'previousEndTime': None,
        'startTime': datetime.now(),
        'endTime': datetime.now(),
        'venueId': 4,
    }


def _format_as_venue(profile_data: dict, creator_id: str) -> dict:
    address = ''
    address_str = profile_data.get('business_address_json')
    if isinstance(address_str, str) and address_str:
        try:
            address_json = json.loads(address_str)
            address = address_json.get('street_address', '')
        except json.JSONDecodeError:
            address = ''

    # Use the Instagram profile ID as the primary key for the record
    return {
        'id': random.randint(100, 9999),
        'userId': 289,
        'venueName': profile_data.get('full_name', ''),
        'email': profile_data.get('business_email'),
        'phoneNumber': profile_data.get('business_phone_number'),
        'address': address,
        'openHours': None,
        'closingHours': None,
        'latitude': None,
        'longitude': None,
        'capacity': None,
        'description': profile_data.get('biography', ''),
        'website': profile_data.get('external_url', ''),
        'profileImageUrl': profile_data.get('profile_pic_url', ''),
        'coverImageUrl': None,
        'allowsDirectBookings': False,
        'createdAt': datetime.now(),
        'updatedAt': datetime.now(),
        'deletedAt': None
    }


def upsert_to_db(record_type: str, data: dict):
    """
    Inserts a new record or updates an existing one based on the ID.
    This is known as an "UPSERT" operation.
    """
    table_name = "events" if record_type == "event" else "venues"

    # Build the parts of the SQL query
    columns = data.keys()
    columns_sql = ', '.join(f'"{col}"' for col in columns)
    placeholders = ', '.join(['%s'] * len(columns))

    # Create the "ON CONFLICT... DO UPDATE" part
    update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in columns if col != 'id']
    update_sql = ', '.join(update_cols)

    # Combine into the final UPSERT statement
    upsert_statement = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {update_sql};
    """
    values = list(data.values())
    conn = None
    try:
        conn = psycopg2.connect(**db_connection_params)
        cur = conn.cursor()
        cur.execute(upsert_statement, values)
        conn.commit()
        print(f"💾 Data successfully upserted to the '{table_name}' table.")
    except psycopg2.Error as e:
        print(f"❌ Database error during upsert: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


def InstaScrapper(scraped_data: list, user_id: str) -> tuple:
    """
    Determines if a profile is an Event or Venue and returns the
    type, formatted data, and the latest post ID for change detection.
    """
    latest_post_id = None
    try:
        profile_data = scraped_data[0]['data']
        posts = profile_data.get('edge_owner_to_timeline_media', {}).get('edges', [])
        if posts:
            # Get the ID of the most recent post
            latest_post_id = posts[0].get('node', {}).get('id')
    except (IndexError, KeyError, TypeError):
        return "error", {"error": "Invalid data structure."}, None

    recent_posts = posts[:5]
    is_event = False
    best_event_post = None
    for post in recent_posts:
        node = post.get('node', {})
        if node.get('has_upcoming_event'):
            is_event = True
            best_event_post = node
            break
    if not is_event and recent_posts:
        captions = [p['node']['edge_media_to_caption']['edges'][0]['node']['text'] for p in recent_posts if
                    p.get('node', {}).get('edge_media_to_caption', {}).get('edges')]
        text_to_search = (profile_data.get('biography', '') + ' ' + ' '.join(captions)).lower()
        if any(keyword in text_to_search for keyword in EVENT_KEYWORDS):
            is_event = True
            best_event_post = recent_posts[0].get('node', {})

    if is_event:
        print("✅ Profile identified as an EVENT.")
        return "event", _format_as_event(profile_data, best_event_post, user_id), latest_post_id
    else:
        print("🏠 Profile identified as a VENUE.")
        return "venue", _format_as_venue(profile_data, user_id), latest_post_id


def process_user(username: str, last_post_ids: dict):
    """
    Contains the logic for fetching and processing a single user.
    """
    root = "https://ensembledata.com/apis"
    endpoint = "/instagram/user/detailed-info"
    params = {"username": username, "token": TOKEN}
    print("-" * 50)
    print(f"🔎 Scraping data for user: {username}...")
    try:
        response = requests.get(root + endpoint, params=params)
        response.raise_for_status()
        scraped_json = response.json()

        logged_in_user_id = '1234'

        record_type, filtered_data, new_post_id = InstaScrapper([scraped_json], logged_in_user_id)

        # CHECK FOR NEW DATA
        if new_post_id and new_post_id == last_post_ids.get(username):
            print(f"👍 No new posts found for {username}. Skipping.")
            return

        print("\n--- Filtered Results ---")
        pprint.pprint(filtered_data)

        if record_type != "error":
            upsert_to_db(record_type, filtered_data)
            # Update the dictionary with the latest post ID
            if new_post_id:
                last_post_ids[username] = new_post_id

    except requests.exceptions.RequestException as e:
        print(f"❌ API Request Failed for {username}: {e}")
    except json.JSONDecodeError:
        print(f"❌ Failed to parse JSON for {username}.")


# def start_scraping_cycle():
#     """
#     Initializes and runs the continuous scraping loop.
#     """
#     last_post_ids = {}
#     while True:
#         print("\n===== Starting new scrape cycle... =====")
#         for username in PLACES_TO_SCRAPE:
#             process_user(username, last_post_ids)
#             print(f"--- Waiting for {SCRAPE_INTERVAL} seconds... ---")
#             time.sleep(SCRAPE_INTERVAL)
#
#         print(f"\n===== Full cycle complete. Waiting for 2 hours before restarting... =====")
#         time.sleep(7200)
#
#
# start_scraping_cycle()




