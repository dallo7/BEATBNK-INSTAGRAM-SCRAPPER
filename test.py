# import os
# import random
# import requests
# import pprint
# import json
# from datetime import datetime
# from dotenv import load_dotenv
# import psycopg2
# import uuid
#
# # Generate a random, unique ID object
# random_id_obj = uuid.uuid4()
#
# # Convert the ID object to a string for storage or use
# id_as_string = str(random_id_obj)
#
# load_dotenv()
# TOKEN = os.getenv('EnsembleApi')
#
# # --- DATABASE CONNECTION PARAMETERS ---
# db_connection_params = {
#     "dbname": "beatbnk_db",
#     "user": "user",
#     "password": "X1SOrzeSrk",
#     "host": "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
#     "port": "5432"
# }
#
# EVENT_KEYWORDS = [
#     'event', 'concert', 'show', 'performance', 'party', 'celebration',
#     'festival', 'workshop', 'seminar', 'conference', 'meetup', 'launch',
#     'opening', 'closing', 'anniversary', 'birthday', 'wedding', 'gala',
#     'competition', 'tournament', 'match', 'game', 'screening', 'premiere',
#     'sale', 'promotion', 'special', 'limited time', 'today only', 'live',
#     'happening', 'join us', 'come celebrate', 'don\'t miss'
# ]
#
#
# def _format_as_event(profile_data: dict, event_post: dict) -> dict:
#     """Helper function to structure data for an Event."""
#     if event_post is None:
#         event_post = {}
#
#     caption_edges = event_post.get('edge_media_to_caption', {}).get('edges', [])
#     caption = caption_edges[0]['node']['text'] if caption_edges else profile_data.get('biography', '')
#
#     location_obj = event_post.get('location')
#     venue_id = location_obj.get('id') if location_obj else None
#
#     # CRITICAL: Use Python's None, not the string "None", for database compatibility
#     event_data = {
#         'id': random.randint(1000, 9999),
#         'performerId': None,
#         'eventName': profile_data.get('full_name', ''),
#         'description': caption,
#         'minAmount': None,
#         'eventDate': datetime.now(),
#         'posterUrl': event_post.get('display_url', profile_data.get('profile_pic_url', '')),
#         'createdBy': 289,
#         'deletedAt': None,
#         'createdAt': datetime.now(),
#         'updatedAt': datetime.now(),
#         'isPaid': False,
#         'ticketingURL': profile_data.get('external_url', ''),
#         'eventQRCode': None,
#         'eventStatus': 'UNPUBLISHED',
#         'previousEventDate': None,
#         'previousStartTime': None,
#         'previousEndTime': None,
#         'startTime': datetime.now(),
#         'endTime': datetime.now(),
#         'venueId': venue_id
#     }
#     return event_data
#
#
# def _format_as_venue(profile_data: dict) -> dict:
#     """Helper function to structure data for a Venue."""
#     address = ''
#     address_str = profile_data.get('business_address_json')
#
#     if isinstance(address_str, str) and address_str:
#         try:
#             address_json = json.loads(address_str)
#             address = address_json.get('street_address', '')
#         except json.JSONDecodeError:
#             address = ''
#
#     venue_data = {
#         'id': random.randint(1000, 99999),
#         'userId': 289,
#         'venueName': profile_data.get('full_name', ''),
#         'email': profile_data.get('business_email'),
#         'phoneNumber': profile_data.get('business_phone_number'),
#         'address': address,
#         'openHours': datetime.now(),
#         'closingHours': datetime.now(),
#         'latitude': None,
#         'longitude': None,
#         'capacity': None,
#         'description': profile_data.get('biography', ''),
#         'website': profile_data.get('external_url', ''),
#         'profileImageUrl': profile_data.get('profile_pic_url', ''),
#         'coverImageUrl': '',
#         'deletedAt': None,
#         'createdAt': datetime.now(),
#         'updatedAt': datetime.now(),
#         'allowsDirectBookings': False
#     }
#     return venue_data
#
#
# # --- NEW: Function to Save Data to PostgreSQL ---
# def save_to_db(record_type: str, data: dict):
#     """
#     Saves a dictionary of data to the appropriate PostgreSQL table.
#     """
#     table_name = "events" if record_type == "event" else "venues"
#
#     columns = data.keys()
#     columns_sql = ', '.join(f'"{col}"' for col in columns)
#
#     placeholders = ', '.join(['%s'] * len(columns))
#
#     insert_statement = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
#
#     values = list(data.values())
#
#     conn = None
#     try:
#         conn = psycopg2.connect(**db_connection_params)
#         cur = conn.cursor()
#         cur.execute(insert_statement, values)
#         conn.commit()
#         print(f"✅ Data successfully saved to the '{table_name}' table.")
#
#     except psycopg2.Error as e:
#         print(f"❌ Database error: {e}")
#     finally:
#         if conn:
#             cur.close()
#             conn.close()
#
#
# def InstaScrapper(scraped_data: list) -> tuple:
#     """
#     Intelligently determines if a profile is an Event or Venue and returns
#     the type and formatted data.
#     """
#     try:
#         profile_data = scraped_data[0]['data']
#         posts = profile_data.get('edge_owner_to_timeline_media', {}).get('edges', [])
#     except (IndexError, KeyError, TypeError):
#         return "error", {"error": "Invalid or incomplete data structure provided."}
#
#     recent_posts = posts[:5]
#     is_event = False
#     best_event_post = None
#
#     for post in recent_posts:
#         node = post.get('node', {})
#         if node.get('has_upcoming_event'):
#             is_event = True
#             best_event_post = node
#             break
#
#     if not is_event and recent_posts:
#         captions = []
#         for post in recent_posts:
#             node = post.get('node', {})
#             caption_edges = node.get('edge_media_to_caption', {}).get('edges', [])
#             if caption_edges:
#                 captions.append(caption_edges[0]['node']['text'])
#
#         text_to_search = (profile_data.get('biography', '') + ' ' + ' '.join(captions)).lower()
#
#         if any(keyword in text_to_search for keyword in EVENT_KEYWORDS):
#             is_event = True
#             best_event_post = recent_posts[0].get('node', {})
#
#     if is_event:
#         print("✅ Profile identified as an EVENT.")
#         # Return both the type and the data
#         return "event", _format_as_event(profile_data, best_event_post)
#     else:
#         print("🏠 Profile identified as a VENUE.")
#         # Return both the type and the data
#         return "venue", _format_as_venue(profile_data)
#
#
# def connectScrapper(username: str):
#     """
#     Connects to the API, fetches data, filters it, and saves it to the DB.
#     """
#     root = "https://ensembledata.com/apis"
#     endpoint = "/instagram/user/detailed-info"
#     params = {"username": username, "token": TOKEN}
#
#     print(f"🔎 Scraping data for user: {username}...")
#
#     try:
#         response = requests.get(root + endpoint, params=params)
#         response.raise_for_status()
#         scraped_json = response.json()
#
#         # Get both the record type and the formatted data
#         record_type, filtered_data = InstaScrapper([scraped_json])
#
#         print("\n--- Filtered Results ---")
#         pprint.pprint(filtered_data)
#
#         # If there wasn't an error, save the results to the database
#         if record_type != "error":
#             save_to_db(record_type, filtered_data)
#
#     except requests.exceptions.RequestException as e:
#         print(f"❌ API Request Failed: {e}")
#     except json.JSONDecodeError:
#         print("❌ Failed to parse JSON from the API response.")
#
#
# if __name__ == "__main__":
#     connectScrapper("kamatake")

