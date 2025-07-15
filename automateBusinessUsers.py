import os
import requests
import pprint
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('EnsembleApi')

# Define the keywords that signal an event
EVENT_KEYWORDS = [
    'event', 'concert', 'show', 'performance', 'party', 'celebration',
    'festival', 'workshop', 'seminar', 'conference', 'meetup', 'launch',
    'opening', 'closing', 'anniversary', 'birthday', 'wedding', 'gala',
    'competition', 'tournament', 'match', 'game', 'screening', 'premiere',
    'sale', 'promotion', 'special', 'limited time', 'today only', 'live',
    'happening', 'join us', 'come celebrate', 'don\'t miss'
]


def _format_as_event(profile_data: dict, event_post: dict) -> dict:
    """Helper function to structure data for an Event."""
    if event_post is None:
        event_post = {}  # Ensure no errors if no specific post was found

    caption_edges = event_post.get('edge_media_to_caption', {}).get('edges', [])
    caption = caption_edges[0]['node']['text'] if caption_edges else profile_data.get('biography', '')

    location_obj = event_post.get('location')
    venue_id = location_obj.get('id') if location_obj else None

    event_data = {
        'performerId': "None",
        'eventName': profile_data.get('full_name', ''),
        'description': caption,
        'minAmount': "None",
        'eventDate': "None",
        'posterUrl': event_post.get('display_url', profile_data.get('profile_pic_url', '')),
        'createdBy': profile_data.get('username', ''),
        'deletedAt': "None",
        'createdAt': datetime.now().isoformat(),
        'updatedAt': datetime.now().isoformat(),
        'isPaid': "False",
        'ticketingURL': profile_data.get('external_url', ''),
        'eventQRCode': "None",
        'eventStatus': 'active',
        'previousEventDate': "None",
        'previousStartTime': "None",
        'previousEndTime': "None",
        'startTime': "None",
        'endTime': "None",
        'venueId': venue_id
    }
    return event_data


def _format_as_venue(profile_data: dict) -> dict:
    """Helper function to structure data for a Venue."""
    address = ''
    # Get the business_address_json field, which might be None.
    address_str = profile_data.get('business_address_json')

    # FIX: Only try to parse the JSON if address_str is actually a string.
    if isinstance(address_str, str) and address_str:
        try:
            address_json = json.loads(address_str)
            address = address_json.get('street_address', '')
        except json.JSONDecodeError:
            # If the string is somehow not valid JSON, default to an empty address.
            address = ''

    venue_data = {
        'userId': "None",
        'venueName': profile_data.get('full_name', ''),
        'email': profile_data.get('business_email'),
        'phoneNumber': profile_data.get('business_phone_number'),
        'address': address,
        'openHours': '',
        'closingHours': '',
        'latitude': "None",
        'longitude': "None",
        'capacity': "None",
        'description': profile_data.get('biography', ''),
        'website': profile_data.get('external_url', ''),
        'profileImageUrl': profile_data.get('profile_pic_url', ''),
        'coverImageUrl': '',
        'allowsDirectBookings': "False",
        'createdAt': datetime.now().isoformat(),
        'updatedAt': datetime.now().isoformat(),
        'deletedAt': "None"
    }
    return venue_data


def InstaScrapper(scraped_data: list) -> dict:
    """
    Intelligently determines if an Instagram profile is for an Event or a Venue
    by analyzing the 5 most recent posts, and formats the output data accordingly.

    Args:
        scraped_data: The raw JSON data from the scraper.

    Returns:
        A dictionary formatted as either event_data or venue_data.
    """
    try:
        profile_data = scraped_data[0]['data']
        posts = profile_data.get('edge_owner_to_timeline_media', {}).get('edges', [])
    except (IndexError, KeyError, TypeError):
        return {"error": "Invalid or incomplete data structure provided."}

    recent_posts = posts[:5]

    is_event = False
    best_event_post = None

    for post in recent_posts:
        node = post.get('node', {})
        if node.get('has_upcoming_event'):
            is_event = True
            best_event_post = node
            break

    # Second signal: Check for keywords in bio and the 5 recent post captions.
    if not is_event and recent_posts:
        captions = []
        for post in recent_posts:
            node = post.get('node', {})
            caption_edges = node.get('edge_media_to_caption', {}).get('edges', [])
            if caption_edges:
                captions.append(caption_edges[0]['node']['text'])

        # Combine profile bio and all recent captions for a comprehensive search
        text_to_search = (profile_data.get('biography', '') + ' ' + ' '.join(captions)).lower()

        if any(keyword in text_to_search for keyword in EVENT_KEYWORDS):
            is_event = True
            # If a keyword is found, assume the most recent post is the most relevant one
            best_event_post = recent_posts[0].get('node', {})

    # --- Format the Output Based on the Decision ---
    if is_event:
        print("✅ Profile identified as an EVENT.")
        return _format_as_event(profile_data, best_event_post)
    else:
        print("🏠 Profile identified as a VENUE.")
        return _format_as_venue(profile_data)


def connectScrapper(username: str):
    """
    Connects to the API, fetches data, filters it, and prints the result.
    """
    root = "https://ensembledata.com/apis"
    endpoint = "/instagram/user/detailed-info"
    params = {
        "username": username,
        "token": TOKEN
    }

    print(f"🔎 Scraping data for user: {username}...")

    try:
        # 1. Make the API request
        response = requests.get(root + endpoint, params=params)
        response.raise_for_status()

        # 2. Parse the JSON from the response
        scraped_json = response.json()

        # 3. Pass the parsed JSON to the filtering function.
        filtered_results = InstaScrapper([scraped_json])

        # 4. Print the final, filtered list
        print("\n--- Filtered Results ---")
        pprint.pprint(filtered_results)

        return filtered_results

    except requests.exceptions.RequestException as e:
        print(f"❌ API Request Failed: {e}")
        return None
    except json.JSONDecodeError:
        print("❌ Failed to parse JSON from the API response.")
        return None


if __name__ == "__main__":
    connectScrapper("quiverlounge_kilimani")
