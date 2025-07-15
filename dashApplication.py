import os
import json
import random
import time
from datetime import datetime
import base64
import requests
import dash
import dash_bootstrap_components as dbc
import psycopg2
from dash import dcc, html, Input, Output, State

# --- Environment and Database Configuration ---
TOKEN = os.getenv('EnsembleApi')
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


# ==============================================================================
# REFACTORED BACKEND LOGIC FOR DASH INTEGRATION
# ==============================================================================

def image_to_base64(image_url):
    """Fetches an image from a URL and encodes it to a Base64 string."""
    if not image_url:
        return None
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()

        # Get the content type to correctly format the data URI
        content_type = response.headers.get('content-type', 'image/png')

        # Encode the image content in Base64
        encoded_image = base64.b64encode(response.content).decode('utf-8')

        # Return the formatted data URI
        return f"data:{content_type};base64,{encoded_image}"

    except requests.exceptions.RequestException as e:
        print(f"Error fetching image from {image_url}: {e}")
        return None


def _format_as_event(profile_data: dict, event_post: dict) -> dict:
    """Formats scraped data into an event dictionary."""
    if event_post is None: event_post = {}
    caption_edges = event_post.get('edge_media_to_caption', {}).get('edges', [])
    caption = caption_edges[0]['node']['text'] if caption_edges else profile_data.get('biography', '')

    # Using current time for demonstration as in the original script
    now = datetime.now()

    return {
        'id': random.randint(100, 99999),
        'performerId': None,
        'eventName': profile_data.get('full_name', 'Unnamed Event'),
        'description': caption,
        'minAmount': None,
        'eventDate': now,
        'posterUrl': event_post.get('display_url', profile_data.get('profile_pic_url', '')),
        'createdBy': 289,
        'deletedAt': None,
        'createdAt': now,
        'updatedAt': now,
        'isPaid': False,
        'ticketingURL': profile_data.get('external_url', ''),
        'eventQRCode': None,
        'eventStatus': 'UNPUBLISHED',
        'previousEventDate': None,
        'previousStartTime': None,
        'previousEndTime': None,
        'startTime': now,
        'endTime': now,
        'venueId': 7,
    }


def _format_as_venue(profile_data: dict) -> dict:
    """Formats scraped data into a venue dictionary."""
    address = ''
    address_str = profile_data.get('business_address_json')
    if isinstance(address_str, str) and address_str:
        try:
            address_json = json.loads(address_str)
            address = address_json.get('street_address', '')
        except json.JSONDecodeError:
            address = ''

    now = datetime.now()
    return {
        'id': random.randint(100, 9999),
        'userId': 289,
        'venueName': profile_data.get('full_name', 'Unnamed Venue'),
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
        'createdAt': now,
        'updatedAt': now,
        'deletedAt': None
    }


def upsert_to_db(record_type: str, data: dict):
    """Upserts a record and returns a status tuple (success, message)."""
    table_name = "events" if record_type == "event" else "venues"
    record_name = data.get('eventName', data.get('venueName'))
    columns = data.keys()
    columns_sql = ', '.join(f'"{col}"' for col in columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in columns if col != 'id']
    update_sql = ', '.join(update_cols)

    upsert_statement = f"""
        INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_sql};
    """
    values = list(data.values())
    try:
        with psycopg2.connect(**db_connection_params) as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement, values)
        return (True, f"Successfully upserted '{record_name}' to '{table_name}'.")
    except psycopg2.Error as e:
        return (False, f"Database error for '{record_name}': {e}")
    except Exception as e:
        return (False, f"An unexpected error occurred during DB operation: {e}")


def InstaScrapper(scraped_data: list) -> tuple:
    """Determines profile type and returns formatted data."""
    try:
        profile_data = scraped_data[0]['data']
        posts = profile_data.get('edge_owner_to_timeline_media', {}).get('edges', [])
    except (IndexError, KeyError, TypeError):
        return "error", {"error": "Invalid data structure from API."}

    recent_posts = posts[:5]
    is_event, best_event_post = False, None
    for post in recent_posts:
        node = post.get('node', {})
        if node.get('has_upcoming_event'):
            is_event, best_event_post = True, node
            break
    if not is_event and recent_posts:
        captions = [p['node']['edge_media_to_caption']['edges'][0]['node']['text'] for p in recent_posts if
                    p.get('node', {}).get('edge_media_to_caption', {}).get('edges')]
        text_to_search = (profile_data.get('biography', '') + ' ' + ' '.join(captions)).lower()
        if any(keyword in text_to_search for keyword in EVENT_KEYWORDS):
            is_event, best_event_post = True, recent_posts[0].get('node', {})

    return ("event", _format_as_event(profile_data, best_event_post)) if is_event else (
        "venue", _format_as_venue(profile_data))


def create_summary_card(record_type, data, db_success):
    """Creates a dbc.Card component to summarize the scraping result with an embedded image."""
    if record_type == 'event':
        title = data.get('eventName', 'N/A')
        # Get the original image URL
        image_url = data.get('posterUrl')
        description = data.get('description', 'No description available.')
        short_desc = (description[:120] + '...') if len(description) > 120 else description
        event_date_obj = data.get('eventDate')
        event_date_str = event_date_obj.strftime('%a, %b %d, %Y') if event_date_obj else 'N/A'
        icon = "🎉"

        card_body = [
            html.P(f"🗓️ Date: {event_date_str}", className="card-text fw-bold"),
            html.P(short_desc, className="card-text text-muted small"),
        ]
        if data.get('ticketingURL'):
            card_body.append(dbc.CardLink("Get Tickets", href=data.get('ticketingURL'), target="_blank"))

    elif record_type == 'venue':
        title = data.get('venueName', 'N/A')
        # Get the original image URL
        image_url = data.get('profileImageUrl')
        description = data.get('description', 'No description available.')
        short_desc = (description[:120] + '...') if len(description) > 120 else description
        icon = "🏠"

        card_body = [html.P(short_desc, className="card-text text-muted small")]
        if data.get('address'):
            card_body.append(html.P(f"📍 {data.get('address')}", className="card-text small"))
        if data.get('phoneNumber'):
            card_body.append(html.P(f"📞 {data.get('phoneNumber')}", className="card-text small"))

    else:  # Error case
        return None

    # --- KEY CHANGE IS HERE ---
    # Convert the image URL to a Base64 string before passing it to the card
    base64_image = image_to_base64(image_url)

    db_badge = dbc.Badge("DB Success", color="success", className="me-1") if db_success else dbc.Badge("DB Failed",
                                                                                                       color="danger",
                                                                                                       className="me-1")

    return dbc.Col(dbc.Card([
        dbc.CardHeader(f"{icon} {title} ({record_type.capitalize()})"),
        # Use the base64_image string as the source for dbc.CardImg
        dbc.CardImg(src=base64_image, top=True, className="card-img-top") if base64_image else None,
        dbc.CardBody(card_body),
        dbc.CardFooter(db_badge)
    ], className="mb-4 h-100 shadow-lg border-primary fade-in"), md=6, lg=4)


# ==============================================================================
# UPDATED SCRAPING JOB GENERATOR
# ==============================================================================
def run_scraping_job(usernames: list):
    """Manages scraping and yields logs, alerts, and card data."""
    yield 'log', html.P("🚀 Starting new scrape cycle...", className="log-entry")

    for username in (u.strip() for u in usernames if u.strip()):
        yield 'log', html.P(f"🔎 Scraping data for: {username}...", className="log-entry")

        try:
            response = requests.get(f"https://ensembledata.com/apis/instagram/user/detailed-info",
                                    params={"username": username, "token": TOKEN}, timeout=20)
            response.raise_for_status()
            record_type, filtered_data = InstaScrapper([response.json()])

            if record_type != "error":
                yield 'log', html.P(f"✅ Profile '{username}' identified as an {record_type.upper()}.",
                                    className="log-entry")
                success, message = upsert_to_db(record_type, filtered_data)

                alert_color = "success" if success else "danger"
                yield 'alert', dbc.Alert(message, color=alert_color, dismissable=True, duration=6000,
                                         className="fade-in")

                yield 'card', create_summary_card(record_type, filtered_data, success)
            else:
                yield 'log', html.P(f"❌ Error processing {username}: {filtered_data.get('error')}",
                                    className="log-entry error")

        except requests.exceptions.RequestException as e:
            yield 'log', html.P(f"❌ API Request Failed for {username}: {e}", className="log-entry error")
        except Exception as e:
            yield 'log', html.P(f"❌ An unexpected error occurred for {username}: {e}", className="log-entry error")

        time.sleep(0.5)

    yield 'log', html.P("✅ Scraping cycle complete.", className="log-entry")


# ==============================================================================
# DASH APPLICATION (with updated layout)
# ==============================================================================
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.VAPOR, dbc.icons.FONT_AWESOME])
app.title = "BB Instagram Scrapper"

app.layout = dbc.Container([
    dcc.Store(id='session-store'),
    html.Div([
        html.H1("🚀 BB Instagram Scrapper", className="display-3 title-glow"),
        html.P("Enter Instagram profiles to scrape and save them as events or venues.", className="lead")
    ], className="text-center my-4"),

    dbc.Row(justify="center", children=[
        dbc.Col(lg=6, md=10, sm=12, children=[
            dbc.Textarea(id="profiles-input", placeholder="Enter Instagram usernames, one per line...", rows=5,
                         className="mb-3"),
            dbc.Tooltip("Enter each Instagram username on a new line.", target="profiles-input", placement="top"),
            dbc.Button("Scrape Instagram Profiles", id="scrape-button", color="primary", className="w-100 shadow",
                       n_clicks=0, style={'fontWeight': 'bold'}),
            html.Div(id="alert-container", className="mt-4")
        ])
    ]),

    # NEW: Summary Cards Section
    html.H3("Scraping Summary", className="text-center mt-5 mb-3 title-glow"),
    dbc.Row(id="summary-cards-container", className="mb-5"),

    html.Hr(),

    # Detailed Log Section
    html.H3("Detailed Logs", className="text-center mt-4 mb-3"),
    dcc.Loading(id="loading-spinner", type="default", children=[
        html.Div(id="log-output", className="log-container")
    ])
], fluid=True, className="p-5")


@app.callback(
    [Output('summary-cards-container', 'children'),
     Output('log-output', 'children'),
     Output('alert-container', 'children')],
    [Input('scrape-button', 'n_clicks')],
    [State('profiles-input', 'value')],
    prevent_initial_call=True
)
def update_output(n_clicks, profiles_value):
    """Triggers scraping and updates the UI with cards, logs, and alerts."""
    if not profiles_value:
        return [], [html.P("Please enter at least one profile username.", className="text-warning")], []

    cards, logs, alerts = [], [], []
    for item_type, content in run_scraping_job(profiles_value.split('\n')):
        if item_type == 'log':
            logs.append(content)
        elif item_type == 'alert':
            alerts.append(content)
        elif item_type == 'card' and content:
            cards.append(content)

    return cards, logs, alerts


if __name__ == '__main__':
    app.run(debug=True)
