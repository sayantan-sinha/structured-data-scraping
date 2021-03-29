from pprint import pprint
import requests
import extruct
from bs4 import BeautifulSoup
from w3lib.html import get_base_url
from urllib.parse import urlparse, urljoin, urlsplit
import sqlite3 as sq

def create_event_list():
    url = "https://www.eventbrite.com/d/online/all-events/"
    resp = get_html(url)
    soup = BeautifulSoup(resp, "html.parser")
    events = soup.findAll("div", {'class': 'search-event-card-wrapper'})
    links = []
    for event in events:
        links.append(event.select_one("a", {'class': 'eds-event-card-content__action-link', 'tabindex': '0'}).attrs.get("href"))

    return links

counter = 1
def get_html(url):
    """Get raw HTML from a URL."""
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '3600',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }
    req = requests.get(url, headers=headers)
    return req.text

def scrape(url):
    """Parse structured data from a target page."""
    html = get_html(url)
    metadata = get_metadata(html, url)
    # pprint(metadata, indent=2, width=150)
    return metadata

def get_metadata(html: str, url: str):
    """Fetch JSON-LD structured data."""
    metadata = extruct.extract(
        html,
        base_url=get_base_url(url),
        syntaxes=['json-ld'],
        uniform=True
    )['json-ld']
    if bool(metadata) and isinstance(metadata, list):
        metadata = metadata[0]
    return metadata

def get_details(data):
    global counter
    print(f"=========== EVENT {counter} ===========")
    card = {
        'title': data.get('name', " "),
        'location': data.get('location', dict()).get('@type'),
        # 'location': data.get('location', "Online"),
        'organiser': data.get('organizer', dict()).get('name'),
        'date': data.get('startDate', None)
    }
    counter += 1
    pprint(card, indent=2)
    return card

#   ============= URL Portion =============

def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def classify_url(url) -> bool:
    parsed = urlsplit(url)
    if parsed.path.split("/")[1] == 'e':
        return True
    return False

def get_urls(url):
    """
        Returns all URLs that is found on `url` in which it belongs to the same website
        """
    # all URLs of `url`
    internal_urls = set()
    external_urls = set()
    interesting_url = set()
    non_interesting_url = set()
    # urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            continue
        if domain_name not in href:
            if href not in external_urls:
                external_urls.add(href)
            continue
        internal_urls.add(href)

        if classify_url(href):
            interesting_url.add(href)
        else:
            non_interesting_url.add(href)

    return (interesting_url, non_interesting_url)

def save_events(events, table_name):
    conn = sq.connect('EventsDB.db')
    conn.execute(
        f"CREATE TABLE {table_name} ([ID] INTEGER PRIMARY KEY, [Title] TEXT, [Organiser] TEXT, [Location] TEXT, [Start_date] Text, [Interesting_URL] Text, [Non_Interesting_URL] Text);"
    )

    for event in events:
        date = event.get('date', " ")
        location = event.get('location', " ")
        organiser = event.get('organiser', " ")
        title = event.get('title', " ")

        int_urls_ls = event.get('int_urls')
        if isinstance((int_urls_ls), set):
            int_urls = " ".join(int_urls_ls)
        else:
            int_urls = " "

        non_int_urls_ls = event.get('non_int_urls')
        if isinstance((non_int_urls_ls), set):
            non_int_urls = " ".join(non_int_urls_ls)
        else:
            non_int_urls = " "

        conn.execute(
            f"INSERT INTO {table_name} (Title, Organiser, Location, Start_date, Interesting_URL, Non_Interesting_URL) VALUES (?,?,?,?,?,?);", (title, organiser, location, date, int_urls, non_int_urls)
        )

    conn.commit()
    print("Inserted into DB")
    conn.close()

if __name__ == '__main__':
    events = create_event_list()
    event_meta = list()
    for event in events[:10]:
        meta_data = scrape(event)
        card = get_details(meta_data)
        int_url, non_int_url = get_urls(event)
        card['int_urls'] = int_url
        card['non_int_urls'] = non_int_url

        print("======== Interesting URLs ========")
        for url in int_url:
            print(url)

        print("======== Non - Interesting URLs ========")
        for url in non_int_url:
            print(url)

        event_meta.append(card)

    save_events(event_meta, 'EventBrite')

# https://hackingandslacking.com/scrape-structured-data-with-python-and-extruct-ee1305493307