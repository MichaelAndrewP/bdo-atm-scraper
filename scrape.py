import os
import requests
import geohash2 as geohash
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import googlemaps
from dotenv import load_dotenv
from google.cloud import firestore
import pytz  # Import pytz for time zone conversion

# Load environment variables from .env file
load_dotenv()

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Initialize Google Maps client with API key from environment variable
gmaps = googlemaps.Client(key=os.getenv('GOOGLE_MAPS_API_KEY'))

# Initialize Firestore client
db = firestore.Client()

# Base URL to fetch the HTML content, this is for metro manila only
# &area parameter represents an area on interest
# Branch_location represents the province, Metro Manila = 2, Luzon = 3, Be sure to change this if needed
# Makati = 11, Taguig = 22, Ortigas = 921, Pampanga = 41

base_url = 'https://www.bdo.com.ph/branches-atms-locator-0?type=atm&branch_location=2&area={area}&keyword=&title=&aid=0&form_build_id=form-MzZyboi6IBXkcLIYApR9muiRba7F18QO74gIGDF1txY&form_id=branch_atm_page_form'

# List of area values
# 11,22,921
areas = [13]

# Define the bank document path as a variable
bank_document_path = 'banks/97EvAbFBAF1J8X7eMaYG'

# Function to fetch HTML content
def fetch_html(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36 Edg/127.0.0.0'}
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    return response.text

# Function to perform reverse geocoding
def reverse_geocode(lat, lng):
    results = gmaps.reverse_geocode((lat, lng))
    
    address = {
        'city': '',
        'country': '',
        'fullAddress': '',
        'postalCode': '',
        'stateProvince': '',
        'streetAddress': ''
    }
    
    for result in results:
        address_components = result['address_components']
        
        for component in address_components:
            if 'locality' in component['types']:
                address['city'] = component['long_name']
            elif 'country' in component['types']:
                address['country'] = component['short_name']
            elif 'postal_code' in component['types']:
                address['postalCode'] = component['long_name']
            elif 'administrative_area_level_1' in component['types']:
                address['stateProvince'] = component['long_name']
            elif 'route' in component['types']:
                address['streetAddress'] = component['long_name']
        
        # If we have at least one of the required components, we can use this result
        if any(address.values()):
            address['fullAddress'] = result['formatted_address']
            return address
    
    return None

# Function to transform item into the new model
def transform_item(item):
    # Convert current time to Asia/Manila time zone
    manila_tz = pytz.timezone('Asia/Manila')
    current_time = datetime.now(manila_tz)
    
    geohash_code = geohash.encode(item['geopoint']['lat'], item['geopoint']['lng'])
    external_id = f"{item['name']}_{current_time.strftime('%Y%m%d%H%M%S')}"
    address_details = reverse_geocode(item['geopoint']['lat'], item['geopoint']['lng'])
    geopoint = firestore.GeoPoint(item['geopoint']['lat'], item['geopoint']['lng'])
    
    # Create a reference to the bank document using the variable
    bank_ref = db.document(bank_document_path)
    
    return {
        'address': address_details if address_details else {
            'city': "",
            'country': "PH",
            'fullAddress': item['address'],
            'postalCode': "",
            'stateProvince': "",
            'streetAddress': ""
        },
        'bank': bank_ref,
        'createdAt': current_time,
        'updatedAt': current_time,
        'externalId': external_id,
        'id': '',  # Leave id empty initially
        'lastReportedStatus': {
            'reportedBy': {
                'appVersion': '',
                'deviceId': '',
                'deviceModel': '',
                'osVersion': ''
                        },
            'status': 'online', 
            'timestamp': current_time,
        },       
        'location': {
            'geohash': geohash_code,
            'geopoint': geopoint
        },
        'name': item['name'],
        'qrCode': 'https://example.com/qrcode/ ',
        'status': '',
          'addedBy': 'admin',
    }

# Function to scrape data
def scrape_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    items = []

    for row in soup.select('.views-row'):
        name_elem = row.select_one('.views-field-title .field-content')
        address_elem = row.select_one('.views-field-body .field-content')
        href_elem = row.select_one('.views-field-nothing .field-content a')

        if name_elem and address_elem and href_elem:
            name = name_elem.text.strip()
            address = address_elem.text.strip()
            href = href_elem['href']

            # Parse the latitude and longitude from the href
            parsed_url = urlparse(href)
            query_params = parse_qs(parsed_url.query)
            latitude = query_params.get('latitude', [None])[0]
            longitude = query_params.get('longitude', [None])[0]

            if latitude and longitude:
                geopoint = {'lat': float(latitude), 'lng': float(longitude)}
            else:
                geopoint = None

            item = {
                'name': name,
                'address': address,
                'href': href,
                'geopoint': geopoint
            }

            # Only transform and add the item if geopoint is not None
            if geopoint:
                items.append(transform_item(item))

    return items

# Function to get the number of pages
def get_num_pages(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pager_total = soup.select_one('.pager-total a')
    if pager_total:
        last_page_num = int(pager_total.text.strip())
        return last_page_num + 1
    
    # If pager-total is not available, get the last pager-item
    pager_items = soup.select('.pager-item a')
    if pager_items:
        last_page_num = int(pager_items[-1].text.strip())
        return last_page_num + 1
    
    return 1

# Main function
def main():
    all_data = []
    not_saved_count = 0

    for area in areas:
        area_url = base_url.format(area=area)
        initial_html = fetch_html(area_url)
        num_pages = get_num_pages(initial_html)

        for page in range(num_pages):
            url = f"{area_url}&page={page}"
            print(f"Fetching data from: {url}")
            html_content = fetch_html(url)
            print(f"Scraping data from: {url}")  # Add this line to print the URL being scraped
            data = scrape_data(html_content)
            all_data.extend(data)
    
    for item in all_data:
        print(item)
        try:
            # Check if the item already exists in Firestore
            existing_docs = db.collection('atms').where('name', '==', item['name']).stream()
            if any(existing_docs):
                print(f"Item with name {item['name']} already exists. Skipping.")
                not_saved_count += 1
                continue
            
            # Save each item to Firestore and get the document reference
            doc_ref = db.collection('atms').add(item)[1]
            # Update the item with the document ID
            item['id'] = doc_ref.id
            # Update the document with the new ID
            db.collection('atms').document(doc_ref.id).set(item)
        except Exception as e:
            print(f"Error saving item to Firestore: {e}")
    
    print(f"Total number of objects: {len(all_data)}")
    print(f"Number of objects not saved because it already exists OR raw data is not available: {not_saved_count}")

if __name__ == "__main__":
    main()