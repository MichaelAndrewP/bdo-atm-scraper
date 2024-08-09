BDO ATM Scraper
This Python script scrapes ATM location data from the BDO website for specified areas. The script fetches the HTML content of the pages, parses the data, and extracts relevant information such as the name, address, and geographical coordinates of each ATM.

Prerequisites
Python 3.x
requests library
beautifulsoup4 libraryBDO ATM Scraper

GOOGLE_MAPS_API_KEY
GOOGLE_APPLICATION_CREDENTIALS (service account json)

```sh

pip install .
```

```sh
Usage

1. Clone the repository or download the script:


2. Update the areas list in the script with the area values you want to scrape:

areas = [11, 22, 921] # Example area values for Makati, Taguig, and Ortigas

3. Run the script:

python scrape.py

4. Current output

The script will scrape data from BDO's ATM Locations and model these data to be saved into your firebase firestore database

```
