from setuptools import setup, find_packages

setup(
    name='bdo_atm_loc_scraper',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'google-cloud-firestore',
        'googlemaps',
        'requests',
        'beautifulsoup4',
        'geohash2',
        'python-dotenv',
        'pytz'
    
    ],
)