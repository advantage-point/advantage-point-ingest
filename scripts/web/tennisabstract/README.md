# TennisAbstract

The general steps of ingesting TennisAbstract data into Google Cloud Storage are:
1. Get URL list
    - A list of URLs to scrape is either retrieved from a site containing a list (ex. matches) or constructed using information from other sources (ex. players, tournaments).
    - Additional data, either parsed from the url or used in url creation, is included with the returned list.
2. Get data list
    - URLs are grouped into batches for multiple reasons:
        - If a certain web element (ex. Selenium browser, Puppeteer page) is required to access webpage contents, a balance must be struck between using one element for ALL urls and opening/closing an element each time a url is visited.
        - There is a risk of exceeding any in-memory storage if the data size is large.
    - Data is scraped from each url and appended to the 'url data' from the earlier step.
3. Create dataframe
    - Dataframe provides flexibility in handling data and prepping it for SQL data type compatibility.
    - Because data types are unknown and subject to inconsistency, dataframes allow for 'stringify'-ing all incoming data.
        - Handles cases in which records may contain both quoted and unquotes values (ex. 2 and '2').
        - Handles cases in which complex data structures (ex. dictionary, list) exist even if the data warehouse does not provide compatibility for such structures.
4. Write to GCS
    - Data is uploaded to Cloud Storage.
    - Data is written in NDJSON format.