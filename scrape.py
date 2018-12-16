import requests
from bs4 import BeautifulSoup

def scrape (url):

    response = requests.get(url)
    assert response.status_code >= 200 and response.status_code < 300

    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    links = [link for link in soup.find_all("a") if link.get("href")[-4:] == ".tgz"]

    return links