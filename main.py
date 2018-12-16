import os

from scrape import scrape
from fetch import fetch
from clean import clean

FILENAME = "corpus.txt"
URL = "http://acl-arc.comp.nus.edu.sg/archives/acl-arc-160301-parscit/"

if os.path.exists(FILENAME):
    os.remove(FILENAME)

links = scrape(URL)
for link in links:
    name_to_text = fetch(URL + "/" + link.get("href"), link.text)
    with open(FILENAME, "a+") as corpus:
        for name in name_to_text:
            text = name_to_text[name]
            cleaned_text = clean(text)
            corpus.write("(" + name + "," + cleaned_text + ")\n")