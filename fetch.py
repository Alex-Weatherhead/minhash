import os
from urllib.request import urlretrieve
import tarfile
from lxml import etree

def get_text_from_element_tree (root):
    """
    Performs a depth-first walk through the element tree starting from root, 
    and concatenates the text from each bodyText and listItem tag.

    Args:
        root - the root node for the element tree.

    Returns:
        The text from each bodyText and listItem tag in the element tree concatenated together.
    """

    text = ""
    for child in root:
        if child.tag == "bodyText":
            text += child.text + " "
        if child.tag == "listItem":
            text += child.text + " "
        else:
            text += get_text_from_element_tree(child)

    return text

def fetch (url, filename):
    """
    Retrieves the tarfile at the given url, and then extracts and parses each file within it.
    After each file in the tarfile has been processed, the tarfile is deleted.

    Args:
        url - the url at which the tarfile can be found.
        filename - the filename under which to temporarily download the tarfile.

    Returns:
        A dictionary which maps each filename to the text parsed from said file.
    """

    name_to_text = {}

    urlretrieve(url, filename)
    archive = tarfile.open(name=filename, mode="r:gz")

    names = archive.getnames()
    members = archive.getmembers()
    for name, member in zip(names, members):

        print(name)

        extracted_file = archive.extractfile(member)
        xml = extracted_file.read() 

        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml, parser)
        
        text = get_text_from_element_tree(root)
        
        name_to_text[name] = text
    
    os.remove(filename)

    return name_to_text