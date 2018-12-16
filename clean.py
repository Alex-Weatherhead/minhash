import re
import string
import html

def remove_in_text_citations (text):
    """
    Removes in-text citations by naively tossing out everything in 
    () and [] brackets. This approach was chosen because there are 
    too many possible styles of in-text citation to consider for any 
    non-trivial collection of documents.
    While this approach may result in a loss of content, it should be
    minimal, and a worthwhile exchange. 
    """
    
    pattern = r"\(.*\)|\[.*\]"

    text = re.sub(pattern, "", text)

    return text

def simplify_punctuation (text):
    """
    Removes all ASCII characters considered to be punctuation,
    except for those which represent the end of a sentence, 
    (e.g. '.', '?', '!'), which are all replaced with "." for
    simplicity.
    """

    punctuation = string.punctuation # All punctuation characters.

    for character in punctuation[:12] + punctuation[13:]:
        if character in ".?!":
            text = text.replace(character, ".")
        else:
            text = text.replace(character, "")

    character = punctuation[12] # the "-" character
    text = text.replace(character + "\n", "") # If the "-" character is followed by a newline, then we want to replace it with an empty string.
    text = text.replace(character, " ") # If the "-" character is not followed by a newline, then we want to replace it with a space.

    return text

def standardize_whitespace_characters (text):
    """
    Replaces all ASCII characters considered to be whitespace with a standard space.
    This function has the added effect of removing leading and trailing whitespaces,
    and reducing consecutive occurrences of whitespace characters to a single
    occurrence.
    """

    text = " ".join(text.split())

    return text

def clean (text):
    """
    Carries out the following seven tasks on the given text:
    
    (1) Remove all bracketed text as a means of naively removing in-text citations;
    (2) Replaces instances of "&amp;quot;" with "&quot;" in order to correct what seems to be an issue with the ParCit parser;
    (3) Calls the html unescape function to decode all html entities;
    (4) Removes all non-sentence-ending punctuation and replaces "?" and "!" with ".". 
    (5) Converts all letters to their lowercase forms.
    (6) Removes all characters except for lowercase letters, whitespace characters, and periods.
    (7) Replaces potentially annoying whitespace characters, and sequences with a single space;
    """

    text = remove_in_text_citations(text)
    text = text.replace("&amp;quot;", "&quot;")
    text = html.unescape(text)
    text = simplify_punctuation(text)
    text = text.lower()
    text = ".".join(["".join([letter if letter in string.ascii_lowercase or letter in string.whitespace or letter == "." else "" for letter in text]).strip() for text in text.split(".")])
    text = standardize_whitespace_characters(text)

    return text