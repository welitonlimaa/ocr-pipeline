import re


def simple_sentence_split(text: str):
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return sentences
