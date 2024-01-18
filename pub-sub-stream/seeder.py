import json

from model.biography import Biography
from streamchannel.streams import StreamProducer


def map_to_message(bio: Biography) -> str:
    return json.dumps( bio.__dict__)


def run():
    biographies = [
        Biography("Clarissa Harlow", "A young woman whose quest for virtue is continually thwarted by her family."),
        Biography("Pamela Andrews", "A young woman whose virtue is rewarded."),
        Biography("Harriet Byron", "An orphan, and heir to a considerable fortune of fifteen thousand pounds"),
        Biography("Charles Grandison", "A man of feeling who truly cannot be said to feel")
    ]
    for bio in biographies:
        with StreamProducer(Biography, map_to_message) as channel:
            channel.send(bio)
            print("Sent Message: ", map_to_message(bio))


if __name__ == "__main__":
    run()




