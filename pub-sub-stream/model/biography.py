from dataclasses import dataclass

from streamchannel.streams import Record


class Biography(Record):
    def __init__(self, id: str, description: str) -> None:
        super().__init__(id)
        self.id = id
        self.description = description
