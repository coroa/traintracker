from typing import Optional
import datetime
import requests
from pydantic import BaseModel
from rich.markup import escape

from .const import API_PREFIX


class Station(BaseModel):
    id: int
    name: str

    @classmethod
    def from_search(cls, search: str) -> "Station":
        return resolve_station(search, cls)


class Message(BaseModel):
    timestamp: datetime.datetime
    value: int
    text: str


class Departure(BaseModel):
    station: Station
    date: datetime.date
    name: str
    number: int
    start: str
    end: str
    starting_time: datetime.datetime
    scheduled_time: datetime.datetime
    actual_time: datetime.datetime
    delay: int
    message: Optional[Message] = None


class ResolveError(Exception):
    pass


def resolve_station(station: str, cls=Station):
    results = requests.get(f"{API_PREFIX}/stopPlace/v1/search/{station}").json()
    match results:
        case [{"evaNumber": number, "name": name}]:
            return cls(id=number, name=name)
        case [*_]:
            raise ResolveError(
                "[bold red]ERROR[/]  Multiple stations found for "
                f"[bold yellow]{escape(station)}[/]: ",
                {r["name"]: r["evaNumber"] for r in results},
            )
        case []:
            raise ResolveError(
                "[bold red]ERROR[/]  No station found for "
                f"[bold yellow]{escape(station)}[/]"
            )
        case _:
            raise ResolveError("No matches")
