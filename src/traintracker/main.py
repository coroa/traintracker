"""
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from sqlite3 import Connection

import arrow
import requests
import typer
from rich import print

from .const import API_PREFIX
from .model import Departure, Message, ResolveError, Station
from .utils import as_time, fields_from_schema, flatten_dict, placeholders

_logger = logging.getLogger(__name__)


def departures_to_sqlite(
    db: Connection | str | Path,
    table: str,
    departures: list[Departure],
    replace_date=True,
):
    if not isinstance(db, Connection):
        db = Connection(db)

    cur = db.cursor()

    if not departures:
        return

    sixhoursago = arrow.now().shift(hours=-6, minutes=-30)

    fields = list(fields_from_schema(Departure.schema()))
    cur.execute(
        'CREATE TABLE IF NOT EXISTS "{table}"({fields})'.format(
            table=table, fields=", ".join(f'"{f}"' for f in fields)
        )
    )

    dates = set(d.date for d in departures)
    stations = set(d.station.name for d in departures)
    if replace_date:
        cur.execute(
            f'DELETE FROM "{table}" WHERE '
            f"date IN ({placeholders(dates)}) AND "
            f"station_name IN ({placeholders(stations)}) AND "
            f"actual_time > ?",
            tuple(dates) + tuple(stations) + (sixhoursago.datetime,),
        )

    defaults = {
        "message_timestamp": None,
        "message_value": None,
        "message_text": None,
    }
    data = [
        defaults | flatten_dict(d.dict())
        for d in departures
        if d.actual_time > sixhoursago
    ]
    cur.executemany(
        f'INSERT INTO "{table}" VALUES({placeholders(fields, named=True)})', data
    )

    db.commit()


def decompose_departure(station, date, d):
    def summarize_delays(delays):
        if not delays:
            return None

        earliest_delay = min(delays, key=lambda m: m["timestamp"])
        return Message(**earliest_delay)

    scheduled = as_time(d["departure"]["scheduledTime"])
    actual = as_time(d["departure"]["time"])
    delay = (actual - scheduled).total_seconds() // 60

    return Departure(
        station=station,
        date=date,
        name=d["train"]["name"],
        number=d["train"]["number"],
        start=d["route"][0]["name"],
        end=d["route"][-1]["name"],
        starting_time=as_time(d["initialDeparture"]).datetime,
        scheduled_time=scheduled.datetime,
        actual_time=actual.datetime,
        delay=delay,
        message=summarize_delays(d["messages"]["delay"]),
    )


def request_departures(station: Station):
    # get all previous trains for the day (the station is cached)
    res = requests.get(
        f"{API_PREFIX}/iris/v2/abfahrten/{station.id}",
        params=dict(lookahead=0, lookbehind=7 * 60),
    )
    res.raise_for_status()

    response = res.json()
    departures = response.get("lookbehind")
    if departures:
        day = as_time(departures[0]["initialDeparture"]).date()
        trains = [
            decompose_departure(station, day, d) for d in departures if "departure" in d
        ]

    # if warn_about_following_trains and response.get("departures"):
    #     print(
    #         f":warning: [bold red]Further trains planned for today for {station.name}"
    #     )

    return trains


# ---- CLI ----


app = typer.Typer()


@app.command()
def main(
    stations: list[str],
    db_file: Path = typer.Option("trains.db", "--file", "-f", help="SQLite file"),
    table: str = typer.Option("trains", "--table", "-t", help="Table to append to"),
):
    """
    Grab all trains and delays that passed through the given stations and add them to
    `table` in `db_file`
    """
    try:
        stations = [Station.from_search(station) for station in stations]
    except ResolveError as e:
        print(*e.args)
        raise typer.Exit()

    print("Found [green]all[/green] stations:", stations)

    with ThreadPoolExecutor() as exec:
        departures = sum(exec.map(request_departures, stations), [])

    departures_to_sqlite(db_file, table, departures)


if __name__ == "__main__":
    app()
