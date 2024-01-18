import json
from queue import Queue
from threading import Thread
import time

import mysql.connector

from model.biography import Biography
from streamchannel.streams import polling_consumer, cancellation_token


def map_from_message(message_body: str) -> Biography:
    vals = json.loads(message_body)
    return Biography(vals["id"], vals["description"])


def handle_message(bio: Biography) -> bool:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="Lookup"
    )

    cursor = mydb.cursor()

    sql = "INSERT INTO Biography (Id, Description) VALUES (%s, %s)"
    val = (bio.id, bio.description)
    cursor.execute(sql, val)

    mydb.commit()
    return True


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=polling_consumer,
                          args=(cancellation_queue, Biography, map_from_message, handle_message, 'localhost:9092'),
                          daemon=True)

    polling_loop.start()

    while True:
        try:
            time.sleep(1)  # yield, delays responsiveness to keyboard interrupt though
        except KeyboardInterrupt:
            print("Shutting down consumer")
            cancellation_queue.put(cancellation_token)  # this will terminate the worker
            polling_loop.join(timeout=30)  # wait for orderly termination, if not when process ends demon will die
            break


if __name__ == "__main__":
    run()
