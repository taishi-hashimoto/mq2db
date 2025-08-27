import time
from pathlib import Path
from threading import Thread
import yaml
from datetime import datetime
from secrets import token_hex, choice, randbelow
import zmq
from mq2db import Mq2db

HERE = Path(__file__).parent 
    

def produce_csv_messages():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("ipc:///tmp/mq2db-test_csvloader")
    while True:
        nmsgs = randbelow(10) + 1
        # SSR like dummy messages.
        msgs = []
        for _ in range(nmsgs):
            t = datetime.now().isoformat()
            rssi = randbelow(500) / 10
            msg = token_hex(choice([7, 14]))
            msgs.append(f"{t},{rssi},{msg}")
        socket.send_string("\n".join(msgs), zmq.DONTWAIT)
        time.sleep(0.1)


def test_csvloader():
    # Start producer thread.
    producer_thread = Thread(target=produce_csv_messages, daemon=True)
    producer_thread.start()
    
    time.sleep(0.1)

    mq2db = Mq2db(HERE / "test_csvloader.yaml")
    mq2db.start()
