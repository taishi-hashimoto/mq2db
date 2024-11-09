import zmq
import time
import yaml
import json
from os import makedirs
from os.path import expanduser, dirname
from datetime import datetime, timedelta, timezone
from threading import Thread, Event
from importlib import import_module
import sqlalchemy



class JsonLoader:
    "Default JSON loader."

    def __init__(self, verbose: bool = False):
        self._verbose = verbose

    def __call__(self, data: bytes) -> dict:
        "Return dict representation of data, assuming it is JSON."
        result = json.loads(data)
        if self._verbose:
            print(result)
        return result


class TextLoader:
    "Default text (utf-8) loader."

    def __init__(self, verbose: bool = False):
        self._verbose = verbose

    def __call__(self, data: bytes) -> dict:
        result = data.decode()
        if self._verbose:
            print(result)
        return result


class _Worker(Thread):
    "Worker thread processing each target."

    def __init__(self, name: str, conf: dict):
        """Initialize the worker thread.
        
        Parameters
        ==========
        name: str
            Target name.
        conf: dict
            Configuration for this worker thread."""
        super().__init__()
        self._stop_event = Event()
        self._name = name
        self._conf = conf
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.SocketType.SUB)
        self._sock.subscribe("")
        self._sock.connect(self._conf["address"])

        # Dynamically import the loader class.
        module_name, class_name = self._conf["loader"]["class"].rsplit(".", 1)
        module = import_module(module_name)
        self._loader = getattr(module, class_name)(
            *self._conf["loader"].get("args", []),
            **self._conf["loader"].get("kwargs", {}))

        # Prepare database configuration and URL.
        dbconf = self._conf.get("database")
        self._db_url: str = dbconf["url"]
        if self._db_url.startswith("sqlite"):
            self._db_url = expanduser(self._db_url)
            db_root = dirname(self._db_url.split("sqlite:///")[1])
            makedirs(db_root, exist_ok=True)

        # Table settings.
        self._auto_datetime = dbconf.get("_datetime_", False)
        self._auto_raw = dbconf.get("_raw_", False)
        primary_key = dbconf.get("primary_key", ["_datetime_"] if self._auto_datetime else None)
        columns_spec = [",\n".join(f"  {key} {val}" for key, val in dbconf["columns"].items())]  # User specified columns
        if self._auto_datetime:
            columns_spec.insert(0, "  _datetime_ DATETIME NOT NULL")
        if self._auto_raw:
            columns_spec.append("  _raw_ BLOB NOT NULL")  # Special, raw bytes
        if primary_key is not None:
            columns_spec.append(f"  PRIMARY KEY({','.join(primary_key)})")
        columns_spec = ",\n".join(columns_spec)
        self._sql_table = sqlalchemy.text(f"CREATE TABLE IF NOT EXISTS {self._name} (\n{columns_spec});")
        
        # Indices settings.
        indices = dbconf.get("indices")
        self._sql_indices = []
        if indices:
            for index, columns in indices.items():
                self._sql_indices.append(
                    sqlalchemy.text(f"CREATE INDEX IF NOT EXISTS {index} ON {self._name}({','.join(columns)});"))

        # Insert statement.
        self._columns = list(dbconf["columns"])
        insert_columns = self._columns.copy()
        if self._auto_datetime:
            insert_columns.append("_datetime_")
        if self._auto_raw:
            insert_columns.append("_raw_")
        placeholders = ",".join(f":{column}" for column in insert_columns)
        self._sql_insert = sqlalchemy.text(
            f"""
            INSERT INTO {self._name}({', '.join(insert_columns)})
            VALUES({placeholders})
            """)

        # Interval for database writing.
        self._interval = timedelta(**dbconf["interval"])

    def stop(self):
        self._stop_event.set()

    def run(self):
        rows = []
        now = prev = datetime.now(timezone.utc)
        while not self._stop_event.is_set():
            try:
                data = self._sock.recv(zmq.NOBLOCK)
                now = datetime.now(timezone.utc)
                dict_repr = self._loader(data)
                if self._auto_datetime:
                    dict_repr["_datetime_"] = now
                if self._auto_raw:
                    dict_repr["_raw_"] = data
                rows.append(dict_repr)
            except zmq.ZMQError:
                time.sleep(0.1)
            
            if now - prev > self._interval:
                try:
                    url = now.strftime(self._db_url)
                    with sqlalchemy.create_engine(url).connect() as con:
                        with con.begin():
                            con.execute(self._sql_table)
                            for each in self._sql_indices:
                                con.execute(each)
                            con.execute(self._sql_insert, rows)
                            rows.clear()
                    prev = now
                except:
                    import traceback
                    traceback.print_exc()


class Mq2db:
    "mq2db controller."

    def __init__(self, path_yaml: str):
        """Intializes each listener/writer thread.
        
        Parameters
        ==========
        path_yaml: str
            Path to YAML configuration file.
        """
        with open(path_yaml) as f:
            self._conf = yaml.safe_load(f)
        self._threads: list[_Worker] = []
        for name, conf in self._conf["targets"].items():
            thread = _Worker(name, conf)
            self._threads.append(thread)

    def start(self):
        "Start each listener/writer threads."
        for thread in self._threads:
            thread.start()
        print()
        print("-- LOGGING STARTED --")
        print()
        print("Press Ctrl + C to exit.")
        print() 
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                for thread in self._threads:
                    thread.stop()
                break
        print("Stopping...")
        for thread in self._threads:
            thread.join()
        print()
        print("-- LOGGING STOPPED --")
        print()
