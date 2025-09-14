"mq2db core"
import os
import time
import json
import csv
import traceback
from pprint import pprint
from typing import Optional, List, Any
from os import makedirs
from os.path import expanduser, dirname
from datetime import datetime, timedelta, timezone
from threading import Thread, Event
from importlib import import_module
import zmq
import yaml
import sqlalchemy


class DictLoader:
    "Default loader for Python dict object."

    def __init__(self, verbose: bool = False) -> None:
        self._verbose = verbose

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        if self._verbose:
            pprint(data, sort_dicts=False)
        return data


class JSONLoader:
    "Default JSON loader."

    def __init__(self, verbose: bool = False):
        self._verbose = verbose

    def __call__(self, data: bytes) -> dict:
        "Return dict representation of data, assuming it is JSON."
        result = json.loads(data)
        if self._verbose:
            pprint(result, sort_dicts=False)
        return result


class TextLoader:
    "Default text (utf-8) loader."

    def __init__(self, verbose: bool = False):
        self._verbose = verbose

    def __call__(self, data: bytes) -> str:
        result = data.decode()
        if self._verbose:
            pprint(result, sort_dicts=False)
        return result


class CSVLoader:
    "Default comma-separated-variables (CSV) loader."

    def __init__(self, encoding: str = "utf-8", header: Optional[List[str]] = None, verbose: bool = False):
        self._encoding = encoding
        self._header = header
        self._verbose = verbose

    def __call__(self, data: bytes) -> List[dict[str, str]]:
        result = []
        for line in csv.DictReader(data.decode(self._encoding).splitlines(), fieldnames=self._header):
            line_data = {}
            for key, value in line.items():
                line_data[key.strip()] = value.strip()
            result.append(line_data)
        if self._verbose:
            pprint(result, sort_dicts=False)
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
        mq_type: str = conf.get("type", "sub").upper()
        self._sock = self._ctx.socket(zmq.SocketType[mq_type])
        if mq_type == "SUB":
            topic = conf.get("topic", "")
            self._sock.subscribe(topic)
        mq_method: str = conf.get("method", "connect").lower()
        getattr(self._sock, mq_method)(self._conf["address"])
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.setsockopt(zmq.RCVTIMEO, 100)

        # recv method name
        recv_method = self._conf.get("recv", {"method": "recv_pyobj"}).get("method", "recv_pyobj")
        self._recv = getattr(self._sock, recv_method)

        # Dynamically import the loader class.
        default_loader = {"class": "mq2db.DictLoader"}
        module_name, class_name = self._conf.get("loader", default_loader).get("class", "mq2db.DictLoader").rsplit(".", 1)
        module = import_module(module_name)
        self._loader = getattr(module, class_name)(
            *self._conf.get("loader", default_loader).get("args", []),
            **self._conf.get("loader", default_loader).get("kwargs", {}))

        # Prepare database configuration and URL.
        dbconf = self._conf["database"]
        self._db_url: str = dbconf["url"]
        if self._db_url.startswith("sqlite"):
            db_path = expanduser(self._db_url.split("sqlite:///")[1])
            self._db_url = "sqlite:///" + db_path
            makedirs(dirname(db_path), exist_ok=True)

        # Table settings.
        self._auto_datetime = dbconf.get("_datetime_", False)
        self._auto_timestamp = dbconf.get("_timestamp_", False)
        self._auto_raw = dbconf.get("_raw_", False)
        primary_key: list[str] = dbconf.get("primary_key", ["_datetime_"] if self._auto_datetime else None)
        unique: dict = dbconf.get("unique")
        # User specified columns
        columns_spec = [",\n".join(f"  {key} {val}" for key, val in dbconf.get("columns", {}).items())]
        if self._auto_datetime:
            columns_spec.insert(0, "  _datetime_ DATETIME NOT NULL")
        if self._auto_timestamp:
            columns_spec.insert(0, "  _timestamp_ INTEGER NOT NULL")
        if self._auto_raw:
            columns_spec.append("  _raw_ BLOB NOT NULL")  # Special, raw bytes
        if primary_key is not None:
            columns_spec.append(f"  PRIMARY KEY({','.join(primary_key)})")
        if unique:
            for name, columns in unique.items():
                columns_spec.append(f"  CONSTRAINT {name} UNIQUE({','.join(columns)})")
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
        insert_prefix = dbconf.get("insert_prefix", "")
        self._columns = list(dbconf.get("columns", {}).keys())
        insert_columns = self._columns.copy()
        if self._auto_datetime:
            insert_columns.append("_datetime_")
        if self._auto_timestamp:
            insert_columns.append("_timestamp_")
        if self._auto_raw:
            insert_columns.append("_raw_")
        placeholders = ",".join(f":{column}" for column in insert_columns)
        self._sql_insert = sqlalchemy.text(
            f"""
            INSERT {insert_prefix} INTO {self._name}({', '.join(insert_columns)})
            VALUES({placeholders})
            """)

        # Interval for database writing.
        self._interval = timedelta(**dbconf.get("interval", {"seconds": 1}))
        self._sql_init = [
            sqlalchemy.text(cmd) for cmd in dbconf.get("init", [])
        ]

    def stop(self):
        "Stop the worker thread."
        self._stop_event.set()

    def flush(self, now: datetime, rows: list[dict]):
        "Flush stored rows into the database."
        if not rows:
            return
        url = now.strftime(self._db_url)
        with sqlalchemy.create_engine(url, isolation_level="AUTOCOMMIT").connect() as con:
            with con.begin():
                for cmd in self._sql_init:
                    con.execute(cmd)
                con.execute(self._sql_table)
                for each in self._sql_indices:
                    con.execute(each)
                con.execute(self._sql_insert, rows)
                rows.clear()

    def run(self):
        now = prev = datetime.now(timezone.utc)
        rows = []
        while not self._stop_event.is_set():
            # Add new data in rows.
            try:
                data = self._recv()
                now = datetime.now(timezone.utc)
                list_of_dict = self._loader(data)
                if not isinstance(list_of_dict, list):
                    list_of_dict = [list_of_dict]
                for each in list_of_dict:
                    if self._auto_datetime:
                        each["_datetime_"] = now
                    if self._auto_timestamp:
                        each["_timestamp_"] = int(now.timestamp())
                    if self._auto_raw:
                        each["_raw_"] = data
                    dict_repr = {key: None for key in self._columns}
                    dict_repr.update(each)
                    rows.append(dict_repr)
            except zmq.Again:
                continue
            # Flush and clean rows
            if now - prev > self._interval:
                try:
                    self.flush(now, rows)
                    prev = now
                except Exception:
                    traceback.print_exc()
        self.flush(now, rows)


class Mq2db:
    "mq2db controller."

    def __init__(self, path_yaml: os.PathLike, section: Optional[str] = None):
        """Intializes each listener/writer thread.
        
        Parameters
        ==========
        path_yaml: os.PathLike
            Path to YAML configuration file.
        section: Optional[str]
            Section name in the YAML file for mq2db specific configuration.
            Something like "mq2db.specific.setting".
        """
        with open(path_yaml, encoding="utf-8") as f:
            self._conf = yaml.safe_load(f)
        self._threads: list[_Worker] = []
        if section is not None:
            self._conf = self._get_section(self._conf, section)
        for name, conf in self._conf["targets"].items():
            thread = _Worker(name, conf)
            self._threads.append(thread)

    def _get_section(self, conf: dict, section: str) -> dict:
        "Get a subsection of the configuration."
        for sec in section.split("."):
            conf = conf[sec]
        return conf

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

    def __str__(self) -> str:
        # Show typical settings in the conf.
        settings = []
        for target, each in self._conf["targets"].items():
            addr = each["address"]
            type = each.get("type", "sub").upper()
            method = each.get("method", "connect")
            settings.append(f"{target}: {addr} ({type}, {method})")
        return "\n".join(settings)

    def __repr__(self) -> str:
        return f"Mq2db({'|'.join(self.__str__().splitlines())})"
