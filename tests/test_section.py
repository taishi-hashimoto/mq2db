
from pathlib import Path
from mq2db import Mq2db

HERE = Path(__file__).parent

def test_section():
    mq2db = Mq2db(HERE / "test_section.yaml", "mq2db.specific.configurations")
    print(mq2db)
