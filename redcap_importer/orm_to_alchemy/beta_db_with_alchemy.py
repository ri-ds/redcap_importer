# This file has the same function as beta/models.py, which uses Django ORM;
# this is implemented using SQL Alchemy Core.

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, String, Text, Boolean
from sqlalchemy import ForeignKey

engine = create_engine("sqlite+pysqlite:///beta.sqlite3", echo=True)

metadata_obj = MetaData()

project_root = Table(
    "project_root",
    metadata_obj,
    Column("record_id", String(255), primary_key=True),
    Column("record_id_display", Text, nullable=True),
)

my_first_instrument = Table(
    "my_first_instrument",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("project_root_id", Integer, ForeignKey("project_root.record_id", ondelete="CASCADE")),
    Column("redcap_repeat_instance", Integer, nullable=True),
    Column("record_id", Text, nullable=True),
    Column("how_other", Text, nullable=True),
    Column("challenges_other", Text, nullable=True),
    Column("distance", Text, nullable=True),
    Column("distance_display_value", Text, nullable=True),
    Column("computer", Boolean, nullable=True),
    Column("usephone", Boolean, nullable=True),
    Column("useapps", Text, nullable=True),
    Column("do_you_use_facebook", Boolean, nullable=True),
    Column("does_the_vip_use_facebook", Boolean, nullable=True),
    Column("connect", Text, nullable=True),
)

my_first_instrument_how_lookup = Table(
    "my_first_instrument_how_lookup",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("my_first_instrument_id", BigInteger, ForeignKey("my_first_instrument.id", ondelete="CASCADE")),
    Column("how", Text),
    Column("how_display_value", Text),
)

my_first_instrument_communication_lookup = Table(
    "my_first_instrument_communication_lookup",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("my_first_instrument_id", BigInteger, ForeignKey("my_first_instrument.id", ondelete="CASCADE")),
    Column("communication", Text),
    Column("communication_display_value", Text),
)

my_first_instrument_challenges_lookup = Table(
    "my_first_instrument_challenges_lookup",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("my_first_instrument_id", BigInteger, ForeignKey("my_first_instrument.id", ondelete="CASCADE")),
    Column("challenges", Text),
    Column("challenges_display_value", Text),
)

metadata_obj.create_all(engine)