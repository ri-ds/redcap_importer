import json

from django.core.management.base import BaseCommand, CommandError

import requests

from redcap_importer import models

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column
from sqlalchemy import BigInteger,  Boolean, Date, Float, ForeignKey, Integer, String, Text

# Create engine and metadata
engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
metadata_obj = MetaData()

# This class takes the metadata from the RedcapConnection Django Model and uses it
# to create SQL tables, via SQL Alchemy Core.
class Command(BaseCommand):
    help = "Populates tables for schema (Instrument and Event) but not Field definitions using SQL Alchemy"

    def add_arguments(self, parser):
        parser.add_argument("connection_name")

    def handle(self, *args, **options):
        connection_name = options["connection_name"]
        oProject = models.RedcapConnection.objects.get(unique_name=connection_name).projectmetadata

        # Dictionary to store all table objects.
        self.tables = {}
        
        # Generate SQL Alchemy Tables for each table.
        self.create_root_table(oProject)
        if oProject.is_longitudinal:
            self.create_event_table(oProject)
        for oInstrument in oProject.instrumentmetadata_set.all():
            self.create_instrument_table(oInstrument, oProject.is_longitudinal)
            self.create_lookup_tables(oInstrument)

        # Create all tables in the database from the saved SQL Alchemy tables.
        metadata_obj.create_all(engine)
        
        # Print summary.
        print("Successfully created {} tables:".format(len(self.tables)))
        for table_name in self.tables.keys():
            print(table_name)

    # Create the ProjectRoot table.
    def create_root_table(self, oProject):
        table_name = "project_root"
        table = Table(
            table_name,
            metadata_obj,
            Column(oProject.primary_key_field, String(255), primary_key=True), # Is this actually a CharField or should it be an Int?
            Column("{}_display".format(oProject.primary_key_field), Text, nullable=True),
        )
        
        # "Save" object to this class, to be created by the engine later.
        self.tables[table_name] = table
        
    # Create the RedcapEvent table for longitudinal studies.
    # This table is always the same aside from the ProjectRoot link, so can be mostly hard-coded.
    def create_event_table(self, oProject):
        table_name = "redcap_event"
        table = Table(
            table_name,
            metadata_obj,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("project_root_id", String(255), ForeignKey("project_root.{}".format(oProject.primary_key_field), ondelete="CASCADE")),
            Column("event_unique_name", Text),
            Column("event_label", Text),
            Column("arm_number", Integer),
            Column("redcap_repeat_instance", Integer, nullable=True),
        )
        
        # "Save" table, instantiated at program end
        self.tables[table_name] = table

    # Create a table for each instrument.
    def create_instrument_table(self, oInstrument, is_longitudinal):
        table_name = oInstrument.get_django_model_name().lower() 
        
        # Columns vary depending on the instrument, so create each Column object individually
        # and add them to a list.

        # These columns should appear in every instrument.
        columns = [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("redcap_repeat_instance", Integer, nullable=True),
        ]
        
        # Add "parent" foreign key.
        if is_longitudinal:
            # RedcapEvent is standardized, so again hard-defined.
            columns.append(Column("redcap_event_id", Integer, ForeignKey("redcap_event.id", ondelete="CASCADE")))
        else:
            # Need to get the primary key field from the project.
            pk_field = oInstrument.project.primary_key_field
            columns.append(Column("project_root_id", String(255), ForeignKey("project_root.{}".format(pk_field), ondelete="CASCADE")))
        
        # Add fields for each instrument field (excluding many-to-many).
        for oField in oInstrument.fieldmetadata_set.exclude(is_many_to_many=True):
            field_name = oField.get_django_field_name()
            
            columns.append(Column(field_name, self.get_alchemy_type(oField.django_data_type), nullable=True))
            if oField.get_display_lookup():
                columns.append(Column("{}_display_value".format(field_name), Text, nullable=True))
        
        # Create instrument table with all created columns and save it.
        table = Table(table_name, metadata_obj, *columns)
        self.tables[table_name] = table

    # Create lookup tables for the fields that were skipped in create_instrument_table.
    def create_lookup_tables(self, oInstrument):
        instrument_table_name = oInstrument.get_django_model_name().lower()
        
        for oField in oInstrument.fieldmetadata_set.filter(is_many_to_many=True):
            field_name = oField.get_django_field_name()
            lookup_table_name = "{}_{}_lookup".format(instrument_table_name, field_name)
            
            table = Table(
                lookup_table_name,
                metadata_obj,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("{}_id".format(instrument_table_name), Integer, ForeignKey("{}.id".format(instrument_table_name), ondelete="CASCADE")),
                Column(field_name, Text),
                Column("{}_display_value".format(field_name), self.get_alchemy_type(oField.django_data_type)),
            )
            
            # Store Table for later.
            self.tables[lookup_table_name] = table

    #Convert Django field types to SQLAlchemy types.
    def get_alchemy_type(self, django_type):
        map = {
            "TextField": Text,
            "CharField": String(255),
            "IntegerField": Integer,
            "BigIntegerField": BigInteger,
            "FloatField": Float,
            "BooleanField": Boolean,
            "DateField": Date,
            "DateTimeField": Date,
        }
        
        return map.get(django_type, Text)  # Default to Text if Django type is unknown.

