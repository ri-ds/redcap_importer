import datetime
from io import StringIO
import sys
import time

from django.core.management.base import BaseCommand, CommandError

import requests
from requests.adapters import HTTPAdapter

from redcap_importer import models

from sqlalchemy import create_engine, MetaData, select, insert, delete


class Command(BaseCommand):
    help = "Populates SQLite database with REDCap data using SQLAlchemy Core"

    def __init__(self, *args, **kwargs):
        self.query_count = 0
        self.log_comments = []  # a list of comments to save with the ETL log
        session = requests.Session()
        adapter = HTTPAdapter()
        super().__init__(*args, **kwargs)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self.session = session
        
        # Initialize SQLAlchemy components (will be set up in handle())
        self.engine = None
        self.metadata = None

    def print_out(self, *args, **kwargs):
        """A wrapper for self.stdout.write() that converts anything into a string"""
        strings = []
        for arg in args:
            strings.append(str(arg))
        output = ",".join(strings)
        if "log" in kwargs and kwargs["log"]:
            self.log_comments.append(output)
        self.stdout.write(output)

    def start_capture_stdout(self):
        self.captured_stdout = []
        self.original_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()

    def finish_capture_stdout(self, log=True):
        sys.stdout = self.original_stdout
        for line in self.captured_stdout:
            self.print_out(line, log)

    def add_arguments(self, parser):
        parser.add_argument("connection_name")
        parser.add_argument(
            "--db-path", 
            default="r.sqlite3", 
            help="Path to SQLite database file"
        )

    def run_request(self, content, oConnection, addl_options={}):
        addl_options["content"] = content
        addl_options["token"] = oConnection.get_api_token()
        addl_options["format"] = "json"
        addl_options["returnFormat"] = "json"
        self.query_count += 1
        for i in range(500):
            try:
                response = self.session.post(oConnection.api_url.url, addl_options).json()
                break
            except requests.exceptions.ConnectionError as e:
                print("fail time: ", datetime.datetime.now())
                print("fail # {}. url: {}; options: {}".format(i, oConnection.api_url.url, addl_options))
                time.sleep(10)
                continue
        return response


    def handle(self, *args, **options):
        connection_name = options["connection_name"]
        
        # Get Django connection for configuration
        oConnection = models.RedcapConnection.objects.get(unique_name=connection_name)
        self.print_out(oConnection.projectmetadata, log=True)
        self.query_count = 0
        
        # Initialize SQLAlchemy engine and metadata
        engine = create_engine("sqlite+pysqlite:///"+connection_name+".sqlite3", echo=False)
        self.metadata_obj = MetaData()
        self.metadata_obj.reflect(bind=engine)
        self.print_out("Connected to SQLite database")
        
        self.oEtlLog = models.EtlLog(
            redcap_project=oConnection.unique_name,
            start_date=datetime.datetime.now(),
            status=models.EtlLog.STATUS_ETL_STARTED,
        )
        self.oEtlLog.save()
        self.start_capture_stdout()

        # Clear existing data from SQLite database
        with engine.begin() as conn:
            # Get project_root table
            if 'project_root' in self.metadata_obj.tables:
                project_root_table = self.metadata_obj.tables['project_root']
                # Due to foreign key constraints, child tables will cascade delete
                conn.execute(delete(project_root_table))
                self.print_out("Cleared existing data from SQLite database", log=True)

        # get a list of all primary keys
        response = self.run_request(
            "record",
            oConnection,
            {
                "fields": oConnection.projectmetadata.primary_key_field,
            },
        )
        pk_list = []
        for entry in response:
            pk = entry[oConnection.projectmetadata.primary_key_field]
            if not pk in pk_list:
                pk_list.append(pk)

        for pk in pk_list:
            options = {"records[0]": pk}
            instrument_names = oConnection.get_instrument_names()
            if instrument_names:
                for idx, instrument_name in enumerate(instrument_names):
                    options["forms[{}]".format(idx)] = instrument_name
            response = self.run_request("record", oConnection, options)
            if oConnection.projectmetadata.is_longitudinal:
                for entry in response:
                    self.insert_longitudinal(entry, oConnection)
            else:
                for entry in response:
                    self.insert_non_longitudinal(entry, oConnection)
        
        oConnection.projectmetadata.date_last_downloaded_data = datetime.datetime.now()
        oConnection.projectmetadata.save()

        instruments_loaded = oConnection.get_instrument_names()
        if instruments_loaded:
            instruments_loaded = "\n".join(instruments_loaded)
        self.finish_capture_stdout()
        self.oEtlLog.end_date = datetime.datetime.now()
        self.oEtlLog.query_count = self.query_count
        self.oEtlLog.instruments_loaded = instruments_loaded
        self.comment = "\n".join(self.log_comments)
        self.oEtlLog.status = self.oEtlLog.STATUS_ETL_COMPLETE
        self.oEtlLog.save()

    def insert_non_longitudinal(self, entry, oConnection):
        pk_field = oConnection.projectmetadata.primary_key_field
        pk_value = entry[pk_field]
        
        # Ensure project root exists
        project_root_id = self.ensure_project_root(pk_value, pk_field)
        
        if "redcap_repeat_instrument" in entry and entry["redcap_repeat_instrument"]:
            # repeat instrument, have 1 instrument to load
            instrument_name = entry["redcap_repeat_instrument"]
            if not oConnection.check_include_instrument(instrument_name):
                return
            
            # Get Django metadata for this instrument
            oInstrumentMetadata = models.InstrumentMetadata.objects.get(
                project=oConnection.projectmetadata, unique_name=instrument_name
            )
            self.insert_instrument_data(oInstrumentMetadata, entry, project_root_id=project_root_id)
        else:
            # base_record, load all non-repeating instruments (verify not empty)
            qInstrument = oConnection.projectmetadata.instrumentmetadata_set.exclude(
                repeatable=True
            )
            for oInstrument in qInstrument:
                if not oConnection.check_include_instrument(oInstrument.unique_name):
                    continue
                if self.has_instrument_data(entry, oInstrument):
                    self.insert_instrument_data(oInstrument, entry, project_root_id=project_root_id)

    def insert_longitudinal(self, entry, oConnection):
        pk_field = oConnection.projectmetadata.primary_key_field
        pk_value = entry[pk_field]
        
        # Ensure project root exists
        project_root_id = self.ensure_project_root(pk_value, pk_field)
        
        # Ensure event exists
        event_name = entry["redcap_event_name"]
        oEventMetadata = models.EventMetadata.objects.get(
            project=oConnection.projectmetadata, unique_name=event_name
        )
        event_id = self.ensure_event_record(project_root_id, entry, oEventMetadata)

        if "redcap_repeat_instrument" in entry and entry["redcap_repeat_instrument"]:
            # repeat instrument, have 1 instrument to load
            instrument_name = entry["redcap_repeat_instrument"]
            if not oConnection.check_include_instrument(instrument_name):
                return
            
            oInstrumentMetadata = models.InstrumentMetadata.objects.get(
                project=oConnection.projectmetadata, unique_name=instrument_name
            )
            self.insert_instrument_data(oInstrumentMetadata, entry, redcap_event_id=event_id)
        else:
            # base_record, load all non-repeating instruments (verify not empty)
            qInstrument = oConnection.projectmetadata.instrumentmetadata_set.exclude(
                repeatable=True
            )
            for oInstrument in qInstrument:
                if not oConnection.check_include_instrument(oInstrument.unique_name):
                    continue
                if self.has_instrument_data(entry, oInstrument):
                    self.insert_instrument_data(oInstrument, entry, redcap_event_id=event_id)

    def ensure_project_root(self, pk_value, pk_field):
        """Ensure project root exists in SQLite, return the primary key"""
        project_root_table = self.metadata.tables['project_root']
        
        with self.engine.begin() as conn:
            # Check if exists
            result = conn.execute(
                select(project_root_table.c[pk_field])
                .where(project_root_table.c[pk_field] == pk_value)
            ).first()
            
            if not result:
                # Insert new project root
                conn.execute(
                    insert(project_root_table).values(**{
                        pk_field: pk_value,
                        f"{pk_field}_display": str(pk_value)  # Convert to string for display
                    })
                )
            
            return pk_value

    def ensure_event_record(self, project_root_id, entry, oEventMetadata):
        """Ensure event record exists in SQLite for longitudinal projects, return event ID"""
        event_table = self.metadata.tables['redcap_event']
        
        event_name = entry["redcap_event_name"]
        repeat_instance = entry.get("redcap_repeat_instance")
        
        with self.engine.begin() as conn:
            # Build where clause
            where_clause = (
                (event_table.c.project_root_id == project_root_id) &
                (event_table.c.event_unique_name == event_name)
            )
            
            if repeat_instance:
                where_clause = where_clause & (event_table.c.redcap_repeat_instance == repeat_instance)
            else:
                where_clause = where_clause & (event_table.c.redcap_repeat_instance.is_(None))
            
            result = conn.execute(
                select(event_table.c.id).where(where_clause)
            ).first()
            
            if result:
                return result.id
            else:
                # Insert new event
                result = conn.execute(
                    insert(event_table).values(
                        project_root_id=project_root_id,
                        event_unique_name=event_name,
                        event_label=oEventMetadata.label,  # Use Django metadata
                        arm_number=oEventMetadata.arm_number,  # Use Django metadata
                        redcap_repeat_instance=repeat_instance
                    )
                )
                return result.inserted_primary_key[0]

    def has_instrument_data(self, entry, oInstrument):
        """Check if entry contains data for the specified instrument using Django metadata"""
        # Get field names from Django metadata
        field_names = []
        for oField in oInstrument.fieldmetadata_set.all():
            field_names.append(oField.get_django_field_name())
        
        # Check if any of these fields have non-empty values
        for field_name in field_names:
            if field_name in entry and entry[field_name] not in ['', None]:
                return True
        
        return False

    def insert_instrument_data(self, oInstrumentMetadata, entry, project_root_id=None, redcap_event_id=None):
        """Insert instrument data into SQLite using Django metadata for field mapping"""
        table_name = oInstrumentMetadata.get_django_model_name().lower()
        
        if table_name not in self.metadata.tables:
            self.print_out(f"Warning: Table {table_name} not found in SQLite database", log=True)
            return
        
        table = self.metadata.tables[table_name]
        
        with self.engine.begin() as conn:
            # Build insert values
            values = {}
            
            # Add parent relationship
            if redcap_event_id:
                values['redcap_event_id'] = redcap_event_id
            elif project_root_id:
                values['project_root_id'] = project_root_id
            
            # Add repeat instance
            if 'redcap_repeat_instance' in entry:
                values['redcap_repeat_instance'] = entry['redcap_repeat_instance']
            
            # Add field data using Django metadata
            for oField in oInstrumentMetadata.fieldmetadata_set.exclude(is_many_to_many=True):
                field_name = oField.get_django_field_name()
                
                # Regular field
                if field_name in entry and entry[field_name] not in ['', None]:
                    values[field_name] = entry[field_name]
                
                # Display value field
                if oField.get_display_lookup():
                    display_field_name = f"{field_name}_display_value"
                    if display_field_name in table.columns and field_name in entry:
                        values[display_field_name] = self.get_display_value(oField, entry[field_name])
            
            # Handle many-to-many fields separately
            many_to_many_data = {}
            for oField in oInstrumentMetadata.fieldmetadata_set.filter(is_many_to_many=True):
                field_name = oField.get_django_field_name()
                if field_name in entry and entry[field_name] not in ['', None]:
                    many_to_many_data[field_name] = {
                        'field_obj': oField,
                        'value': entry[field_name]
                    }
            
            # Insert the main instrument record
            if values:  # Only insert if we have data
                result = conn.execute(insert(table).values(**values))
                instrument_record_id = result.inserted_primary_key[0]
                
                # Insert many-to-many data
                for field_name, data in many_to_many_data.items():
                    self.insert_many_to_many_data(
                        conn, table_name, field_name, data['field_obj'], 
                        data['value'], instrument_record_id
                    )

    def insert_many_to_many_data(self, conn, instrument_table_name, field_name, oField, field_value, instrument_record_id):
        """Insert many-to-many field data into lookup table"""
        lookup_table_name = f"{instrument_table_name}_{field_name}_lookup"
        
        if lookup_table_name not in self.metadata.tables:
            self.print_out(f"Warning: Lookup table {lookup_table_name} not found", log=True)
            return
        
        lookup_table = self.metadata.tables[lookup_table_name]
        
        # Handle multiple values (assuming they're separated by some delimiter)
        if isinstance(field_value, str) and ',' in field_value:
            values = [v.strip() for v in field_value.split(',')]
        else:
            values = [field_value]
        
        for value in values:
            if value:  # Skip empty values
                conn.execute(
                    insert(lookup_table).values(**{
                        f"{instrument_table_name}_id": instrument_record_id,
                        field_name: value,
                        f"{field_name}_display_value": self.get_display_value(oField, value)
                    })
                )

    def get_display_value(self, oField, field_value):
        """Get display value for a field using Django metadata"""
        if not field_value:
            return None
        
        # Use Django field metadata to get display value
        display_lookup = oField.get_display_lookup()
        if display_lookup and str(field_value) in display_lookup:
            return display_lookup[str(field_value)]
        
        return str(field_value)