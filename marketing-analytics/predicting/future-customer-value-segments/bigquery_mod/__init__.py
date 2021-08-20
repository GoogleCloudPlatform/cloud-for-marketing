# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import decimal
import json
import logging
import uuid

from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam import PTransform
from apache_beam.io.filesystems import CompressionTypes
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.iobase import BoundedSource
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.textio import _TextSource as TextSource
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible

FieldSchema = collections.namedtuple('FieldSchema', 'fields mode name type')


def _to_bool(value):
    return value == 'true'


def _to_decimal(value):
    return decimal.Decimal(value)


def _to_bytes(value):
    return value.encode('utf-8')


class _JsonToDictCoder(coders.Coder):
    def __init__(self, table_schema):
        self.fields = self._convert_to_tuple(table_schema.fields)
        self._converters = {
            'INTEGER': int,
            'INT64': int,
            'FLOAT': float,
            'BOOLEAN': _to_bool,
            'NUMERIC': _to_decimal,
            'BYTES': _to_bytes,
        }

    @classmethod
    def _convert_to_tuple(cls, table_field_schemas):
        if not table_field_schemas:
            return []

        return [
            FieldSchema(cls._convert_to_tuple(x.fields), x.mode, x.name, x.type)
            for x in table_field_schemas
        ]

    def decode(self, value):
        value = json.loads(value.decode('utf-8'))
        return self._decode_with_schema(value, self.fields)

    def _decode_with_schema(self, value, schema_fields):
        for field in schema_fields:
            if field.name not in value:
                value[field.name] = None
                continue

            if field.type == 'RECORD':
                value[field.name] = self._decode_with_schema(
                    value[field.name], field.fields)
            else:
                try:
                    converter = self._converters[field.type]
                    value[field.name] = converter(value[field.name])
                except KeyError:
                    pass
        return value

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return dict


class _CustomBigQuerySource(BoundedSource):
    def __init__(
            self,
            # gcs_location=None,
            get_destination_uri=None,
            table=None,
            dataset=None,
            project=None,
            query=None,
            validate=False,
            coder=None,
            use_standard_sql=False,
            flatten_results=True,
            kms_key=None):
        if table is not None and query is not None:
            raise ValueError(
                'Both a BigQuery table and a query were specified.'
                ' Please specify only one of these.')
        elif table is None and query is None:
            raise ValueError('A BigQuery table or a query must be specified')
        elif table is not None:
            self.table_reference = bigquery_tools.parse_table_reference(
                table, dataset, project)
            self.query = None
            self.use_legacy_sql = True
        else:
            if isinstance(query, (str, unicode)):
                query = StaticValueProvider(str, query)
            self.query = query
            # TODO(BEAM-1082): Change the internal flag to be standard_sql
            self.use_legacy_sql = not use_standard_sql
            self.table_reference = None

        self.get_destination_uri = get_destination_uri
        # self.gcs_location = gcs_location
        if isinstance(project, (str, unicode)):
            project = StaticValueProvider(str, query)
        self.project = project
        self.validate = validate
        self.flatten_results = flatten_results
        self.coder = coder or _JsonToDictCoder
        self.kms_key = kms_key
        self.split_result = None

    @check_accessible(['query'])
    def estimate_size(self):
        bq = bigquery_tools.BigQueryWrapper()
        if self.table_reference is not None:
            table = bq.get_table(
                self.table_reference.projectId,
                self.table_reference.datasetId,
                self.table_reference.tableId)
            return int(table.numBytes)
        else:
            job = bq._start_query_job(
                self.project.get(),
                self.query.get(),
                self.use_legacy_sql,
                self.flatten_results,
                job_id=uuid.uuid4().hex,
                dry_run=True,
                kms_key=self.kms_key)
            size = int(job.statistics.totalBytesProcessed)
            return size

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if self.split_result is None:
            bq = bigquery_tools.BigQueryWrapper()

            if self.query is not None:
                self._setup_temporary_dataset(bq)
                self.table_reference = self._execute_query(bq)

            schema, metadata_list = self._export_files(bq)
            self.split_result = [
                TextSource(
                    metadata.path,
                    0,
                    CompressionTypes.UNCOMPRESSED,
                    True,
                    self.coder(schema)) for metadata in metadata_list
            ]
            if self.query is not None:
                bq.clean_up_temporary_dataset(self.project.get())

        for source in self.split_result:
            yield SourceBundle(0, source, None, None)

    def get_range_tracker(self, start_position, stop_position):
        class CustomBigQuerySourceRangeTracker(RangeTracker):
            """A RangeTracker that always returns positions as None."""

            def start_position(self):
                return None

            def stop_position(self):
                return None

        return CustomBigQuerySourceRangeTracker()

    def read(self, range_tracker):
        raise NotImplementedError('BigQuery source must be split before being read')

    @check_accessible(['query'])
    def _setup_temporary_dataset(self, bq):
        location = bq.get_query_location(
            self.project.get(), self.query.get(), self.use_legacy_sql)
        bq.create_temporary_dataset(self.project.get(), location)

    @check_accessible(['query'])
    def _execute_query(self, bq):
        job = bq._start_query_job(
            self.project.get(),
            self.query.get(),
            self.use_legacy_sql,
            self.flatten_results,
            job_id=uuid.uuid4().hex,
            kms_key=self.kms_key)
        job_ref = job.jobReference
        bq.wait_for_bq_job(job_ref)
        return bq._get_temp_table(self.project.get())

    def _export_files(self, bq):
        """Runs a BigQuery export job.

        Returns:
          bigquery.TableSchema instance, a list of FileMetadata instances
        """
        job_id = uuid.uuid4().hex
        gcs_location = self.get_destination_uri()
        job_ref = bq.perform_extract_job([gcs_location],
                                         job_id,
                                         self.table_reference,
                                         bigquery_tools.FileFormat.JSON,
                                         include_header=False)
        bq.wait_for_bq_job(job_ref)
        metadata_list = FileSystems.match([gcs_location])[0].metadata_list

        table = bq.get_table(
            self.table_reference.projectId,
            self.table_reference.datasetId,
            self.table_reference.tableId)

        return table.schema, metadata_list


class _PassThroughThenCleanup(PTransform):
    def __init__(self, cleanup_dofn):
        self.cleanup_dofn = cleanup_dofn

    def expand(self, input):
        class PassThrough(beam.DoFn):
            def process(self, element):
                yield element

        output = input | beam.ParDo(PassThrough()).with_outputs(
            'cleanup_signal', main='main')
        main_output = output['main']
        cleanup_signal = output['cleanup_signal']

        _ = (
            input.pipeline
            | beam.Create([None])
            | beam.ParDo(
                self.cleanup_dofn, beam.pvalue.AsSingleton(cleanup_signal)))

        return main_output


class ReadFromBigQuery(PTransform):
    def __init__(self, gcs_location=None, validate=False, *args, **kwargs):
        if gcs_location:
            if not isinstance(gcs_location, (str, unicode, ValueProvider)):
                raise TypeError(
                    '%s: gcs_location must be of type string'
                    ' or ValueProvider; got %r instead' %
                    (self.__class__.__name__, type(gcs_location)))

            if isinstance(gcs_location, (str, unicode)):
                gcs_location = StaticValueProvider(str, gcs_location)

        self.gcs_location = gcs_location
        self.validate = validate
        self.job_id = uuid.uuid4().hex

        self._args = args
        self._kwargs = kwargs

    def _get_destination_uri(self):
        """Returns the fully qualified Google Cloud Storage URI where the
        extracted table should be written.
        """
        file_pattern = 'bigquery-table-dump-*.json'
        gcs_base = ''

        if self.gcs_location is not None:
            if self.gcs_location.is_accessible():
                gcs_base = self.gcs_location.get()
        else:
            raise ValueError(
                '{} requires a GCS location to be provided'.format(
                    self.__class__.__name__))
        if self.validate:
            self._validate_gcs_location(gcs_base)

        return FileSystems.join(gcs_base, self.job_id, file_pattern)

    @staticmethod
    def _validate_gcs_location(gcs_location):
        if not gcs_location.startswith('gs://'):
            raise ValueError('Invalid GCS location: {}'.format(gcs_location))

    def expand(self, pcoll):
        class RemoveJsonFiles(beam.DoFn):
            def __init__(self, get_destination_uri):
                self.get_destination_uri = get_destination_uri

            def process(self, unused_element, signal):
                gcs_location = self.get_destination_uri()
                match_result = FileSystems.match([gcs_location])[0].metadata_list
                logging.debug(
                    "%s: matched %s files", self.__class__.__name__, len(match_result))
                paths = [x.path for x in match_result]
                FileSystems.delete(paths)

        return (
            pcoll
            | beam.io.Read(
                _CustomBigQuerySource(
                    # gcs_location=self.gcs_location,
                    get_destination_uri=self._get_destination_uri,
                    validate=self.validate,
                    *self._args,
                    **self._kwargs))
            | _PassThroughThenCleanup(RemoveJsonFiles(self._get_destination_uri)))
