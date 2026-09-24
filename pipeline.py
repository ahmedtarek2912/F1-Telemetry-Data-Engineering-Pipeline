import csv
from datetime import datetime
import logging
import math
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
 
 
class ValidateAndCleanDoFn(beam.DoFn):
 
  TAG_CLEAN = 'clean_records'
  TAG_DEAD_LETTER = 'dead_letter'
 
  def clean_text(self, value):
    return str(value).strip() if value is not None else ''
 
  def clean_float(self, value, field_name):
    val = self.clean_text(value)
    if val.lower() in ['', 'nan', 'none', 'null', 'n/a', 'na']:
      raise ValueError(f'Missing {field_name}')
    num = float(val)
    if not math.isfinite(num):
      raise ValueError(f'Invalid {field_name}: {val}')
    return num
 
  def clean_int(self, value, field_name):
    num = self.clean_float(value, field_name)
    if not num.is_integer():
      raise ValueError(f'Invalid {field_name}: {value}')
    return int(num)
 
  def clean_bool(self, value):
    val = self.clean_text(value).lower()
    if val in ['true', '1', 'yes']:
      return True
    if val in ['false', '0', 'no']:
      return False
    raise ValueError(f'Invalid Brake: {value}')
 
  def clean_date(self, value):
    val = self.clean_text(value)
    if not val:
      raise ValueError('Missing Date')
    for fmt in [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    ]:
      try:
        return datetime.strptime(val, fmt).strftime('%Y-%m-%d %H:%M:%S.%f')
      except ValueError:
        continue
    raise ValueError(f'Invalid Date: {value}')
 
  def process(self, element):
    try:
      fields = list(csv.reader([element]))[0]
      if len(fields) < 11:
        raise ValueError(f'Expected 11 fields, found {len(fields)}')
 
      date = self.clean_date(fields[0])
      rpm = self.clean_float(fields[1], 'RPM')
      speed = self.clean_float(fields[2], 'Speed')
      gear = self.clean_int(fields[3], 'nGear')
      throttle = self.clean_float(fields[4], 'Throttle')
      brake = self.clean_bool(fields[5])
      drs = self.clean_int(fields[6], 'DRS')
      source = self.clean_text(fields[7])
      time_val = self.clean_text(fields[8])
      session_time = self.clean_text(fields[9])
      driver = self.clean_int(fields[10], 'Driver')
 
      # Domain Validation Rules
      if rpm < 0:
        raise ValueError(f'Invalid RPM: {rpm}')
      if speed < 0:
        raise ValueError(f'Invalid Speed: {speed}')
      if gear < 0 or gear > 8:
        raise ValueError(f'Invalid nGear: {gear}')
      if throttle < 0 or throttle > 100:
        raise ValueError(f'Invalid Throttle: {throttle}')
      if drs < 0 or drs > 15:
        raise ValueError(f'Invalid DRS: {drs}')
      if not source:
        raise ValueError('Missing Driver or Source')
      if not time_val or not session_time:
        raise ValueError('Missing Time')
      if driver < 0:
        raise ValueError(f'Invalid Driver: {driver}')
 
      yield beam.pvalue.TaggedOutput(
          self.TAG_CLEAN,
          {
              'Date': date,
              'RPM': rpm,
              'Speed': speed,
              'nGear': gear,
              'Throttle': throttle,
              'Brake': brake,
              'DRS': drs,
              'Source': source,
              'Time': time_val,
              'SessionTime': session_time,
              'Driver': driver,
          },
      )
 
    except Exception as err:
      yield beam.pvalue.TaggedOutput(
          self.TAG_DEAD_LETTER, f'Error: {str(err)} | Raw Line: {element}'
      )
 
 
def run_pipeline(input_file):
  options = PipelineOptions()
  with beam.Pipeline(options=options) as p:
    raw = p | 'Read' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
    results = raw | 'Validate' >> beam.ParDo(
        ValidateAndCleanDoFn()
    ).with_outputs(
        ValidateAndCleanDoFn.TAG_CLEAN, ValidateAndCleanDoFn.TAG_DEAD_LETTER
    )
 
    results[ValidateAndCleanDoFn.TAG_CLEAN] | 'Save Clean' >> beam.io.WriteToText(
        'cleaned/clean_telemetry',
        file_name_suffix='.json',
        shard_name_template='',
    )
    results[
        ValidateAndCleanDoFn.TAG_DEAD_LETTER
    ] | 'Save DLQ' >> beam.io.WriteToText(
        'error_logs/corrupted_telemetry',
        file_name_suffix='.log',
        shard_name_template='',
    )
 
 
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  file_path = (
      sys.argv[1] if len(sys.argv) > 1 else 'landing/f1_car_data_dirty.csv'
  )
  run_pipeline(file_path)