import os
import subprocess
import sys
import time
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

LANDING_DIR = './landing'


class CSVHandler(FileSystemEventHandler):

  def on_created(self, event):
    if not event.is_directory and event.src_path.endswith('.csv'):
      print(f'\n[EVENT] New CSV file detected: {event.src_path}')
      time.sleep(1)

      print('[STEP 1/2] Executing Apache Beam ELT Pipeline...')
      beam_res = subprocess.run(
          [sys.executable, 'pipeline.py', event.src_path],
          capture_output=True,
          text=True,
      )
      if beam_res.returncode != 0:
        print(f'[ERROR] Pipeline failed:\n{beam_res.stderr}')
        return

      print('[STEP 2/2] Ingesting files into PostgreSQL...')
      db_res = subprocess.run(
          [sys.executable, 'db_loader.py'], capture_output=True, text=True
      )
      if db_res.returncode != 0:
        print(f'[ERROR] DB load failed:\n{db_res.stderr}')
        return

      print(db_res.stdout)
      print('--- [SUCCESS] Workflow Executed Successfully! ---')


if __name__ == '__main__':
  os.makedirs(LANDING_DIR, exist_ok=True)
  os.makedirs('cleaned', exist_ok=True)
  os.makedirs('error_logs', exist_ok=True)

  event_handler = CSVHandler()
  observer = Observer()
  observer.schedule(event_handler, path=LANDING_DIR, recursive=False)
  observer.start()

  print(f"Monitoring '{LANDING_DIR}' folder. Drop a CSV to execute.\n")
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
  observer.join()