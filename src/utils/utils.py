import time
import csv
import math
import psutil
import os

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss/(1024**3) # GB units


def write_log(task, framework, data_size, file_type, time_sec, mem_gb):
   csv_file = os.getenv('CSV_RESULT_FILE', "result.csv")
   time_sec = round(time_sec, 6)
   mem_gb = round(mem_gb, 6)
   if math.isnan(time_sec):
      time_sec = ""
   if math.isnan(mem_gb):
      mem_gb = ""
   log_row = [task, framework, data_size, file_type, time_sec, mem_gb]
   log_header = ['task', 'framework', 'data_size', 'file_type', 'time_sec', 'mem_gb']
   if os.path.isfile(csv_file) and not(os.path.getsize(csv_file)):
      os.remove(csv_file)
   append = os.path.isfile(csv_file)
   csv_verbose = os.getenv('CSV_VERBOSE', "true")
   if csv_verbose.lower()=="true":
      print('# ' + ','.join(str(x) for x in log_row))
   if append:
      with open(csv_file, 'a') as f:
         w = csv.writer(f, lineterminator='\n')
         w.writerow(log_row)
   else:
      with open(csv_file, 'w+') as f:
         w = csv.writer(f, lineterminator='\n')
         w.writerow(log_header)
         w.writerow(log_row)