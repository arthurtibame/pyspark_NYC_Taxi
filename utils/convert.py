import argparse
import os 
import pandas as pd
from tqdm import tqdm
import enum

# Enum for size units
class SIZE_UNIT(enum.Enum):
   BYTES = 1
   KB = 2
   MB = 3
   GB = 4

def convert_unit(size_in_bytes, unit):
   """ Convert the size from bytes to other units like KB, MB or GB"""
   if unit == SIZE_UNIT.KB:
       return size_in_bytes/1024
   elif unit == SIZE_UNIT.MB:
       return size_in_bytes/(1024*1024)
   elif unit == SIZE_UNIT.GB:
       return size_in_bytes/(1024*1024*1024)
   else:
       return size_in_bytes

def get_file_size(file_name, size_type=SIZE_UNIT.GB ):
   """ Get file in size in given unit like KB, MB or GB"""
   size = os.path.getsize(file_name)
   return convert_unit(size, size_type)

def csv2parquet(input_path, file, output_path):    
    print(" file name: " + file)
    file_ = file[:-4]
    # df = pd.read_csv(input_path + file ,delimiter=",")
    try:
      df = pd.read_csv(input_path + file ,delimiter=",")
      df.to_parquet(output_path + file_ + ".parquet")
      del df
    except:
      df = pd.read_csv(input_path + file, skiprows=2, engine='python')
      df.to_parquet(output_path + file_ + ".parquet")
      del df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='convert.py')
    parser.add_argument('--input', nargs='+', type=str, default='yolov5s.pt', help='*.csv path')
    parser.add_argument('--output', type=str, default='./', help='*.parquet path')
    opt = parser.parse_args()
    input_size_total = 0
    output_size_total = 0
    if opt.input:
        base_dir = opt.input
        output_dir = opt.output
        files = os.listdir(base_dir)
        count = 0     
        for file in tqdm(files):   
            input_size = get_file_size(os.path.join(base_dir, file))             
            csv2parquet(base_dir,file , output_dir)
            output_size = get_file_size(os.path.join(output_dir, file))           
            input_size_total+=input_size
            output_size_total+=output_size
            print(" input_size_total: {} GB, output_size_total: {} GB".format(input_size_total, output_size_total))
            