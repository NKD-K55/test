import os
import shutil
import pytz
import time
import pyodbc
from datetime import datetime
from typing import Dict, List, Tuple

from Fullcombine_AdvanceTest.TSV_parsing import stored_procedure

Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')

def ts():
    return datetime.now(Vietnam_time).strftime("%Y-%m-%d %H:%M:%S")

# ASC_SOURCE = "/kiomagd/737/Full_Lot_Combine_result_file"
# FCL_TARGET_ROOT = "/kiomagd/737/Full_Lot_Combine_result_file/full_combine_result_file"

ASC_SOURCE = "./737/results/Full_Lot_Combine_result_file/BACKUP/2025/08/26"
FCL_TARGET_ROOT = "./737/results/FCL_asc"

# ====== DB link ======
# DB_LINK = (
#     "DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1;"
#     "UID=cimitar2;PWD=TFAtest1!2!;Database=MCSDB;Server=10.201.21.84,50150;"
#     "TrustServerCertificate=yes;"
# )

DB_LINK = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "UID=cimitar2;PWD=TFAtest1!2!;Database=MCSDB;Server=10.201.21.84,50150;"
    "TrustServerCertificate=yes;"
)

# ====== Move FCL asc ======
def move_to_target(src_file: str) -> str:
    os.makedirs(FCL_TARGET_ROOT, exist_ok=True)
    dest_path = os.path.join(FCL_TARGET_ROOT, os.path.basename(src_file))
    base, ext = os.path.splitext(dest_path)
    i = 1
    while os.path.exists(dest_path):
        dest_path = f"{base}_{i}{ext}"
        i += 1
    shutil.move(src_file, dest_path)
    print(f"MOVED -> {dest_path}")
    return dest_path

def get_recent_files(directory: str, time_threshold: int = None):
    files = []
    cur = time.time()
    for name in os.listdir(directory):
        if not (name.lower().endswith(".asc")):
            continue
        path = os.path.join(directory, name)
        if not os.path.isfile(path):
            continue
        if time_threshold is None:
            files.append(path)
        else:
            mtime = os.path.getmtime(path)
            if cur - mtime <= time_threshold:
                files.append(path)
    return sorted(files)

def extract_tracecode(file_path: str) -> Tuple[str, str]:
    """
    Trả về (tracecode, basename). Theo pattern cũ: {tracecode}_{...}.asc
    """
    base = os.path.basename(file_path)
    parts = base.split("_")
    if len(parts) < 2:
        return ("", base)
    return (parts[0], base)

# ====== Check FCL ======
def check_tracecode(cursor, tracecode: str) -> bool:
    data_list = ['get_lot_dcc', '', '', '', tracecode]

    result = stored_procedure.set_rel_unit_data(data_list)

    if result == None:
        return 1
    else:
        return 0

def make_tracecode_list(directory: str, time_threshold: int = None) -> Tuple[List[str], Dict[str, List[str]]]:
    files = get_recent_files(directory, time_threshold)
    trace_to_files: Dict[str, List[str]] = {}
    for f in files:
        code, base = extract_tracecode(f)
        if not code:
            continue
        trace_to_files.setdefault(code, []).append(f)
    uniq_tracecodes = sorted(trace_to_files.keys())
    return uniq_tracecodes, trace_to_files

def check_FCL_asc(time_threshold_seconds: int = None):
    start = time.time()
    files = get_recent_files(ASC_SOURCE, time_threshold_seconds)
    tracecodes, trace_to_files = make_tracecode_list(ASC_SOURCE, time_threshold_seconds)
    
    # print(f"{tracecodes}")
    if not files:
        print(f"-----------------------Waiting file input!--------------------\t {ts()}")
        print(f"Total script execution time: {time.time() - start:.2f} seconds")
        return

    with pyodbc.connect(DB_LINK) as cnxn:
        with cnxn.cursor() as cursor:
            for tracecode in tracecodes:
                try:
                    result = check_tracecode(cursor, tracecode)
                    if result == 1:
                        try:
                            print(f"-----------------------Start_Modifying-----------------------\t {ts()}")
                            print(f"{tracecode} Is Full Combine Lot")

                            for f in trace_to_files[tracecode]:
                                move_to_target(f)

                        except Exception as ex:
                            print(f"ERROR - Cannot {os.path.basename(f)}: {ex}") 
                    else:
                        # print(f"[{datetime}] INFO - {base} is NOT FCL")
                        pass
                except Exception as e:
                    print(f"ERROR - Cannot process {e}")    
    print(f"Total script execution time: {time.time() - start:.2f} seconds")

if __name__ == "__main__":
    check_FCL_asc(time_threshold_seconds=None)