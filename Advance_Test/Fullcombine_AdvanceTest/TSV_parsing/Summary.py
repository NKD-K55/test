# -*- coding: utf-8 -*-
"""
summary_generate_FCL_advance.py

Bản dựng lại theo FCL gốc (giữ thứ tự/hệ thống), chỉ thay phần đọc DAT có sẵn
bằng đọc metadata từ S2 + stored_procedure (logic lấy metadata từ summary_generate_main).
"""

import pyodbc
from datetime import datetime
import sys
from os import listdir
import os
from os.path import isfile, join
from pathlib import Path
import ntpath
import shutil
import pytz
import pandas as pd
import glob
import summary_generate_kioxia
import configparser
import separate_sublot

# Advance stored_procedure để lấy metadata từ DB thay cho đọc DAT gốc
from Fullcombine_AdvanceTest.TSV_parsing import stored_procedure

# timezone
Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
datetime_str = str(datetime.now(Vietnam_time))
datetime_str = datetime_str.split('.')[0]

# Read config
config = configparser.ConfigParser()
path_config = '/home/testit/SRC/SRC/src/KIOXIA/Fullcombinelot/TSV_Parsing/path_config.ini'
config.read(path_config)

kiomagd_path = config['Paths']['path_kiomagd']
Combine_lot_result_file = config['Paths']['asc_processed']
Separate_Sublot = config['Paths']['Separate_Sublot']
kiomagd_C1_S2_Done = config['Paths']['C1_S2_Done']
kiomagd_DAT_Done = config['Paths']['DAT_Done']
kiomagd_TSV_Summary = config["Paths"]['TSV_Summary']

server = config['Database']['server']
password = config['Database']['password']

# global list to collect generated files for optional move
list_file_C1_S2_DAT = []

# ----------------------
# Helper utilities (giữ nguyên thứ tự như FCL gốc)
# ----------------------
def list_folders(directory_path):
    folders = []
    for item in os.listdir(directory_path):
        p = os.path.join(directory_path, item)
        if os.path.isdir(p):
            folders.append(p)
    return folders


def get_test_station(station: str):
    all_station = {
        "T1L1": {"name": "TEST1", "code": "801"},
        "TH1": {"name": "TEST2", "code": "805"},
        "TL1": {"name": "TEST3", "code": "807"},
        "TL2": {"name": "TEST4", "code": "850"},
        "TH2": {"name": "TEST5", "code": "852"},
        "TH3": {"name": "TEST6", "code": "872"}
    }
    if station in all_station:
        return all_station[station]['code']
    return ""


def get_lot_dcc(lot_dcc: str):
    get_dot = lot_dcc.count('.')
    if get_dot > 1:
        lot_no = lot_dcc.split('.')[0] + '.' + lot_dcc.split('.')[1]
        dcc = lot_dcc.split('.')[-1]
    elif get_dot == 1:
        dcc = lot_dcc.split('.')[1]
        if len(dcc) > 2:
            lot_no = lot_dcc
            dcc = ''
        else:
            lot_no = lot_dcc.split('.')[0]
            dcc = lot_dcc.split('.')[-1]
    else:
        lot_no = lot_dcc
        dcc = ''
    return lot_no, dcc


def normal_round(num, ndigits=0):
    if ndigits == 0:
        return int(num + 0.5)
    else:
        digit_value = 10 ** ndigits
        return int(num * digit_value + 0.5) / digit_value


# ----------------------
# Core: full_parsing - bám sát FCL gốc thứ tự và logic
# ----------------------
def full_parsing(mypath: str):
    """
    Xử lý một thư mục sublot (mypath) chứa *.asc (_processed.asc)
    Flow:
      - parseAllFiles: đọc từng file _processed.asc, tổng hợp sumtxt (chuỗi tất cả dòng)
      - tạo DataFrame df_test từ sumtxt
      - duyệt từng sub tracecode -> tạo C1, tạo S2, tạo DAT Summary
      - phần tạo DAT Summary: thay đọc DAT gốc bằng đọc S2 header + stored_procedure
    """

    # local states (giống FCL)
    bins = {}
    totalBins = {}
    tBoard = []
    dut = []
    keyno = []
    linBin = []
    catData = []
    SWBin = {}
    sBin = {}

    # parseAllFiles y hệt FCL, giữ thứ tự logic
    def parseAllFiles(f, file, tBoard, dut, keyno, linBin, catData):
        alltxt = ""
        testFlag = ""
        recipe = ""
        rtFlag = ""
        lines = f.readlines()

        rtRule = {"A": "2,3,4,5,6,7,8",  # Retest Rule
                  "B": "3,4,5,6,7,8",
                  "C": "4,5,6,7,8",
                  "D": "5,6,7,8",
                  "E": "5,6,8",
                  "F": "5,8",
                  "G": "5,8",
                  "H": "3,5,6,7,8",
                  "I": "5,6,8",
                  "J": "5,7,8",
                  "K": "5,6,8",
                  "L": "4,5,8"}

        try:
            testFlag = file.split("_")[3].strip()
            recipe = file.split("_")[4].strip()

            for line in lines:
                alltxt += line
                if "STN" not in line:
                    curBin = line.split(",")[3].strip()
                    if curBin not in bins:
                        if testFlag != "INI":
                            rtFlag = recipe[0]  # Retest Bin Rule
                            if curBin != "001":
                                curBin_temp = curBin[-1]
                                if curBin_temp in rtRule[rtFlag]:
                                    bins.update({curBin: 1})
                                else:
                                    bins.update({curBin: 1})
                            else:
                                bins.update({curBin: 1})
                        else:
                            bins.update({curBin: 1})
                    else:
                        tmpBin = bins[curBin] + 1
                        bins.update({curBin: tmpBin})

                    tempCat = line.split(",")[4].strip()[::-1]
                    tBoard.append(line.split(",")[0].strip())
                    dut.append(line.split(",")[1].strip())
                    keyno.append(line.split(",")[2].strip())
                    linBin.append(line.split(",")[3].strip())
                    catData.append(tempCat)
                    swcnt = 1
                    tmpVal = ""
                    swPos = ""
                    for val in tempCat:
                        if val != "0":
                            tmpVal = val
                            break
                        swcnt += 1
                    if tmpVal == "1":
                        swPos = "SWCAT_" + str((swcnt * 4) - 3) + "_issue"
                    elif tmpVal == "2":
                        swPos = "SWCAT_" + str((swcnt * 4) - 2) + "_issue"
                    elif tmpVal == "4":
                        swPos = "SWCAT_" + str((swcnt * 4) - 1) + "_issue"
                    elif tmpVal == "8":
                        swPos = "SWCAT_" + str((swcnt * 4)) + "_issue"
                    else:
                        swPos = "good"

                    if swPos != "good":
                        if curBin not in sBin:
                            sBin.update({curBin: {}})
                        if swPos not in sBin[curBin]:
                            sBin[curBin].update({swPos: 1})
                        else:
                            tmpSbin = sBin[curBin][swPos] + 1
                            sBin[curBin].update({swPos: tmpSbin})
                    else:
                        if curBin not in sBin:
                            sBin.update({curBin: {swPos: 1}})
                        else:
                            tmpSbin = sBin[curBin][swPos] + 1
                            sBin[curBin].update({swPos: tmpSbin})

            # Update totalBins
            for val in bins.keys():
                if val not in totalBins:
                    totalBins[val] = bins.get(val, 0)
                else:
                    for val2 in totalBins.keys():
                        if val == val2:
                            temp = totalBins.get(val, 0) + bins.get(val, 0)
                            totalBins.update({val: temp})
            # Update SWBin merge
            for val in sBin.keys():
                if val not in SWBin:
                    if len(sBin) > 0:
                        SWBin[val] = sBin.get(val, 0)
                else:
                    for val2 in SWBin.keys():
                        if val in SWBin:
                            if val == val2:
                                for val3 in sBin[val].keys():
                                    for val4 in SWBin[val].keys():
                                        if val4 == val3:
                                            temp = SWBin[val].get(val3, 0) + sBin[val].get(val3, 0)
                                            SWBin[val].update({val3: temp})
                                        else:
                                            SWBin[val].update(sBin[val])
                                            break
                        else:
                            SWBin.update({val: sBin[val]})
                            break

        except Exception as e:
            print(f"Cannot parse: {file} -> {e}")

        return alltxt

    # --- Connect DB (Linux driver) ---
    data_base_os = '/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1'
    cnxn = pyodbc.connect(f"DRIVER={data_base_os};UID=cimitar2;PWD={password};Database=MCSDB;Server={server};TrustServerCertificate=yes;")
    cursor = cnxn.cursor()

    # paths: tất cả *.asc trong folder sublot
    paths = glob.glob(os.path.join(mypath, '*.asc'))
    # paths sorted để ổn định
    paths = sorted(paths)
    hBin = ""
    totalSbin = ""
    sumtxt = ""

    for fullpath in paths:
        totalOutQty = 0
        otherQty = 0
        lot_name = ""
        dcc = ""
        file_temp = ntpath.basename(fullpath)
        # lấy end_time, time_get_file để match file C1/S2 gốc
        try:
            end_time = str(file_temp).split('_')[5]
        except Exception:
            end_time = ""
        time_get_file = end_time[:-2] if end_time else ""
        main_tracecode = str(file_temp).split('_')[0]
        qty_sublot = file_temp.split("_")[-2] if "_" in file_temp else "01"
        testtime = str(file_temp).split('_')[3] if len(file_temp.split('_')) > 3 else ""
        testcode = file_temp.split("_")[1] if len(file_temp.split('_')) > 1 else ""
        f = open(fullpath, "r", encoding='utf-8', errors='ignore')
        if "_processed.asc" in file_temp:
            sumtxt += "\n" + parseAllFiles(f, file_temp, tBoard, dut, keyno, linBin, catData)
            tester = file_temp.split("_")[2].strip()
            f.close()

        totalOutQty += totalBins.get("001", 0)
        otherQty += sum(totalBins.values())

    # --- Build DataFrame từ sumtxt ---
    header = ['STN', 'DUT', 'Assemblylot#', 'BIN', 'CAT']
    all_lines = []
    lines = sumtxt.splitlines()
    for line in lines:
        if "STN,DUT,Assemblylot#,BIN,CAT" not in line and line != "":
            all_lines.append(line.strip().split(',')[0:-1])

    df_test = pd.DataFrame(columns=header)
    for i, value in enumerate(all_lines):
        # xử lý phòng ngừa nếu line có format khác
        try:
            df_test.loc[i] = value
        except Exception:
            if len(value) == 5:
                df_test.loc[i] = value
            else:
                padded = (value + [""])[:5]
                df_test.loc[i] = padded

    if df_test.empty:
        print("[INFO] No valid data from sumtxt; exiting full_parsing")
        cursor.close()
        cnxn.close()
        return

    trace_code_list = df_test['Assemblylot#'].unique()

    # --- Lấy firstLine của C1/S2 reference (theo time_get_file) ---
    source_C1S2 = kiomagd_path
    allFile_C1S2 = os.listdir(source_C1S2)
    tracecode_S2 = "S2_1_" + main_tracecode
    tracecode_C1 = "C1_1_" + main_tracecode

    firstLine_S2_end_time = None
    endLine_S2_end_time = None
    line_no_S2 = 0
    firstLine_C1_end_time = None

    def get_firstLine_S2_C1(file_name):
        list_file_C1_S2_DAT.append(file_name)
        file_path = os.path.join(source_C1S2, file_name)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as rf:
            first_line = rf.readline()
        return first_line

    for file in allFile_C1S2:
        if tracecode_S2 in str(file) and time_get_file in str(file):
            fp = os.path.join(source_C1S2, file)
            try:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as read_file:
                    firstLine_S2_end_time = read_file.readline()
                    rest_lines = read_file.readlines()
                    if len(rest_lines) > 0:
                        try:
                            last_line = rest_lines[-1]
                            line_no_S2 = int(last_line.split(',')[0])
                        except Exception:
                            line_no_S2 = 256
                    else:
                        line_no_S2 = 256
                list_file_C1_S2_DAT.append(file)
            except Exception as e:
                print(f"Cannot read S2 file {fp} -> {e}")
        elif tracecode_C1 in str(file) and time_get_file in str(file):
            try:
                firstLine_C1_end_time = get_firstLine_S2_C1(file)
            except Exception:
                firstLine_C1_end_time = None

    # --- Helper για C1/S2 header building (giống FCL) ---
    def get_content_C1_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1):
        get_first_line_1 = ",".join(firstLine.split(",")[10:18])
        get_first_line_2 = ",".join(firstLine.split(",")[21:27])
        try:
            yield_percent = round((Qty_IN / Qty_OUT) * 100, 2) if Qty_OUT else 0.00
        except Exception:
            yield_percent = 0.00
        get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(yield_percent) + "," + get_first_line_2
        firstLine_C1 = f"C1,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:"
        all_content = firstLine_C1 + content_C1 + ";"
        return all_content

    def get_content_S2_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data):
        get_first_line_1 = ",".join(firstLine.split(",")[10:18])
        get_first_line_2 = ",".join(firstLine.split(",")[21:27])
        try:
            yield_percent = round((Qty_IN / Qty_OUT) * 100, 2) if Qty_OUT else 0.00
        except Exception:
            yield_percent = 0.00
        get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(yield_percent) + "," + get_first_line_2
        firstLine_S2 = f"S2,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:\n"
        all_content = firstLine_S2 + out_data
        return all_content

    def generate_C1S2_file(file, all_content):
        with open(file, 'w', encoding='utf-8', errors='ignore') as output:
            output.write(all_content)

    # --- Duyệt từng sub tracecode để tạo C1,S2,DAT ---
    for trace_code in trace_code_list:
        print(f"[{datetime.now(Vietnam_time).strftime('%Y-%m-%d %H:%M:%S')}] {trace_code}")
        # lấy tempDf
        dut_condition = df_test['Assemblylot#'] == trace_code
        tempDf = df_test[dut_condition]

        # generate content_C1 (SWBin histogram)
        CAT_tempDf = tempDf['CAT'].fillna('')
        swPos_list = []
        for cat_line in CAT_tempDf:
            tempCat = str(cat_line).strip()[::-1]
            swcnt = 1
            tempval = ""
            for val in tempCat:
                if val != '0':
                    tempval = val
                    break
                swcnt += 1
            swPos = ""
            if tempval == '1':
                swPos = str((swcnt * 4) - 3)
            elif tempval == '2':
                swPos = str((swcnt * 4) - 2)
            elif tempval == '4':
                swPos = str((swcnt * 4) - 1)
            elif tempval == '8':
                swPos = str(swcnt * 4)
            if swPos:
                swPos_list.append(swPos)

        content_C1 = list("0" * 200)
        for i in swPos_list:
            idx = int(i) - 1
            if 0 <= idx < len(content_C1):
                content_C1[idx] = str(int(content_C1[idx]) + 1)
        content_C1 = ",".join(content_C1)

        # Qty_OUT/IN
        Qty_OUT = len(tempDf)
        Qty_IN = len(tempDf[tempDf['BIN'] == '001'])

        # pivot for S2 body
        pivot_table = tempDf.pivot_table(index='DUT', columns='BIN', values='Assemblylot#', aggfunc='count', fill_value=0)
        all_bins = [f"{i:03d}" for i in range(1, 9)]
        for bin_col in all_bins:
            if bin_col not in pivot_table.columns:
                pivot_table[bin_col] = 0
        pivot_table['TotalBin'] = pivot_table.sum(axis=1)
        pivot_table['Yield'] = (pivot_table['001'] / pivot_table['TotalBin']).fillna(0) * 100.0
        pivot_table['Yield'] = pivot_table['Yield'].round(2)
        pivot_table['GoodBin'] = pivot_table['001']
        pivot_table = pivot_table[['TotalBin', 'GoodBin', 'Yield', '001', '002', '003', '004', '005', '006', '007', '008']]

        try:
            pivot_table = pivot_table.sort_index(key=lambda s: s.astype(int))
        except Exception:
            pivot_table = pivot_table.sort_index()

        out_data = ""
        for dut_no, row in pivot_table.iterrows():
            try:
                no_str = str(int(dut_no))
            except Exception:
                no_str = str(dut_no)
            line = [
                no_str,
                str(int(row['TotalBin'])),
                str(int(row['GoodBin'])),
                f"{row['Yield']:.2f}",
                str(int(row['001'])),
                str(int(row['002'])),
                str(int(row['003'])),
                str(int(row['004'])),
                str(int(row['005'])),
                str(int(row['006'])),
                str(int(row['007'])),
                str(int(row['008']))
            ]
            out_data += ",".join(line) + ";\n"

        # Auto fill lines 1..line_no_S2
        tracecode_lines = [x for x in out_data.strip().splitlines() if x]
        existing = {}
        for line in tracecode_lines:
            no = line.split(',')[0]
            try:
                no_i = int(no)
            except Exception:
                continue
            existing[no_i] = line

        cur_no = 1
        max_line = line_no_S2 if line_no_S2 else (max(existing.keys()) if existing else 256)
        final_out_data = ""
        while cur_no <= max_line:
            if cur_no in existing:
                final_out_data += existing[cur_no] + "\n"
            else:
                final_out_data += f"{cur_no},0,0,0.00,0,0,0,0,0,0,0,0;\n"
            cur_no += 1

        # write C1 and S2 files (use firstLine templates if có)
        sub_tracecode = trace_code[:6]
        output_S2_C1_destination = kiomagd_path

        if firstLine_S2_end_time:
            # use template header from S2 to build headers
            try:
                # C1 header: if firstLine_C1_end_time exists use nó, else derive from S2 header by replacing S2->C1
                header_C1 = firstLine_C1_end_time if firstLine_C1_end_time else firstLine_S2_end_time.replace("S2", "C1", 1)
                header_S2 = firstLine_S2_end_time
            except Exception:
                header_C1 = None
                header_S2 = None
        else:
            header_C1 = None
            header_S2 = None

        # derive lot_no/dcc from S2 header if available
        if header_S2:
            try:
                lot_and_dcc = header_S2.split(',')[4]
            except Exception:
                lot_and_dcc = ""
        else:
            lot_and_dcc = ""

        lot_no, dcc_val = get_lot_dcc(lot_and_dcc)

        # Build and write C1
        if header_C1:
            try:
                allContent_C1 = get_content_C1_file(header_C1, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
                file_name_C1 = os.path.join(output_S2_C1_destination, f"C1_1_{sub_tracecode}_{tester}_{end_time}.txt")
                generate_C1S2_file(file_name_C1, allContent_C1)
                list_file_C1_S2_DAT.append(os.path.basename(file_name_C1))
                print(f"File C1 generated -> {file_name_C1}")
            except Exception as e:
                print(f"Cannot write C1 for {sub_tracecode}: {e}")
        else:
            # If no header template, still can attempt to build a minimal C1 header
            try:
                fake_first = f"C1,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,0,0,0,0,0,0,0,0,0,0,0,0,:"
                allContent_C1 = get_content_C1_file(fake_first, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
                file_name_C1 = os.path.join(output_S2_C1_destination, f"C1_1_{sub_tracecode}_{tester}_{end_time}.txt")
                generate_C1S2_file(file_name_C1, allContent_C1)
                list_file_C1_S2_DAT.append(os.path.basename(file_name_C1))
                print(f"[WARN] C1 generated using fallback header -> {file_name_C1}")
            except Exception as e:
                print(f"[ERROR] Cannot generate fallback C1 for {sub_tracecode}: {e}")

        # Build and write S2
        if header_S2:
            try:
                allContent_S2 = get_content_S2_file(header_S2, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, final_out_data)
                file_name_S2 = os.path.join(output_S2_C1_destination, f"S2_1_{sub_tracecode}_{tester}_{end_time}.txt")
                generate_C1S2_file(file_name_S2, allContent_S2)
                list_file_C1_S2_DAT.append(os.path.basename(file_name_S2))
                print(f"File S2 generated -> {file_name_S2}")
            except Exception as e:
                print(f"Cannot write S2 for {sub_tracecode}: {e}")
        else:
            try:
                fake_first_s2 = f"S2,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,0,0,0,0,0,0,0,0,0,0,0,0,:\n"
                allContent_S2 = get_content_S2_file(fake_first_s2, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, final_out_data)
                file_name_S2 = os.path.join(output_S2_C1_destination, f"S2_1_{sub_tracecode}_{tester}_{end_time}.txt")
                generate_C1S2_file(file_name_S2, allContent_S2)
                list_file_C1_S2_DAT.append(os.path.basename(file_name_S2))
                print(f"[WARN] S2 generated using fallback header -> {file_name_S2}")
            except Exception as e:
                print(f"[ERROR] Cannot generate fallback S2 for {sub_tracecode}: {e}")

        # --- Build DAT Summary: THAY phần đọc DAT gốc bằng S2 header + stored_procedure ---
        # Extract metadata from S2 header (if any)
        try:
            if header_S2:
                half_first_line = header_S2.split(':')[0] if ':' in header_S2 else header_S2
                lot_and_dcc = half_first_line.split(',')[4]
                deviceName = half_first_line.split(',')[11]
                test_station_code = half_first_line.split(',')[12]
                recipeName = half_first_line.split(',')[16]
                retestCode = half_first_line.split(',')[-3]
                testerName = half_first_line.split(',')[13]
                handlerNo = half_first_line.split(',')[-4]
                boardID = half_first_line.split(',')[14]
                programName = half_first_line.split(',')[17]
                operatorID = half_first_line.split(',')[-6]
                startTime = half_first_line.split(',')[-7]
                endTime = half_first_line.split(',')[-8]
                # content S2 body for dat
                all_content_S2 = "\n".join(allContent_S2.splitlines()[1:]) if 'allContent_S2' in locals() else ""
            else:
                # fallback minimal
                lot_and_dcc = f"{lot_no}"
                deviceName = ""
                test_station_code = ""
                recipeName = ""
                retestCode = "0"
                testerName = tester
                handlerNo = ""
                boardID = ""
                programName = ""
                operatorID = ""
                startTime = ""
                endTime = ""
                all_content_S2 = final_out_data
        except Exception:
            lot_and_dcc = f"{lot_no}"
            deviceName = ""
            test_station_code = ""
            recipeName = ""
            retestCode = "0"
            testerName = tester
            handlerNo = ""
            boardID = ""
            programName = ""
            operatorID = ""
            startTime = ""
            endTime = ""
            all_content_S2 = final_out_data

        # normalize lot & dcc
        lot_no_from_s2, dcc_from_s2 = get_lot_dcc(lot_and_dcc)

        # call stored_procedure to get operation/teststep/temperature
        data_list = ['get_temperature', lot_no_from_s2, dcc_from_s2, programName, '']
        try:
            get_opr_temp = stored_procedure.set_unit_data(data_list)
        except Exception as e:
            get_opr_temp = None
            print(f"[WARN] stored_procedure.set_unit_data error: {e}")

        if get_opr_temp:
            try:
                OPER_CODE = str(get_opr_temp.get('WPOPR'))
            except Exception:
                OPER_CODE = get_test_station(test_station_code) or "000"
            try:
                TEST_STEP = str(get_opr_temp.get('WPOPRN'))
            except Exception:
                TEST_STEP = "UNKNOWN"
            try:
                TEMPERATURE = str(get_opr_temp.get('WPCOND')).strip().split('+')[0]
                if '.' not in TEMPERATURE:
                    TEMPERATURE += '.00'
            except Exception:
                TEMPERATURE = "25.00"
        else:
            OPER_CODE = get_test_station(test_station_code) or "000"
            TEST_STEP = "UNKNOWN"
            TEMPERATURE = "25.00"

        # final DAT variables
        CUSTOM_LOT_NO = lot_no_from_s2
        LOT_DCC = dcc_from_s2
        if LOT_DCC:
            LOT_ID = f"{CUSTOM_LOT_NO}({LOT_DCC})"
        else:
            LOT_ID = CUSTOM_LOT_NO
        KEY_NO = sub_tracecode
        DEVICE_NAME = deviceName
        RECIPE_NAME = recipeName
        PROGRAM_NAME = programName
        HANDLER_NO = handlerNo
        BOARD_ID = boardID
        OPERATOR_ID = operatorID
        TEST_CODE = TEST_CODE if 'TEST_CODE' in locals() else testcode
        RETEST_CODE = retestCode
        STARTTIME = startTime
        ENDTIME = endTime

        # hBin build from totalBins
        try:
            # rebuild hBin each iteration to ensure synced
            hBin_list_tmp = []
            totalbinperfile = sum(totalBins.values()) if totalBins else sum(bins.values())
            for b in sorted(totalBins.keys()):
                qty = int(totalBins.get(b, 0))
                if b == "001":
                    hBin_list_tmp.append(f"{int(b)},{1},{qty},{normal_round((qty / totalbinperfile) * 100, 2)}")
                else:
                    hBin_list_tmp.append(f"{int(b)},{0},{qty},{normal_round((qty / totalbinperfile) * 100, 2)}")
            # ensure 1..8
            hBin_list_tmp2 = []
            bin_nums = [int(x.split(",")[0]) for x in hBin_list_tmp]
            for i in range(1, 9):
                if i in bin_nums:
                    # find corresponding
                    for item in hBin_list_tmp:
                        if int(item.split(",")[0]) == i:
                            hBin_list_tmp2.append(item)
                            break
                else:
                    if i == 1:
                        hBin_list_tmp2.append(f"{i},1,0,0.00000")
                    else:
                        hBin_list_tmp2.append(f"{i},0,0,0.00000")
            hBin_final = "/".join(hBin_list_tmp2) + "/"
        except Exception:
            hBin_final = ""

        # content_s2
        content_s2_body = all_content_S2 if all_content_S2 else ""

        # Generate DAT via summary_generate_kioxia
        try:
            dat_file_content = summary_generate_kioxia.generate_summary(
                lot_id=LOT_ID, keyno=KEY_NO, device_name=DEVICE_NAME, customer_lot_no=CUSTOM_LOT_NO,
                recipe_name=RECIPE_NAME, oper_code=OPER_CODE, test_step=TEST_STEP, test_code=TEST_CODE,
                retest_code=RETEST_CODE, tester_name=testerName, handler_no=HANDLER_NO, board_id=BOARD_ID,
                temperature=TEMPERATURE, program_name=PROGRAM_NAME, handler_para_file='',
                lot_quantity=sum(totalBins.values()) if totalBins else len(tempDf), operator_id=OPERATOR_ID,
                STARTTIME=STARTTIME, ENDTIME=ENDTIME,
                hBin=hBin_final, swBin=SWBin, content_s2=content_s2_body
            )
        except Exception as e:
            print(f"[ERROR] summary_generate_kioxia.generate_summary error: {e}")
            dat_file_content = ""

        # Save DAT
        try:
            filename_dat = f"KIOXIA_{tester}_{PROGRAM_NAME}_{CUSTOM_LOT_NO}_{sub_tracecode}_{OPER_CODE}_{TEST_CODE}_{RETEST_CODE}_{end_time}.DAT"
            summary_generate_kioxia.save_summary_file(file_path=kiomagd_TSV_Summary, file_name=filename_dat, content=dat_file_content)
            list_file_C1_S2_DAT.append(filename_dat)
            print(f"TSV Summary was generated successfully ! -> {os.path.join(kiomagd_TSV_Summary, filename_dat)}")
        except Exception as e:
            print(f"Cannot save DAT file: {e}")

    # print binning summary (giống FCL)
    print("[Binning]\n" +
          "All Bins: " + str(totalBins) + "\n" +
          "Out Qty: " + str(totalBins.get("001", 0)) + "\n" +
          "Total Qty: " + str(sum(totalBins.values())) + "\n" +
          "Soft Bin: " + str(SWBin) + "\n" +
          "Hard Bin: " + str(hBin_final) + "\n" +
          "[Binning End]\n")

    # cleanup
    totalBins.clear()
    SWBin.clear()
    cursor.close()
    cnxn.close()


# ----------------------
# Move generated files (C1/S2/DAT) - giữ nguyên thứ tự/flow
# ----------------------
def move_C1_S2_DAT(list_file_C1_S2_DAT):
    source_C1S2_DAT = kiomagd_path
    destination_C1S2 = kiomagd_C1_S2_Done
    destination_DAT = kiomagd_DAT_Done
    # list_file_C1_S2_D

    # ensure dirs
    os.makedirs(destination_C1S2, exist_ok=True)
    os.makedirs(destination_DAT, exist_ok=True)

    file_set = set(list_file_C1_S2_DAT)
    for file in file_set:
        try:
            if file.startswith('C1_1') or file.startswith('S2_1'):
                src = os.path.join(source_C1S2_DAT, file)
                dst = os.path.join(destination_C1S2, file)
                # Uncomment to move
                # shutil.move(src, dst)
                # print(f"[MOVE] {src} -> {dst}")
            elif file.upper().endswith('.DAT'):
                # DAT saved in kiomagd_TSV_Summary
                src = os.path.join(kiomagd_TSV_Summary, file)
                dst = os.path.join(destination_DAT, file)
                if os.path.exists(src):
                    shutil.move(src, dst)
                    print(f"[MOVE] {src} -> {dst}")
        except Exception as e:
            print(f"[WARN] Cannot move {file} -> {e}")


# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    print(datetime_str, "\t####################START PARSING####################")
    # separate asc into subfolders
    try:
        separate_sublot.generate_asc_file(in_asc_file_path=Combine_lot_result_file, out_asc_file_path=Separate_Sublot)
    except Exception as e:
        print(f"[ERROR] separate_sublot.generate_asc_file error: {e}")

    list_folder = list_folders(Separate_Sublot)
    if len(list_folder) == 0:
        print(datetime_str, "\t\t\tWAITING_ASC_FILE_CONVERT")
    else:
        for folder in list_folder:
            try:
                full_parsing(folder)
                shutil.rmtree(folder)
                print(f"Folder deleted: {folder}\n")
            except Exception as error_below:
                print(f"Cannot parsing folder: {folder}\n")
                print(error_below)
                try:
                    shutil.rmtree(folder)
                except Exception:
                    pass

        # optionally move generated files
        # move_C1_S2_DAT(list_file_C1_S2_DAT)

    print(f"{datetime.now(Vietnam_time).strftime('%Y-%m-%d %H:%M:%S')}", "\t####################ENDED####################\n")
