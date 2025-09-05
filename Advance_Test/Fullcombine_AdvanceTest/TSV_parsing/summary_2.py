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
import stored_procedure

Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
datetime_str = str(datetime.now(Vietnam_time))
datetime_str = datetime_str.split('.')[0]

config = configparser.ConfigParser()
# Read the path_config.ini file
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

list_file_C1_S2_DAT = []

def list_folders(directory_path):
    folders = []
    for item in os.listdir(directory_path):
        if os.path.isdir(os.path.join(directory_path, item)):
            folders.append(os.path.join(directory_path,item))
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
# ---------------------- PHẦN 2: full_parsing (chi tiết) ----------------------

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

def full_parsing(mypath: str):
    bins = {}
    totalBins = {}
    tBoard = []
    dut = []
    keyno = []
    linBin = []
    catData = []
    SWBin = {}
    sBin = {}

    def parseAllFiles(f,file,tBoard,dut,keyno,linBin,catData):
        alltxt = ""
        testFlag = ""
        recipe = ""
        rtFlag = ""
        lines = f.readlines()
        rtRule = {"A":"2,3,4,5,6,7,8", #Retest Rule
            "B":"3,4,5,6,7,8",
            "C":"4,5,6,7,8",
            "D":"5,6,7,8",
            "E":"5,6,8",
            "F":"5,8",
            "G":"5,8",
            "H":"3,5,6,7,8",
            "I":"5,6,8",
            "J":"5,7,8",
            "K":"5,6,8",
            "L":"4,5,8"}
        try:
            testFlag = file.split("_")[3].strip()
            recipe = file.split("_")[4].strip()
            
            for line in lines:
                alltxt +=line
                if "STN" not in line:
                    curBin = line.split(",")[3].strip()
                    if curBin not in bins:
                        if testFlag != "INI":
                            rtFlag = recipe[0] #Retest Bin Rule
                            if curBin != "001":
                                curBin_temp = curBin[-1]
                                if curBin_temp in rtRule[rtFlag]:
                                    bins.update({curBin: 1}) #add bin if bin rule pass (mean all retest Bin is good if bin rule pass)
                                else:
                                    bins.update({curBin: 1})
                            else:
                                bins.update({curBin: 1}) #add bin if bin is 001
                        else:
                            bins.update({curBin: 1}) #add bin if bin is from INI file
                    else:
                        tmpBin = bins[curBin] + 1 
                        bins.update({curBin: tmpBin}) #add bin + 1 if bin already exist
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
                        swcnt+=1 #Count string index for value placement
                    if tmpVal == "1": #Set value rule (8=1000; 4=0100; 2=0010; 1=0001)
                        swPos = "SWCAT_"+ str((swcnt*4) - 3) + "_issue"
                    elif tmpVal == "2":
                        swPos = "SWCAT_"+str((swcnt*4) - 2) + "_issue"
                    elif tmpVal == "4":
                        swPos = "SWCAT_"+str((swcnt*4) - 1) + "_issue"
                    elif tmpVal == "8":
                        swPos = "SWCAT_"+str((swcnt*4)) + "_issue"
                    else:
                        swPos = "good" #set Sbin name to Good if bin is Sbin has no value
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

            for val in bins.keys(): #Set totalBins values by getting sum of all bins
                if val not in totalBins:
                    totalBins[val] = bins.get(val,0)
                else:
                    for val2 in totalBins.keys():
                        if val == val2:
                            temp = totalBins.get(val,0) + bins.get(val,0)
                            totalBins.update({val:temp})
            for val in sBin.keys(): #Set total Sbin by getting sum of all Sbins
                if val not in SWBin:
                    if len(sBin) > 0:
                        SWBin[val] = sBin.get(val,0)
                else:
                    for val2 in SWBin.keys():
                        if val in SWBin:
                            if val == val2:
                                for val3 in sBin[val].keys():
                                    for val4 in SWBin[val].keys():
                                        if val4 == val3:
                                            temp = SWBin[val].get(val3,0) + sBin[val].get(val3,0)
                                            SWBin[val].update({val3 : temp})
                                        else:
                                            SWBin[val].update(sBin[val])
                                            break
                        else:
                            SWBin.update({val:sBin[val]})
                            break

        except:
            print("Cannot parse: "+str(file))
        return alltxt

    #change pyodbc driver depending on Linux
    data_base_os = '/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1'
    cnxn = pyodbc.connect(f"DRIVER={data_base_os};UID=cimitar2;PWD={password};Database=MCSDB;Server={server};TrustServerCertificate=yes;")
    
    #change pyodbc driver depending on OS
    # databx_linux = 'ODBC Driver 17 for SQL Server'
    # cnxn = pyodbc.connect(f"DRIVER={databx_linux};SERVER={server};DATABASE=TSV;UID=cimitar2;PWD={password}")
    cursor = cnxn.cursor()
    #change source path upon deployment
    # mypath = "/kiomagd/Combine_lot_result_file/output"
    # mypath = "D:/Combine_lot_result_file/output/test/Convert_asc/parsed/Separate_Sublot"
    paths = glob.glob(os.path.join(mypath, '*.asc'))
    # paths = sorted(Path(mypath).iterdir(), key=os.path.getmtime)
    hBin = ""
    totalSbin = ""
    sumtxt = ""
    for fullpath in paths: #iterate each files in the folder
        totalOutQty = 0
        otherQty = 0
        lot_name = ""
        dcc = ""
        file_temp = ntpath.basename(fullpath)
        end_time = str(file_temp).split('_')[5]
        time_get_file = end_time[:-2]
        main_tracecode = str(file_temp).split('_')[0]
        qty_sublot = file_temp.split("_")[-2]
        testtime = str(file_temp).split('_')[3]
        testcode = str(file_temp).split('_')[1]
        f = open(fullpath,"r")
        if "_processed.asc" in file_temp:
            sumtxt += "\n"+parseAllFiles(f,file_temp,tBoard,dut,keyno,linBin,catData)
            tester = file_temp.split("_")[2].strip()
            # original FCL used stored proc or a query here to get lot; keep command for compatibility
            cmd = ("TSV.dbo.USP_Summary_GetLotFromKeyno'"+str(main_tracecode)+"'") #Get lot name using trace code from filename
            try:
                cursor.execute(cmd)
                lotdata = cursor.fetchone()
                if lotdata:
                    lot_name = str(lotdata[0]).strip()
                    if '.' in lot_name:
                        lot_name = lot_name.split('.')[0]
                    dcc = str(lotdata[1]).strip()
                    opr = str(lotdata[2]).strip()
            except Exception:
                # in advance flow we'll rely on S2+stored_procedure later for opr/test step
                pass
            
            totalbinperfile = sum(bins.values()) if bins else 0
            for binno in bins.keys():
                qty = int(bins.get(binno,0))
                if binno == "001":
                    bin1Qty = int(bins.get(binno,0))
                    if totalbinperfile:
                        hBin += binno+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
                    else:
                        hBin += binno+",1,"+str(qty)+",0.00/"
                else:
                    if totalbinperfile:
                        hBin += binno+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
                    else:
                        hBin += binno+",0,"+str(qty)+",0.00/"
            for binno in sBin.keys():
                for name in sBin[binno].keys():
                    qty = int(sBin[binno].get(name,0))
                    if binno == "001":
                        if totalbinperfile:
                            totalSbin += "0,"+binno+","+name+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
                        else:
                            totalSbin += "0,"+binno+","+name+",1,"+str(qty)+",0.00/"
                    else:
                        sbinNo = name.split("_")[1]
                        if totalbinperfile:
                            totalSbin += sbinNo+","+binno+","+name+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
                        else:
                            totalSbin += sbinNo+","+binno+","+name+",0,"+str(qty)+",0.00/"
            tBoard.clear()
            dut.clear()
            keyno.clear()
            bins.clear()
            catData.clear()
            sBin.clear()
            totalSbin = ""
            bins.clear()
            sBin.clear()
            f.close()

        totalOutQty += totalBins.get("001",0)
        otherQty += sum(totalBins.values())
        #__________________Start generating C1S2 and TSV Summary file__________________
        header = ['STN','DUT','Assemblylot#','BIN','CAT']
        all_lines = []

        lines = sumtxt.splitlines() #sumtxt contain all content of full combine lot
        for line in lines:
            if "STN,DUT,Assemblylot#,BIN,CAT" not in line and line != "":
                all_lines.append(line.strip().split(',')[0:-1])
        df_test = pd.DataFrame(columns=header)
        for i,value in enumerate(all_lines):
            df_test.loc[i] = value
        trace_code_list = df_test['Assemblylot#'].unique()
        tempDf = pd.DataFrame()
        out_df = pd.DataFrame()

        #Get first line from C1S2 MAGNUM files
        def get_firstLine_S2_C1(file, list_file_C1_S2_DAT):
            list_file_C1_S2_DAT.append(file)
            file_path = source_C1S2 + "/" + file
            with open(file_path,'r') as read_file:
                first_line = read_file.readline()
            return first_line
        source_C1S2 = kiomagd_path
        # source_C1S2 = r"C:\Users\700445\Downloads\TA0017\summary\input"
        allFile_C1S2 = os.listdir(source_C1S2)
        tracecode_S2 = "S2_1_" + main_tracecode
        tracecode_C1 = "C1_1_" + main_tracecode
        test_station = ''
        for file in allFile_C1S2: #firstLine_tracecode
            #Get S2 file initial test
            # if tracecode_S2 in str(file) and start_time in str(file):
            #     firstLine_S2_start_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
            #Get S2 file retest
            if tracecode_S2 in str(file) and time_get_file in str(file):
                list_file_C1_S2_DAT.append(file)        
                file_S2_end_time = source_C1S2 + "/" + file
                with open(file_S2_end_time,'r') as read_file:
                    firstLine_S2_end_time = read_file.readline()
                    endLine_S2_end_time = read_file.readlines()[-1]
                    line_no_S2 = int(endLine_S2_end_time.split(',')[0])
            #Get C1 file initial test 
            # if tracecode_C1 in str(file) and start_time in str(file):
            #     firstLine_C1_start_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
            #     test_station = firstLine_C1_start_time.split(',')[12]
            #Get C1 file 
            elif tracecode_C1 in str(file) and time_get_file in str(file):
                firstLine_C1_end_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
                # test_station = firstLine_C1_end_time.split(',')[12]
            #Get code station     
            # if test_station:
            #     code_station = get_test_station(test_station)

        # Loop each sub tracecode
        for trace_code in trace_code_list:
            # print(trace_code_list)
            print(f"[{datetime.now(Vietnam_time).strftime('%Y-%m-%d %H:%M:%S')}] {trace_code}")
            dut_condition = df_test['Assemblylot#'] == trace_code
            sub_tracecode = trace_code[:6]
            tempDf = df_test[dut_condition]
            query_sql = f"""SELECT DISTINCT WPLOT#, WPDCC FROM OPENQUERY([DATA400], 'SELECT * FROM EMLIB.ECSRWP04 
                    WHERE WMISC1 = ''{sub_tracecode}'' AND WSTS1 <> ''CLOSE'' AND WSUB# = 0 ') """
            try:
                cursor.execute(query_sql)
                sql_data = cursor.fetchone()
                lot_no = sql_data[0]
                dcc = sql_data[1]
            except Exception:
                lot_no = ""
                dcc = ""
            # print(tempDf)
            # dut_list = tempDf['DUT'].unique()
            # len_dut_list = len(dut_list)
            # Get SWBIN write out to C1 file
            CAT_tempDf = tempDf['CAT']
            swPos_list = []
            for cat_line in CAT_tempDf:
                tempCat = cat_line.strip()[::-1]
                swcnt = 1
                tempval = ""
                swPos = ""
                for val in tempCat:
                    if val != '0':
                        tempval = val
                        break
                    swcnt += 1
                if tempval == '1':
                    swPos = str((swcnt * 4) - 3)
                elif tempval == '2':
                    swPos = str((swcnt * 4) - 2)
                elif tempval == '4':
                    swPos = str((swcnt * 4) - 1)
                elif tempval == '8':
                    swPos = str(swcnt * 4)
                if len(swPos) != 0:
                    swPos_list.append(swPos)
            content_C1 = '0' *200
            content_C1_list = []
            content_C1_list = list(content_C1)
            if len(swPos_list) != 0:
                for i in swPos_list:
                    content_C1_list[int(i)-1] = str(int(content_C1_list[int(i)-1]) + 1)
            # Yield good of sub tracecode
            count_BIN = tempDf['BIN']
            Qty_OUT = 0
            Qty_IN = 0
            for i in count_BIN:
                Qty_OUT += 1
                if i == '001':
                    Qty_IN += 1
            
            content_C1 = ','.join(content_C1_list)
            
            # Write sub tracecode for C1
            output_S2_C1_destination = kiomagd_path

            def get_content_C1_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1):
                get_first_line_1 = ",".join(firstLine.split(",")[10:18])
                get_first_line_2 = ",".join(firstLine.split(",")[21:27])
                get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(round((Qty_IN/Qty_OUT)*100,2)) + "," + get_first_line_2
                firstLine_C1 = f"C1,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:"
                all_content = firstLine_C1 + content_C1 + ";"
                return all_content
            
            def generate_C1S2_file(file, all_content):
                with open(file, 'w') as output:
                    output.write(all_content)

            if testtime == 'INI' or testtime == 'RT1':
            #     if 'firstLine_C1_start_time' not in locals():
            #         print("Cannot find C1 file")
            #         break 
            #     allContent_C1 = get_content_C1_file(firstLine_C1_start_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
            #     file_name_C1 = output_S2_C1_destination +  f"/C1_1_{sub_tracecode}_{tester}_{start_time}.txt"
            #     generate_C1S2_file(file_name_C1, allContent_C1)
            #     print(f"File C1 was generate successfully ! -> {file_name_C1}")
            
            # elif testtime == 'RT1':
                if 'firstLine_C1_end_time' not in locals():
                    print("Cannot find C1 file")
                    break
                allContent_C1 = get_content_C1_file(firstLine_C1_end_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
                file_name_C1 = output_S2_C1_destination + f"/C1_1_{sub_tracecode}_{tester}_{end_time}.txt"
                generate_C1S2_file(file_name_C1, allContent_C1)
                print(f"File C1 was generate successfully ! -> {file_name_C1}")

            bin1 = tempDf[tempDf['BIN'] == "001"]
            bin2 = tempDf[tempDf['BIN'] == "002"]
            bin3 = tempDf[tempDf['BIN'] == "003"]
        
            # Create a pivot table to count occurrences of each BIN for each DUT
            pivot_table = tempDf.pivot_table(index='DUT', columns='BIN', values='Assemblylot#', aggfunc='count', fill_value=0)
            # Add all BIN columns (001 to 008) and set count to 0 if not present
            all_bins = [f"{i:03d}" for i in range(1, 9)]
            for bin_col in all_bins:
                if bin_col not in pivot_table.columns:
                    pivot_table[bin_col] = 0
            # Add a "TotalBin" column that equals the count of all bins
            pivot_table['TotalBin'] = pivot_table.sum(axis=1)
            # Calculate Yield bin 001 relative to the total
            pivot_table['Yield'] = round((pivot_table['001'] / pivot_table['TotalBin']) * 100,2)
            # Add good bin columns
            pivot_table['GoodBin'] = pivot_table['001']
            # Reorder the columns
            pivot_table = pivot_table[['TotalBin','GoodBin','Yield','001','002','003','004','005','006','007','008']]
            str_test = pivot_table.to_string()
            # print(str_test)

            all_lines = str_test.splitlines()
            out_data = ""
            for line in all_lines:
                if "BIN" in line or "DUT" in line:
                    continue
                line_list = " ".join(line.split()).split(" ")
                line_list[0] = str(int(line_list[0]))
                out_data += ",".join(line_list) + ";\n"
            
            # print(out_data)

            # Auto fill line from 1 to 256 ot 512 if it missing
            tracecode_lines = out_data.strip().splitlines()
            last_sub_tracecode = ""
            start_no = [int(line.split(',')[0]) for line in tracecode_lines if line]
            cur_no = 1
            for line in tracecode_lines:
                number = int(line.split(',')[0])
                while cur_no < number:
                    last_sub_tracecode += f"{cur_no},0,0,0.00,0,0,0,0,0,0,0,0;\n"
                    cur_no += 1
                last_sub_tracecode += f"{line}\n"
                cur_no += 1
            while cur_no > start_no[-1]:
                if cur_no == line_no_S2 + 1:
                    break
                last_sub_tracecode += f'{cur_no},0,0,0.00,0,0,0,0,0,0,0,0;\n' 
                cur_no += 1

            out_data = last_sub_tracecode

            # Write sub tracecode for S2
            def get_content_S2_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data):
                get_first_line_1 = ",".join(firstLine.split(",")[10:18])
                get_first_line_2 = ",".join(firstLine.split(",")[21:27])
                get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(round((Qty_IN/Qty_OUT)*100,2)) + "," + get_first_line_2
                firstLine_S2 = f"S2,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:\n"
                all_content = firstLine_S2 + out_data
                return all_content
             
            # if testtime == 'INI':
            #     if 'firstLine_S2_start_time' not in locals():
            #         print("Cannot find S2 file")
            #         break
            #     allContent_S2 = get_content_S2_file(firstLine_S2_start_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data)
            #     file_name_S2 = output_S2_C1_destination + f"/S2_1_{sub_tracecode}_{tester}_{start_time}.txt"
            #     generate_C1S2_file(file_name_S2, allContent_S2)
            #     print(f"File S2 was generate successfully ! -> {file_name_S2}")

            if testtime == 'INI' or testtime == 'RT1':
                if 'firstLine_S2_end_time' not in locals():
                    print("Cannot find S2 file")
                    break
                allContent_S2 = get_content_S2_file(firstLine_S2_end_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data)
                file_name_S2 = output_S2_C1_destination + f"/S2_1_{sub_tracecode}_{tester}_{end_time}.txt"
                generate_C1S2_file(file_name_S2, allContent_S2)
                print(f"File S2 was generate successfully ! -> {file_name_S2}")

            #Generate TSV Summary DAT file
            # --- THAY ĐOẠN: đọc DAT có sẵn -> BẰNG đọc S2 header + stored_procedure ---
            # (FCL gốc quét *.DAT để đọc metadata; ở đây ta dùng S2 header + stored_procedure)
            try:
                # lấy header từ firstLine_S2_end_time (đã đọc trước đó)
                if 'firstLine_S2_end_time' in locals() and firstLine_S2_end_time:
                    half_first_line = firstLine_S2_end_time.split(':')[0] if ':' in firstLine_S2_end_time else firstLine_S2_end_time
                    # parse theo offset giống summary_generate_main
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
                    # content S2 body
                    try:
                        bin_details = "\n".join(allContent_S2.splitlines()[1:])
                    except Exception:
                        bin_details = ""
                else:
                    # fallback: nếu không có S2 header
                    lot_and_dcc = ""
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
                    bin_details = "\n".join(allContent_S2.splitlines()[1:])
            except Exception as e:
                print(f"Cannot parse S2 header: {e}")


            # lấy lot_no/dcc
            lot_no_s2, dcc_s2 = get_lot_dcc(lot_and_dcc)

            # gọi stored_procedure (advance) giống summary_generate_main
            data_list = ['get_temperature', lot_no_s2, dcc_s2, programName,'']
            try:
                get_opr_temp = stored_procedure.set_unit_data(data_list)
            except Exception as e:
                print(f"stored_procedure.set_unit_data error: {e}")
                get_opr_temp = None

            if get_opr_temp:
                try:
                    OPER_CODE = str(get_opr_temp.get('WPOPR'))
                except Exception:
                    OPER_CODE = get_test_station(test_station_code) or ""
                try:
                    TEST_STEP = str(get_opr_temp.get('WPOPRN'))
                except Exception:
                    TEST_STEP = ""
                try:
                    TEMPERATURE = str(get_opr_temp.get('WPCOND')).strip().split('+')[0]
                    if '.' not in TEMPERATURE:
                        TEMPERATURE += '.00'
                except Exception:
                    TEMPERATURE = '25.00'
            else:
                # fallback
                OPER_CODE = get_test_station(test_station_code) or ""
                TEST_STEP = ""
                TEMPERATURE = '25.00'

            # chuẩn hoá LOT ID
            CUSTOM_LOT_NO = lot_no_s2
            LOT_DCC = dcc_s2
            if LOT_DCC == "":
                LOT_ID = CUSTOM_LOT_NO
            else:
                LOT_ID = f"{CUSTOM_LOT_NO}({LOT_DCC})"

            # chuẩn biến cho generate_summary (giữ tên giống FCL)
            KEY_NO = sub_tracecode
            DEVICE_NAME = deviceName
            RECIPE_NAME = recipeName
            OPER_CODE = OPER_CODE
            TEST_STEP = TEST_STEP
            TEST_CODE = testcode
            RETEST_CODE = retestCode
            TESTER_NAME = testerName
            HANDLER_NO = handlerNo
            BOARD_ID = boardID
            TEMPERATURE = TEMPERATURE
            PROGRAM_NAME = programName
            HANDLER_PARA_FILE = ''
            OPERATOR_ID = operatorID
            STARTTIME = startTime
            ENDTIME = endTime

            # build hBin final (giống FCL)
            hBin_list = hBin.split("/")[0:-1]
            hBin_list = [str(int(x.split(",")[0])) + "," + ",".join(x.split(",")[1:]) for x in hBin_list]
            bin_split = [int(x.split(",")[0]) for x in hBin_list]
            for i in range(1,9):
                if i not in bin_split:
                    if i != 1:
                        hBin_list.append(f"{i},0,0,0.00000")
                    else:
                        hBin_list.append(f"{i},1,0,0.00000")
            hBin_list = sorted(hBin_list)
            hBin_final = "/".join(hBin_list) + "/"

            # tạo nội dung DAT bằng summary_generate_kioxia
            try:
                dat_file_content = summary_generate_kioxia.generate_summary(
                    lot_id=LOT_ID, keyno=KEY_NO, device_name=DEVICE_NAME, customer_lot_no=CUSTOM_LOT_NO,
                    recipe_name=RECIPE_NAME, oper_code=OPER_CODE, test_step=TEST_STEP, test_code=TEST_CODE,
                    retest_code=RETEST_CODE, tester_name=TESTER_NAME, handler_no=HANDLER_NO, board_id=BOARD_ID,
                    temperature=TEMPERATURE, program_name=PROGRAM_NAME, handler_para_file=HANDLER_PARA_FILE,
                    lot_quantity=otherQty,operator_id=OPERATOR_ID, STARTTIME=STARTTIME, ENDTIME=ENDTIME,
                    hBin=hBin_final, swBin=SWBin, content_s2=bin_details
                )
            except Exception as e:
                print(f"Error generate_summary: {e}")
                dat_file_content = ""

            filename_dat = f"KIOXIA_{tester}_{PROGRAM_NAME}_{CUSTOM_LOT_NO}_{sub_tracecode}_{OPER_CODE}_{TEST_CODE}_{RETEST_CODE}_{end_time}.DAT"
            try:
                summary_generate_kioxia.save_summary_file(file_path = kiomagd_TSV_Summary, file_name = filename_dat, content = dat_file_content)
                list_file_C1_S2_DAT.append(filename_dat)
                print(f"TSV Summary was generated successfully ! -> {kiomagd_TSV_Summary}/{filename_dat}")
            except Exception as e:
                print(f"Cannot save DAT: {e}")
        #__________________End of generating C1S2 and TSV Summary file__________________
        # hBin = ""
        # sumtxt = ""
        print("[Binning]\n" +
        "All Bins: "+str(totalBins)+"\n"+
        "Out Qty: "+str(totalOutQty)+"\n"+
        "Total Qty: "+str(otherQty)+"\n"+
        "Soft Bin: "+str(SWBin)+"\n"+
        "Hard Bin: "+str(hBin_final)+"\n"+
        "[Binning End]\n")
        # print("#" * len(str(file)),"\t",datetime_str,"\n")
        totalBins.clear()
        SWBin.clear()

# ---------------------- End of PHẦN 2 ----------------------
# ---------------------- PHẦN 3: move_C1_S2_DAT + main ----------------------

def move_C1_S2_DAT(list_file_C1_S2_DAT):
    source_C1S2_DAT = kiomagd_path
    destination_C1S2 = kiomagd_C1_S2_Done
    destination_DAT = kiomagd_DAT_Done
    # source_C1S2_DAT = r"C:\Users\700445\Downloads\TA0017\summary\input"
    # destination_C1S2 = r"C:\Users\700445\Downloads\TA0017\summary\output"
    # destination_DAT = r"C:\Users\700445\Downloads\TA0017\summary\output"
    list_file_C1_S2_DAT = set(list_file_C1_S2_DAT)
    for file in list_file_C1_S2_DAT:
        if 'C1_1' in file or 'S2_1' in file:
            source_path = os.path.join(source_C1S2_DAT, file)
            destination = os.path.join(destination_C1S2, file)
            # shutil.move(source_path, destination)
            # print(f"Moved -> {destination}")
        if str(file).split('.')[-1] == 'DAT':
            source_path = os.path.join(source_C1S2_DAT, file)
            destination = os.path.join(destination_DAT, file)
            shutil.move(source_path, destination)
            # print(f"Moved -> {destination}")

if __name__ == "__main__":
    print(datetime_str,"\t####################START PARSING####################")
    separate_sublot.generate_asc_file(in_asc_file_path=Combine_lot_result_file,
                       out_asc_file_path=Separate_Sublot)
    # # list_folder = list_folders(r"C:\Users\700445\Downloads\TA0017\asc\Separate_Sublot")
    list_folder = list_folders(Separate_Sublot)
    if len(list_folder) == 0:
        print(datetime_str,"\t\t\tWAITING_ASC_FILE_CONVERT", )
    else:
        for folder in list_folder:
            try:
                full_parsing(folder)
                shutil.rmtree(folder)
                print(f"Folder deleted: {folder}\n")
            except Exception as error_below:
                print(f"Cannot parsing folder: {folder}\n")
                print(error_below)
                shutil.rmtree(folder)
            # print(f"Folder deleted: {folder}\n")
        # move_C1_S2_DAT(list_file_C1_S2_DAT)

    print(f"{datetime.now(Vietnam_time).strftime('%Y-%m-%d %H:%M:%S')}", "\t####################END OF PARSING####################\n")

# ---------------------- END OF FILE ----------------------
