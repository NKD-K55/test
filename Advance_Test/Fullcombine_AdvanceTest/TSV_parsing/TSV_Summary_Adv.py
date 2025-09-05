# ##!/usr/bin/python3.12
# import pyodbc
# from datetime import datetime
# import sys
# from os import listdir
# import os
# from os.path import isfile, join
# from pathlib import Path
# import ntpath
# import shutil
# import pytz
# import pandas as pd
# import glob
# import summary_generate_kioxia
# import configparser
# import separate_sublot

# Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
# datetime_str = str(datetime.now(Vietnam_time))
# datetime_str = datetime_str.split('.')[0]

# config = configparser.ConfigParser()
# # Read the path_config.ini file
# path_config = '/home/testit/SRC/SRC/src/KIOXIA/Fullcombinelot/TSV_Parsing/path_config.ini'
# config.read(path_config)
# kiomagd_path = config['Paths']['path_kiomagd']
# Combine_lot_result_file = config['Paths']['asc_processed']
# Separate_Sublot = config['Paths']['Separate_Sublot']
# kiomagd_C1_S2_Done = config['Paths']['C1_S2_Done']
# kiomagd_DAT_Done = config['Paths']['DAT_Done']
# kiomagd_TSV_Summary = config["Paths"]['TSV_Summary']

# server = config['Database']['server']
# password = config['Database']['password']

# list_file_C1_S2_DAT = []

# def list_folders(directory_path):
#     folders = []
#     for item in os.listdir(directory_path):
#         if os.path.isdir(os.path.join(directory_path, item)):
#             folders.append(os.path.join(directory_path,item))
#     return folders

# def get_test_station(station: str):
#     all_station = {
#         "T1L1": {"name": "TEST1", "code": "801"},
#         "TH1": {"name": "TEST2", "code": "805"},
#         "TL1": {"name": "TEST3", "code": "807"},
#         "TL2": {"name": "TEST4", "code": "850"},
#         "TH2": {"name": "TEST5", "code": "852"},
#         "TH3": {"name": "TEST6", "code": "872"}
#     }
#     if station in all_station:
#         return all_station[station]['code']
#     return ""

# def full_parsing(mypath: str):
#     bins = {}
#     totalBins = {}
#     tBoard = []
#     dut = []
#     keyno = []
#     linBin = []
#     catData = []
#     SWBin = {}
#     sBin = {}

#     def normal_round(num, ndigits=0):
#         if ndigits == 0:
#             return int(num + 0.5)
#         else:
#             digit_value = 10 ** ndigits
#             return int(num * digit_value + 0.5) / digit_value
        
#     def parseAllFiles(f,file,tBoard,dut,keyno,linBin,catData):
#         alltxt = ""
#         testFlag = ""
#         recipe = ""
#         rtFlag = ""
#         lines = f.readlines()
#         rtRule = {"A":"2,3,4,5,6,7,8", #Retest Rule
#             "B":"3,4,5,6,7,8",
#             "C":"4,5,6,7,8",
#             "D":"5,6,7,8",
#             "E":"5,6,8",
#             "F":"5,8",
#             "G":"5,8",
#             "H":"3,5,6,7,8",
#             "I":"5,6,8",
#             "J":"5,7,8",
#             "K":"5,6,8",
#             "L":"4,5,8"}
#         try:
#             testFlag = file.split("_")[3].strip()
#             recipe = file.split("_")[4].strip()
            
#             for line in lines:
#                 alltxt +=line
#                 if "STN" not in line:
#                     curBin = line.split(",")[3].strip()
#                     if curBin not in bins:
#                         if testFlag != "INI":
#                             rtFlag = recipe[0] #Retest Bin Rule
#                             if curBin != "001":
#                                 curBin_temp = curBin[-1]
#                                 if curBin_temp in rtRule[rtFlag]:
#                                     bins.update({curBin: 1}) #add bin if bin rule pass (mean all retest Bin is good if bin rule pass)
#                                 else:
#                                     bins.update({curBin: 1})
#                             else:
#                                 bins.update({curBin: 1}) #add bin if bin is 001
#                         else:
#                             bins.update({curBin: 1}) #add bin if bin is from INI file
#                     else:
#                         tmpBin = bins[curBin] + 1 
#                         bins.update({curBin: tmpBin}) #add bin + 1 if bin already exist
#                     tempCat = line.split(",")[4].strip()[::-1]
#                     tBoard.append(line.split(",")[0].strip())
#                     dut.append(line.split(",")[1].strip())
#                     keyno.append(line.split(",")[2].strip())
#                     linBin.append(line.split(",")[3].strip())
#                     catData.append(tempCat)
#                     swcnt = 1
#                     tmpVal = ""
#                     swPos = ""
#                     for val in tempCat:
#                         if val != "0":
#                             tmpVal = val
#                             break
#                         swcnt+=1 #Count string index for value placement
#                     if tmpVal == "1": #Set value rule (8=1000; 4=0100; 2=0010; 1=0001)
#                         swPos = "SWCAT_"+ str((swcnt*4) - 3) + "_issue"
#                     elif tmpVal == "2":
#                         swPos = "SWCAT_"+str((swcnt*4) - 2) + "_issue"
#                     elif tmpVal == "4":
#                         swPos = "SWCAT_"+str((swcnt*4) - 1) + "_issue"
#                     elif tmpVal == "8":
#                         swPos = "SWCAT_"+str((swcnt*4)) + "_issue"
#                     else:
#                         swPos = "good" #set Sbin name to Good if bin is Sbin has no value
#                     if swPos != "good":
#                         if curBin not in sBin:
#                             sBin.update({curBin: {}})
#                         if swPos not in sBin[curBin]:
#                             sBin[curBin].update({swPos: 1})
#                         else:
#                             tmpSbin = sBin[curBin][swPos] + 1
#                             sBin[curBin].update({swPos: tmpSbin})
#                     else:
#                         if curBin not in sBin:
#                             sBin.update({curBin: {swPos: 1}})
#                         else:
#                             tmpSbin = sBin[curBin][swPos] + 1
#                             sBin[curBin].update({swPos: tmpSbin})

#             for val in bins.keys(): #Set totalBins values by getting sum of all bins
#                 if val not in totalBins:
#                     totalBins[val] = bins.get(val,0)
#                 else:
#                     for val2 in totalBins.keys():
#                         if val == val2:
#                             temp = totalBins.get(val,0) + bins.get(val,0)
#                             totalBins.update({val:temp})
#             for val in sBin.keys(): #Set total Sbin by getting sum of all Sbins
#                 if val not in SWBin:
#                     if len(sBin) > 0:
#                         SWBin[val] = sBin.get(val,0)
#                 else:
#                     for val2 in SWBin.keys():
#                         if val in SWBin:
#                             if val == val2:
#                                 for val3 in sBin[val].keys():
#                                     for val4 in SWBin[val].keys():
#                                         if val4 == val3:
#                                             temp = SWBin[val].get(val3,0) + sBin[val].get(val3,0)
#                                             SWBin[val].update({val3 : temp})
#                                         else:
#                                             SWBin[val].update(sBin[val])
#                                             break
#                         else:
#                             SWBin.update({val:sBin[val]})
#                             break

#         except:
#             print("Cannot parse: "+str(fullpath))
#         return alltxt
#     #change pyodbc driver depending on Linux
#     data_base_os = '/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1'
#     cnxn = pyodbc.connect(f"DRIVER={data_base_os};UID=cimitar2;PWD={password};Database=MCSDB;Server={server};TrustServerCertificate=yes;")
    
#     #change pyodbc driver depending on OS
#     # databx_linux = 'ODBC Driver 17 for SQL Server'
#     # cnxn = pyodbc.connect(f"DRIVER={databx_linux};SERVER={server};DATABASE=TSV;UID=cimitar2;PWD={password}")
#     cursor = cnxn.cursor()
#     #change source path upon deployment
#     # mypath = "/kiomagd/Combine_lot_result_file/output"
#     # mypath = "D:/Combine_lot_result_file/output/test/Convert_asc/parsed/Separate_Sublot"
#     paths = glob.glob(os.path.join(mypath, '*.asc'))
#     # paths = sorted(Path(mypath).iterdir(), key=os.path.getmtime)
#     hBin = ""
#     totalSbin = ""
#     sumtxt = ""
#     for fullpath in paths: #iterate each files in the folder
#         totalOutQty = 0
#         otherQty = 0
#         lot_name = ""
#         dcc = ""
#         file_temp = ntpath.basename(fullpath)
#         end_time = str(file_temp).split('_')[5]
#         time_get_file = end_time[:-2]
#         main_tracecode = str(file_temp).split('_')[0]
#         qty_sublot = file_temp.split("_")[-2]
#         testtime = str(file_temp).split('_')[3]
#         testcode = file_temp.split("_")[1]
#         f = open(fullpath,"r")
#         if "_processed.asc" in file_temp:
#             sumtxt += "\n"+parseAllFiles(f,file_temp,tBoard,dut,keyno,linBin,catData)
#             tester = file_temp.split("_")[2].strip()
#             cmd = ("TSV.dbo.USP_Summary_GetLotFromKeyno'"+str(main_tracecode)+"'") #Get lot name using trace code from filename
#             cursor.execute(cmd)
#             lotdata = cursor.fetchone()
#             if lotdata:
#                 lot_name = str(lotdata[0]).strip()
#                 if '.' in lot_name:
#                     lot_name = lot_name.split('.')[0]
#                 dcc = str(lotdata[1]).strip()
#                 opr = str(lotdata[2]).strip()
            
#             totalbinperfile = sum(bins.values())
#             for binno in bins.keys():
#                 qty = int(bins.get(binno,0))
#                 if binno == "001":
#                     bin1Qty = int(bins.get(binno,0))
#                     hBin += binno+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
#                 else:
#                     hBin += binno+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
#             for binno in sBin.keys():
#                 for name in sBin[binno].keys():
#                     qty = int(sBin[binno].get(name,0))
#                     if binno == "001":
#                         totalSbin += "0,"+binno+","+name+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
#                     else:
#                         sbinNo = name.split("_")[1]
#                         totalSbin += sbinNo+","+binno+","+name+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
#             tBoard.clear()
#             dut.clear()
#             keyno.clear()
#             bins.clear()
#             catData.clear()
#             sBin.clear()
#             totalSbin = ""
#             bins.clear()
#             sBin.clear()
#             f.close()

#         totalOutQty += totalBins.get("001",0)
#         otherQty += sum(totalBins.values())
#         #__________________Start generating C1S2 and TSV Summary file__________________
#         header = ['STN','DUT','Assemblylot#','BIN','CAT']
#         all_lines = []

#         lines = sumtxt.splitlines() #sumtxt contain all content of full combine lot
#         for line in lines:
#             if "STN,DUT,Assemblylot#,BIN,CAT" not in line and line != "":
#                 all_lines.append(line.strip().split(',')[0:-1])
#         df_test = pd.DataFrame(columns=header)
#         for i,value in enumerate(all_lines):
#             df_test.loc[i] = value
#         trace_code_list = df_test['Assemblylot#'].unique()
#         tempDf = pd.DataFrame()
#         out_df = pd.DataFrame()

#         #Get first line from C1S2 MAGNUM files
#         def get_firstLine_S2_C1(file, list_file_C1_S2_DAT):
#             list_file_C1_S2_DAT.append(file)
#             file_path = source_C1S2 + "/" + file
#             with open(file_path,'r') as read_file:
#                 first_line = read_file.readline()
#             return first_line
#         source_C1S2 = kiomagd_path
#         # source_C1S2 = r"C:\Users\700445\Downloads\TA0017\summary\input"
#         allFile_C1S2 = os.listdir(source_C1S2)
#         tracecode_S2 = "S2_1_" + main_tracecode
#         tracecode_C1 = "C1_1_" + main_tracecode
#         test_station = ''
#         for file in allFile_C1S2: #firstLine_tracecode
#             #Get S2 file initial test
#             # if tracecode_S2 in str(file) and start_time in str(file):
#             #     firstLine_S2_start_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
#             #Get S2 file retest
#             if tracecode_S2 in str(file) and time_get_file in str(file):
#                 list_file_C1_S2_DAT.append(file)        
#                 file_S2_end_time = source_C1S2 + "/" + file
#                 with open(file_S2_end_time,'r') as read_file:
#                     firstLine_S2_end_time = read_file.readline()
#                     endLine_S2_end_time = read_file.readlines()[-1]
#                     line_no_S2 = int(endLine_S2_end_time.split(',')[0])
#             #Get C1 file initial test 
#             # if tracecode_C1 in str(file) and start_time in str(file):
#             #     firstLine_C1_start_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
#             #     test_station = firstLine_C1_start_time.split(',')[12]
#             #Get C1 file 
#             elif tracecode_C1 in str(file) and time_get_file in str(file):
#                 firstLine_C1_end_time = get_firstLine_S2_C1(file, list_file_C1_S2_DAT)
#                 # test_station = firstLine_C1_end_time.split(',')[12]
#             #Get code station     
#             # if test_station:
#             #     code_station = get_test_station(test_station)

#         # Loop each sub tracecode
#         for trace_code in trace_code_list:
#             # print(trace_code_list)
#             print(f"[{datetime.now(Vietnam_time).strftime("%Y-%m-%d %H:%M:%S")}] {trace_code}")
#             dut_condition = df_test['Assemblylot#'] == trace_code
#             sub_tracecode = trace_code[:6]
#             tempDf = df_test[dut_condition]
#             query_sql = f"""SELECT DISTINCT WPLOT#, WPDCC FROM OPENQUERY([DATA400], 'SELECT * FROM EMLIB.ECSRWP04 
#                     WHERE WMISC1 = ''{sub_tracecode}'' AND WSTS1 <> ''CLOSE'' AND WSUB# = 0 ') """
#             cursor.execute(query_sql)
#             sql_data = cursor.fetchone()
#             lot_no = sql_data[0]
#             dcc = sql_data[1]
#             # print(tempDf)
#             # dut_list = tempDf['DUT'].unique()
#             # len_dut_list = len(dut_list)
#             # Get SWBIN write out to C1 file
#             CAT_tempDf = tempDf['CAT']
#             swPos_list = []
#             for cat_line in CAT_tempDf:
#                 tempCat = cat_line.strip()[::-1]
#                 swcnt = 1
#                 tempval = ""
#                 swPos = ""
#                 for val in tempCat:
#                     if val != '0':
#                         tempval = val
#                         break
#                     swcnt += 1
#                 if tempval == '1':
#                     swPos = str((swcnt * 4) - 3)
#                 elif tempval == '2':
#                     swPos = str((swcnt * 4) - 2)
#                 elif tempval == '4':
#                     swPos = str((swcnt * 4) - 1)
#                 elif tempval == '8':
#                     swPos = str(swcnt * 4)
#                 if len(swPos) != 0:
#                     swPos_list.append(swPos)
#             content_C1 = '0' *200
#             content_C1_list = []
#             content_C1_list = list(content_C1)
#             if len(swPos_list) != 0:
#                 for i in swPos_list:
#                     content_C1_list[int(i)-1] = str(int(content_C1_list[int(i)-1]) + 1)
#             # Yield good of sub tracecode
#             count_BIN = tempDf['BIN']
#             Qty_OUT = 0
#             Qty_IN = 0
#             for i in count_BIN:
#                 Qty_OUT += 1
#                 if i == '001':
#                     Qty_IN += 1
            
#             content_C1 = ','.join(content_C1_list)
            
#             # Write sub tracecode for C1
#             output_S2_C1_destination = kiomagd_path

#             def get_content_C1_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1):
#                 get_first_line_1 = ",".join(firstLine.split(",")[10:18])
#                 get_first_line_2 = ",".join(firstLine.split(",")[21:27])
#                 get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(round((Qty_IN/Qty_OUT)*100,2)) + "," + get_first_line_2
#                 firstLine_C1 = f"C1,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:"
#                 all_content = firstLine_C1 + content_C1 + ";"
#                 return all_content
            
#             def generate_C1S2_file(file, all_content):
#                 with open(file, 'w') as output:
#                     output.write(all_content)

#             if testtime == 'INI' or testtime == 'RT1':
#             #     if 'firstLine_C1_start_time' not in locals():
#             #         print("Cannot find C1 file")
#             #         break 
#             #     allContent_C1 = get_content_C1_file(firstLine_C1_start_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
#             #     file_name_C1 = output_S2_C1_destination +  f"/C1_1_{sub_tracecode}_{tester}_{start_time}.txt"
#             #     generate_C1S2_file(file_name_C1, allContent_C1)
#             #     print(f"File C1 was generate successfully ! -> {file_name_C1}")
            
#             # elif testtime == 'RT1':
#                 if 'firstLine_C1_end_time' not in locals():
#                     print("Cannot find C1 file")
#                     break
#                 allContent_C1 = get_content_C1_file(firstLine_C1_end_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, content_C1)
#                 file_name_C1 = output_S2_C1_destination + f"/C1_1_{sub_tracecode}_{tester}_{end_time}.txt"
#                 generate_C1S2_file(file_name_C1, allContent_C1)
#                 print(f"File C1 was generate successfully ! -> {file_name_C1}")

#             bin1 = tempDf[tempDf['BIN'] == "001"]
#             bin2 = tempDf[tempDf['BIN'] == "002"]
#             bin3 = tempDf[tempDf['BIN'] == "003"]
        
#             # Create a pivot table to count occurrences of each BIN for each DUT
#             pivot_table = tempDf.pivot_table(index='DUT', columns='BIN', values='Assemblylot#', aggfunc='count', fill_value=0)
#             # Add all BIN columns (001 to 008) and set count to 0 if not present
#             all_bins = [f"{i:03d}" for i in range(1, 9)]
#             for bin_col in all_bins:
#                 if bin_col not in pivot_table.columns:
#                     pivot_table[bin_col] = 0
#             # Add a "TotalBin" column that equals the count of all bins
#             pivot_table['TotalBin'] = pivot_table.sum(axis=1)
#             # Calculate Yield bin 001 relative to the total
#             pivot_table['Yield'] = round((pivot_table['001'] / pivot_table['TotalBin']) * 100,2)
#             # Add good bin columns
#             pivot_table['GoodBin'] = pivot_table['001']
#             # Reorder the columns
#             pivot_table = pivot_table[['TotalBin','GoodBin','Yield','001','002','003','004','005','006','007','008']]
#             str_test = pivot_table.to_string()
#             # print(str_test)

#             all_lines = str_test.splitlines()
#             out_data = ""
#             for line in all_lines:
#                 if "BIN" in line or "DUT" in line:
#                     continue
#                 line_list = " ".join(line.split()).split(" ")
#                 line_list[0] = str(int(line_list[0]))
#                 out_data += ",".join(line_list) + ";\n"
            
#             # print(out_data)

#             # Auto fill line from 1 to 256 ot 512 if it missing
#             tracecode_lines = out_data.strip().splitlines()
#             last_sub_tracecode = ""
#             start_no = [int(line.split(',')[0]) for line in tracecode_lines if line]
#             cur_no = 1
#             for line in tracecode_lines:
#                 number = int(line.split(',')[0])
#                 while cur_no < number:
#                     last_sub_tracecode += f"{cur_no},0,0,0.00,0,0,0,0,0,0,0,0;\n"
#                     cur_no += 1
#                 last_sub_tracecode += f"{line}\n"
#                 cur_no += 1
#             while cur_no > start_no[-1]:
#                 if cur_no == line_no_S2 + 1:
#                     break
#                 last_sub_tracecode += f'{cur_no},0,0,0.00,0,0,0,0,0,0,0,0;\n' 
#                 cur_no += 1

#             out_data = last_sub_tracecode

#             # Write sub tracecode for S2
#             def get_content_S2_file(firstLine, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data):
#                 get_first_line_1 = ",".join(firstLine.split(",")[10:18])
#                 get_first_line_2 = ",".join(firstLine.split(",")[21:27])
#                 get_first_line = get_first_line_1 + "," + str(Qty_OUT) + "," + str(Qty_IN) + "," + str(round((Qty_IN/Qty_OUT)*100,2)) + "," + get_first_line_2
#                 firstLine_S2 = f"S2,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_tracecode}_0{qty_sublot},,{get_first_line},0,:\n"
#                 all_content = firstLine_S2 + out_data
#                 return all_content
             
#             # if testtime == 'INI':
#             #     if 'firstLine_S2_start_time' not in locals():
#             #         print("Cannot find S2 file")
#             #         break
#             #     allContent_S2 = get_content_S2_file(firstLine_S2_start_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data)
#             #     file_name_S2 = output_S2_C1_destination + f"/S2_1_{sub_tracecode}_{tester}_{start_time}.txt"
#             #     generate_C1S2_file(file_name_S2, allContent_S2)
#             #     print(f"File S2 was generate successfully ! -> {file_name_S2}")

#             if testtime == 'INI' or testtime == 'RT1':
#                 if 'firstLine_S2_end_time' not in locals():
#                     print("Cannot find S2 file")
#                     break
#                 allContent_S2 = get_content_S2_file(firstLine_S2_end_time, sub_tracecode, lot_no, Qty_OUT, Qty_IN, main_tracecode, qty_sublot, out_data)
#                 file_name_S2 = output_S2_C1_destination + f"/S2_1_{sub_tracecode}_{tester}_{end_time}.txt"
#                 generate_C1S2_file(file_name_S2, allContent_S2)
#                 print(f"File S2 was generate successfully ! -> {file_name_S2}")

#             #Generate TSV Summary DAT file
#             sourceDAT = kiomagd_path
#             # sourceDAT = r"C:\Users\700445\Downloads\TA0017\summary\input"
#             all_DAT_files = glob.glob(os.path.join(sourceDAT, '*.DAT'))
#             for file in all_DAT_files:
#                 # if lot_name in str(file) and main_tracecode in str(file) and start_time in str(file) and testcode in str(file) and testtime == 'INI':
#                 # if "TA0041" in str(file) and main_tracecode in str(file) and testcode in str(file) and (testtime == 'INI' or testtime == 'RT1') and (start_time in str(file) or end_time in str(file)):
#                 filename = os.path.basename(file)
#                 if lot_name in str(filename) and main_tracecode in str(filename) and testcode in str(filename) and (testtime == 'INI' or testtime == 'RT1') and time_get_file in str(filename):    
#                     list_file_C1_S2_DAT.append(filename)
#                     #Read DAT file INI
#                     with open(file, 'r') as input_dat:
#                         lines = input_dat.readlines()  
#                         for line in lines:
#                             if "LOT ID" in line:
#                                 LOT_ID = line.strip().split(':')[1]
#                             if "KEY NO" in line:
#                                 KEY_NO = line.strip().split(':')[1]
#                             if "DEVICE" in line:
#                                 DEVICE_NAME = line.strip().split(':')[1]
#                                 DEVICE_NAME =  DEVICE_NAME.strip()
#                             if "CUSTOM LOT NO" in line:
#                                 CUSTOM_LOT_NO = line.strip().split(':')[1]
#                             if "RECIPE NAME" in line:
#                                 RECIPE_NAME = line.strip().split(':')[1]
#                             if "OPER CODE" in line:
#                                 OPER_CODE = line.strip().split(':')[1]
#                                 OPER_CODE = OPER_CODE.strip()
#                             if "TEST STEP" in line:
#                                 TEST_STEP = line.strip().split(':')[1]
#                             if "TEST CODE" in line:
#                                 if "RETEST CODE" in line:
#                                     RETEST_CODE = line.strip().split(':')[1]
#                                     RETEST_CODE = RETEST_CODE.strip()
#                                 else:
#                                     TEST_CODE = line.strip().split(':')[1]
#                                     TEST_CODE = TEST_CODE.strip()
#                             if "OPERATOR ID" in line:
#                                 OPERATOR_ID = line.strip().split(':')[1]
#                             if "BOARD ID" in line:
#                                 BOARD_ID = line.strip().split(':')[1]
#                             if "TEMPERATURE" in line:
#                                 TEMPERATURE = line.strip().split(':')[1]
#                             if "HANDLER NO" in line:
#                                 HANDLER_NO = line.strip().split(':')[1]
#                             if "PROGRAM NAME" in line:
#                                 PROGRAM_NAME = line.strip().split(':')[1]
#                                 PROGRAM_NAME = PROGRAM_NAME.strip()
#                             if "HANDLER PARA FILE" in line:
#                                 HANDLER_PARA_FILE = line.strip().split(':')[1]
#                             if "START TIME" in line:
#                                 STARTTIME = line.strip().split(':')[1:]
#                             if "END TIME" in line:
#                                 ENDTIME = line.strip().split(':')[1:]
                
#             hBin_list = hBin.split("/")[0:-1]
#             hBin_list = [str(int(x.split(",")[0])) + "," + ",".join(x.split(",")[1:]) for x in hBin_list]
#             bin_split = [int(x.split(",")[0]) for x in hBin_list]
#             for i in range(1,9):
#                 if i not in bin_split:
#                     if i != 1:
#                         hBin_list.append(f"{i},0,0,0.00000")
#                     else:
#                         hBin_list.append(f"{i},1,0,0.00000")
#             hBin_list = sorted(hBin_list)
#             hBin = "/".join(hBin_list) + "/"
            
#             bin_details = "\n".join(allContent_S2.splitlines()[1:])
            
#             CUSTOM_LOT_NO = lot_no
#             LOT_DCC = dcc
#             if LOT_DCC == "":
#                 LOT_ID = CUSTOM_LOT_NO
#             else:
#                 LOT_ID = f"{CUSTOM_LOT_NO}({LOT_DCC})"
#             dat_file_content = summary_generate_kioxia.generate_summary(
#                 lot_id=LOT_ID, keyno=sub_tracecode, device_name=DEVICE_NAME, customer_lot_no=CUSTOM_LOT_NO,
#                 recipe_name=RECIPE_NAME, oper_code=OPER_CODE, test_step=TEST_STEP, test_code=TEST_CODE,
#                 retest_code=RETEST_CODE, tester_name=tester, handler_no=HANDLER_NO, board_id=BOARD_ID,
#                 temperature=TEMPERATURE, program_name=PROGRAM_NAME, handler_para_file=HANDLER_PARA_FILE,
#                 lot_quantity=otherQty,operator_id=OPERATOR_ID, STARTTIME=STARTTIME, ENDTIME=ENDTIME,
#                 hBin=hBin, swBin=SWBin, content_s2=bin_details
#             )
            
#             filename_dat = f"KIOXIA_{tester}_{PROGRAM_NAME}_{CUSTOM_LOT_NO}_{sub_tracecode}_{OPER_CODE}_{TEST_CODE}_{RETEST_CODE}_{end_time}.DAT"
            
#             # summary_generate_kioxia.save_summary_file(file_path=r"C:\Users\700445\Downloads\TA0017\summary\output", file_name=filename_dat, content=dat_file_content)  
#             summary_generate_kioxia.save_summary_file(file_path = kiomagd_TSV_Summary, file_name = filename_dat, content = dat_file_content)        
#         #__________________End of generating C1S2 and TSV Summary file__________________
#         # hBin = ""
#         # sumtxt = ""
#         print("[Binning]\n" +
#         "All Bins: "+str(totalBins)+"\n"+
#         "Out Qty: "+str(totalOutQty)+"\n"+
#         "Total Qty: "+str(otherQty)+"\n"+
#         "Soft Bin: "+str(SWBin)+"\n"+
#         "Hard Bin: "+str(hBin)+"\n"+
#         "[Binning End]\n")
#         # print("#" * len(str(file)),"\t",datetime_str,"\n")
#         totalBins.clear()
#         SWBin.clear()

# def move_C1_S2_DAT(list_file_C1_S2_DAT):
#     source_C1S2_DAT = kiomagd_path
#     destination_C1S2 = kiomagd_C1_S2_Done
#     destination_DAT = kiomagd_DAT_Done
#     # source_C1S2_DAT = r"C:\Users\700445\Downloads\TA0017\summary\input"
#     # destination_C1S2 = r"C:\Users\700445\Downloads\TA0017\summary\output"
#     # destination_DAT = r"C:\Users\700445\Downloads\TA0017\summary\output"
#     list_file_C1_S2_DAT = set(list_file_C1_S2_DAT)
#     for file in list_file_C1_S2_DAT:
#         if 'C1_1' in file or 'S2_1' in file:
#             source_path = os.path.join(source_C1S2_DAT, file)
#             destination = os.path.join(destination_C1S2, file)
#             # shutil.move(source_path, destination)
#             # print(f"Moved -> {destination}")
#         if str(file).split('.')[-1] == 'DAT':
#             source_path = os.path.join(source_C1S2_DAT, file)
#             destination = os.path.join(destination_DAT, file)
#             shutil.move(source_path, destination)
#             # print(f"Moved -> {destination}")

# if __name__ == "__main__":
#     print(datetime_str,"\t####################START PARSING####################")
#     separate_sublot.generate_asc_file(in_asc_file_path=Combine_lot_result_file,
#                        out_asc_file_path=Separate_Sublot)
#     # # list_folder = list_folders(r"C:\Users\700445\Downloads\TA0017\asc\Separate_Sublot")
#     list_folder = list_folders(Separate_Sublot)
#     if len(list_folder) == 0:
#         print(datetime_str,"\t\t\tWAITING_ASC_FILE_CONVERT", )
#     else:
#         for folder in list_folder:
#             try:
#                 full_parsing(folder)
#                 shutil.rmtree(folder)
#                 print(f"Folder deleted: {folder}\n")
#             except Exception as error_below:
#                 print(f"Cannot parsing folder: {folder}\n")
#                 print(error_below)
#                 shutil.rmtree(folder)
#             # print(f"Folder deleted: {folder}\n")
#         # move_C1_S2_DAT(list_file_C1_S2_DAT)

#     print(f"{datetime.now(Vietnam_time).strftime("%Y-%m-%d %H:%M:%S")}", "\t####################END OF PARSING####################\n") 


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Advantest Full-Combine-Lot (FCL) summary:
- Split sub-lot, build C1/S2 per sub tracecode
- Read S2/DB for metadata
- Generate KIOXIA DAT (TSV Summary)
"""
import os, glob, ntpath, shutil, configparser, pyodbc, pytz
from datetime import datetime
import pandas as pd

import separate_sublot
import summary_generate_kioxia
import stored_procedure
# from check_FCL import check_FCL_asc    # optional wait, if you want

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

def nowstr():
    return datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    
# ---------- Config ----------
def load_config(path_ini: str):
    cfg = configparser.ConfigParser()
    cfg.read(path_ini)
    P = cfg['Paths']
    D = cfg['Database']
    return {
        "KIOMAGD": P['path_kiomagd'],
        "ASC_PROCESSED": P['asc_processed'],
        "SEPARATE_SUBLOT": P['Separate_Sublot'],
        "C1S2_DONE": P['C1_S2_Done'],
        "DAT_DONE": P['DAT_Done'],
        "TSV_SUMMARY": P['TSV_Summary'],
        "DB_SERVER": D['server'],
        "DB_PASSWORD": D['password'],
        # Driver path same as your working env in both scripts
        # "ODBC_DRIVER": "/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1",
        "ODBC_DRIVER": "{ODBC Driver 17 for SQL Server}",

    }

# ---------- Helpers ----------
def list_folders(directory):
    return [
        os.path.join(directory, d)
        for d in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, d))
    ]

def normal_round(num, ndigits=0):
    if ndigits == 0:
        return int(num + 0.5)
    k = 10 ** ndigits
    return int(num * k + 0.5) / k

def parse_all_asc(file_path, bins, sBin, totalBins, SWBin):
    """
    Keep same parsing & retest bin rule as your current codes.
    - Counts hard bins into `bins` & aggregate `totalBins`
    - Parses CAT to first '1/2/4/8' bit and increments SWBin sBin dicts
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()

    filename = ntpath.basename(file_path)
    # Retest rule same as existing
    rtRule = {
        "A":"2,3,4,5,6,7,8",
        "B":"3,4,5,6,7,8",
        "C":"4,5,6,7,8",
        "D":"5,6,7,8",
        "E":"5,6,8","F":"5,8",
        "G":"5,8","H":"3,5,6,7,8",
        "I":"5,6,8",
        "J":"5,7,8",
        "K":"5,6,8",
        "L":"4,5,8"
    }
    testFlag = filename.split("_")[3].strip() if "_" in filename else ""
    recipe   = filename.split("_")[4].strip() if "_" in filename else ""

    for line in lines:
        if "STN" in line:
            continue
        parts = line.strip().split(',')
        if len(parts) < 5:  # robust
            continue
        curBin = parts[3].strip()
        # --- Hard bin counting (with retest rule) ---
        if curBin not in bins:
            if testFlag != "INI":
                rtFlag = recipe[0] if recipe else ""
                if curBin != "001" and rtFlag in rtRule:
                    if curBin[-1] in rtRule[rtFlag]:
                        bins[curBin] = 1
                    else:
                        bins[curBin] = 1  # still count if rule doesn't match? keep behavior
                else:
                    bins[curBin] = 1
            else:
                bins[curBin] = 1
        else:
            bins[curBin] += 1

        # --- Soft bin (SWCAT) ---
        tempCat = parts[4].strip()[::-1]
        swcnt, tmpVal, swPos = 1, "", ""
        for v in tempCat:
            if v != "0":
                tmpVal = v
                break
            swcnt += 1
        if   tmpVal == "1": swPos = f"SWCAT_{(swcnt*4)-3}_issue"
        elif tmpVal == "2": swPos = f"SWCAT_{(swcnt*4)-2}_issue"
        elif tmpVal == "4": swPos = f"SWCAT_{(swcnt*4)-1}_issue"
        elif tmpVal == "8": swPos = f"SWCAT_{(swcnt*4)}_issue"
        else:               swPos = "good"

        if swPos != "good":
            if curBin not in sBin:
                sBin[curBin] = {}
            sBin[curBin][swPos] = sBin[curBin].get(swPos, 0) + 1

    # Aggregate totalBins
    for b in list(bins.keys()):
        totalBins[b] = totalBins.get(b, 0) + bins[b]

    # Aggregate SWBin
    for b in sBin:
        if b not in SWBin:
            SWBin[b] = dict(sBin[b])
        else:
            for k, v in sBin[b].items():
                SWBin[b][k] = SWBin[b].get(k, 0) + v

def build_hbin_sbin_strings(bins, sBin):
    """Return hBin string ('bin,flag,qty,ratio/') and (optional) totalSbin string"""
    totalbin = sum(bins.values()) if bins else 0
    hBin = ""
    totalSbin = ""
    for binno, qty in bins.items():
        is_good = 1 if binno == "001" else 0
        ratio = normal_round((qty/totalbin)*100, 2) if totalbin else 0
        hBin += f"{binno},{is_good},{qty},{ratio}/"

    for binno in sBin:
        for name, qty in sBin[binno].items():
            if binno == "001":
                ratio = normal_round((qty/totalbin)*100, 2) if totalbin else 0
                totalSbin += f"0,{binno},{name},1,{qty},{ratio}/"
            else:
                sbinNo = name.split("_")[1] if "_" in name else name
                ratio = normal_round((qty/totalbin)*100, 2) if totalbin else 0
                totalSbin += f"{sbinNo},{binno},{name},0,{qty},{ratio}/"

    # Ensure enough 1..8 hbins (even missing == 0)
    hlist = [s for s in hBin.split("/") if s]
    hlist = [f"{int(s.split(',')[0])}," + ",".join(s.split(',')[1:]) for s in hlist]  # normalize index
    present = {int(s.split(",")[0]) for s in hlist}
    for i in range(1, 9):
        if i not in present:
            if i == 1: hlist.append(f"{i},1,0,0.00000")
            else:      hlist.append(f"{i},0,0,0.00000")
    hBin = "/".join(sorted(hlist)) + "/"
    return hBin, totalSbin

def conn_db(drv, server, password):
    # same DSN style as both scripts
    cnxn = pyodbc.connect(
        f"DRIVER={drv};UID=cimitar2;PWD={password};Database=MCSDB;Server={server};TrustServerCertificate=yes;")
    return cnxn, cnxn.cursor()

def get_lot_dcc_from_s2_firstline(first_line: str):
    # In summary_generate_main.py, lot_and_dcc is [4], parse get_lot_dcc()
    # Keep simple & robust here: prefer exactly 'lot_no,dcc' style
    try:
        half = first_line.split(':')[0]
        lot_and_dcc = half.split(',')[4]
        # emulate get_lot_dcc() logic
        if lot_and_dcc.count('.') > 1:
            lot_no = ".".join(lot_and_dcc.split('.')[:2])
            dcc = lot_and_dcc.split('.')[-1]
        elif lot_and_dcc.count('.') == 1:
            dcc = lot_and_dcc.split('.')[1]
            if len(dcc) > 2:
                lot_no, dcc = lot_and_dcc, ''
            else:
                lot_no = lot_and_dcc.split('.')[0]
        else:
            lot_no, dcc = lot_and_dcc, ''
        return lot_no, dcc
    except Exception:
        return "", ""

def pick_s2_c1_headers(kiomagd_dir, main_trace, time_key):
    """Find S2_1_* and C1_1_* that match main_trace & time_key, return first lines + S2 last line number."""
    s2_first, s2_last_no, c1_first = None, None, None
    for fname in os.listdir(kiomagd_dir):
        if f"S2_1_{main_trace}" in fname and time_key in fname:
            with open(os.path.join(kiomagd_dir, fname), 'r') as f:
                s2_first = f.readline()
                last_line = f.readlines()[-1] if f.seek(0) is None else None
            if last_line:
                try:
                    s2_last_no = int(last_line.split(',')[0])
                except Exception:
                    s2_last_no = None
        elif f"C1_1_{main_trace}" in fname and time_key in fname:
            with open(os.path.join(kiomagd_dir, fname), 'r') as f:
                c1_first = f.readline()
    return s2_first, s2_last_no, c1_first

def build_c1_content(c1_first_line, sub_tracecode, lot_no, qty_out, qty_in, main_trace, sub_idx, content_c1_payload):
    p1 = ",".join(c1_first_line.split(",")[10:18])
    p2 = ",".join(c1_first_line.split(",")[21:27])
    head = p1 + "," + str(qty_out) + "," + str(qty_in) + "," + str(round((qty_in/qty_out)*100,2)) + "," + p2
    first = f"C1,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_trace}_0{sub_idx},,{head},0,:"
    return first + content_c1_payload + ";"

def build_s2_content(s2_first_line, sub_tracecode, lot_no, qty_out, qty_in, main_trace, sub_idx, out_data):
    p1 = ",".join(s2_first_line.split(",")[10:18])
    p2 = ",".join(s2_first_line.split(",")[21:27])
    head = p1 + "," + str(qty_out) + "," + str(qty_in) + "," + str(round((qty_in/qty_out)*100,2)) + "," + p2
    first = f"S2,00,AV,{sub_tracecode},{lot_no},,YOK,,{main_trace}_0{sub_idx},,{head},0:\n"
    return first + out_data

def save_text(file, content):
    os.makedirs(os.path.dirname(file), exist_ok=True)
    with open(file, 'w') as f:
        f.write(content)

# ---------- Main per folder ----------
import os

def parse_sublot_filename_from_separate(file_name: str):
    """
    Hỗ trợ pattern của separate_sublot.py:
    <trace_code_full>_<tail>_<sub_tracecode>_<count>_processed.asc
    Ví dụ:
      TA0005_TH1_MAGNUMV-1_INI_FTH1N3221A_20240606094705_12_processed.asc
    Trả về:
      main_trace, tester, test_stage, end_time, sub_tracecode, sub_idx
    """
    base = os.path.basename(file_name)
    stem = base[:-4] if base.endswith('.asc') else base
    parts = stem.split('_')
    if len(parts) < 6:
        # pattern lạ -> trả rỗng
        return "", "", "", "", "", ""

    main_trace   = parts[0]
    sub_tracecode = parts[-3]   # ngay trước <count> và 'processed'
    sub_idx       = parts[-2]

    # 'tail' là khúc giữa (station, tester, INI/RTx, recipe, timestamp...)
    tail = parts[1:-3]

    # Xác định test_stage (INI/RT1/RT2/...)
    stage_pos = None
    for i, tok in enumerate(tail):
        if tok.startswith('INI') or tok.startswith('RT'):
            stage_pos = i
            break
    test_stage = tail[stage_pos] if stage_pos is not None else ""

    # Theo ví dụ separate_sublot.py, token ngay trước test_stage là tester (vd. MAGNUMV-1)
    tester = tail[stage_pos - 1] if stage_pos and stage_pos - 1 >= 0 else ""

    # Timestamp: token cuối cùng của tail
    end_time = tail[-1] if tail else ""

    return main_trace, tester, test_stage, end_time, sub_tracecode, sub_idx

def process_folder(folder, CFG):

    """
    Xử lý 1 thư mục sub-lot do separate_sublot.py sinh ra:
      - Parse .asc để build bins/sbins + DataFrame
      - Đọc S2/C1 (theo main_trace + time_key)
      - Pivot theo DUT + pad line tới line_no_S2 nếu có
      - Sinh C1/S2 + TSV Summary DAT cho từng sub tracecode
      - Xóa thư mục tạm sau khi xong
    """

    # Collect all processed asc files
    asc_paths = glob.glob(os.path.join(folder, "*.asc"))
    if not asc_paths:
        return

    ref_file = os.path.basename(asc_paths[0])

  
    # 2) Tách thông tin từ tên file (pattern của separate_sublot.py)
    (main_trace, test_code, tester, test_stage,
     end_time, sub_tracecode_from_name, sub_idx) = parse_sublot_filename_from_separate(ref_file)

    if not main_trace:
        print(f"[{nowstr()}] (WARN) Không parse được tên file: {ref_file}")
        # vẫn tiếp tục nhưng nhiều field có thể trống

    # Khớp S2/C1: theo TSV_Summary.py dùng end_time[:-2] để nới lỏng
    time_key = end_time[:-2] if len(end_time) > 2 else end_time
  


    # derive main meta from filename pattern: TRACE_TESTCODE_TESTER_TESTTIME_..._ENDTIME_qty_processed.asc
    # Use first file as reference
    fname = ntpath.basename(asc_paths[0])
    parts = fname.split("_")
    main_trace  = parts[0]
    test_code   = parts[1]
    tester      = parts[2]
    test_stage  = parts[3]  # INI or RT1...
    end_time    = parts[5] if len(parts) > 5 else ""
    time_key    = end_time[:-2]  # same as TSV_Summary.py logic
    sub_idx     = parts[-2]      # 01, 02, ...

    # Aggregate across all asc in this folder
    bins, sBin, totalBins, SWBin = {}, {}, {}, {}
    sumtxt_lines = []
    for p in asc_paths:
        parse_all_asc(p, bins, sBin, totalBins, SWBin)
        with open(p, 'r') as f:
            for line in f:
                if "STN" not in line and line.strip():
                    sumtxt_lines.append(",".join(line.strip().split(',')[:-1]))  # drop trailing empty col like your code

    # Build hBin/SWBin strings
    hBin, _ = build_hbin_sbin_strings(bins, sBin)

    # Read S2/C1 first lines (prefer S2 end-time file like your logic)
    s2_first, s2_last_no, c1_first = pick_s2_c1_headers(CFG["KIOMAGD"], main_trace, time_key)

    # Fallback: if cannot find S2, we can still continue but metadata will be from DB later.
    lot_no, dcc = ("", "")
    device = program = board = handler = operator = ""
    temperature = ""
    start_time = end_time  # placeholder; will format later

    # If S2 first line available, extract key fields like summary_generate_main.py
    if s2_first:
        try:
            half = s2_first.split(':')[0].split(',')
            # indices follow your existing code
            device      = half[11].strip()
            test_stn_cd = half[12].strip()
            recipe_name = half[16].strip()
            retestCode  = half[-3].strip()
            testerName  = half[13].strip()
            handler     = half[-4].strip()
            board       = half[14].strip()
            program     = half[17].strip()
            operator    = half[-6].strip()
            startTime_s = half[-8].strip()
            endTime_s   = half[-7].strip()
            lot_no, dcc = get_lot_dcc_from_s2_firstline(s2_first)
            # format time as yyyy/mm/dd HH:MM:SS
            STARTTIME = f"{startTime_s[:4]}/{startTime_s[4:6]}/{startTime_s[6:8]} {startTime_s[8:10]}:{startTime_s[10:12]}:{startTime_s[12:]}"
            ENDTIME   = f"{endTime_s[:4]}/{endTime_s[4:6]}/{endTime_s[6:8]} {endTime_s[8:10]}:{endTime_s[10:12]}:{endTime_s[12:]}"
        except Exception:
            testerName = tester
            recipe_name = ""
            retestCode = test_stage
            STARTTIME = ENDTIME = ""
    else:
        testerName = tester
        recipe_name = ""
        retestCode = test_stage
        STARTTIME = ENDTIME = ""

    # If temperature/test step missing
    test_step = ""
    oper_code = ""
    if lot_no:
        # cnxn, cursor = conn_db(CFG["ODBC_DRIVER"], CFG["DB_SERVER"], CFG["DB_PASSWORD"])
        # try:
        #     # get oper code / cond / oprn by lot_no+dcc+program
        #     sql = f"""
        #     SELECT WPOPR, WPCOND, WPOPRN FROM OPENQUERY([DATA400], '
        #       SELECT * FROM EMLIB.ECSRWP04
        #       WHERE WPLOT# = ''{lot_no}'' AND WPDCC = ''{dcc}''
        #       AND WPPGMR = ''{program}''
        #     ')
        #     """
        #     cursor.execute(sql)
        #     row = cursor.fetchone()
        #     if row:
        #         oper_code   = str(row[0]).strip()
        #         test_step   = str(row[2]).strip()
        #         temperature = str(row[1]).strip().split('+')[0] + '.00'
        # finally:
        #     cursor.close()
        #     cnxn.close()
        data_list = ['get_temperature', lot_no, dcc, program,'']        
        get_opr_temp = stored_procedure.set_unit_data(data_list)
        if get_opr_temp:
            oper_code = str(get_opr_temp.get('WPOPR'))
            test_step = str(get_opr_temp.get('WPOPRN'))
            temperature = str(get_opr_temp.get('WPCOND')).strip().split('+')[0] + '.00'
        else:
            print(f"Cannot get temperature or code station or test station. Please check \n{data_list}")

    # DataFrame for per-sub tracecode pivot like TSV_Summary.py
    header = ['STN','DUT','Assemblylot#','BIN','CAT']
    df = pd.DataFrame([ln.split(',') for ln in sumtxt_lines], columns=header)
    trace_list = df['Assemblylot#'].unique()
    for trace in trace_list:
        sub_df = df[df['Assemblylot#'] == trace]
        # C1 payload (200-char vector & mark sw positions)
        swpos = []
        for cat in sub_df['CAT']:
            temp = cat.strip()[::-1]
            swcnt = 1; tv = ""
            for v in temp:
                if v != '0': tv=v; break
                swcnt += 1
            if   tv=='1': swpos.append((swcnt*4)-3)
            elif tv=='2': swpos.append((swcnt*4)-2)
            elif tv=='4': swpos.append((swcnt*4)-1)
            elif tv=='8': swpos.append((swcnt*4))
        content_C1 = list("0"*200)
        for i in swpos:
            if 1 <= i <= 200:
                content_C1[i-1] = str(int(content_C1[i-1]) + 1)
        content_C1 = ",".join(content_C1)

        # Yields
        qty_out = len(sub_df)
        qty_in  = (sub_df['BIN'] == '001').sum()

        # Pivot per DUT
        pvt = sub_df.pivot_table(index='DUT', columns='BIN',
                                 values='Assemblylot#', aggfunc='count', fill_value=0)
        bins_all = [f"{i:03d}" for i in range(1, 9)]
        for b in bins_all:
            if b not in pvt.columns:
                pvt[b] = 0
        pvt['TotalBin'] = pvt.sum(axis=1)
        pvt['Yield'] = (pvt['001'] / pvt['TotalBin'] * 100).round(2)
        pvt['GoodBin'] = pvt['001']
        pvt = pvt[['TotalBin','GoodBin','Yield','001','002','003','004','005','006','007','008']]

        # Serialize as DUT lines
        lines = []
        for dut, row in pvt.iterrows():
            vals = [str(int(dut))] + [str(int(row['TotalBin'])),
                    str(int(row['GoodBin'])), f"{row['Yield']:.2f}"] + \
                    [str(int(row[b])) for b in bins_all]
            lines.append(",".join(vals) + ";")
        out_data = "\n".join(lines)

        # Pad 1..N lines up to s2_last_no (if known)
        if s2_last_no is not None:
            present_nums = sorted([int(l.split(',')[0]) for l in lines])
            padded = []
            cur = 1
            pres_idx = 0
            while cur <= s2_last_no:
                if pres_idx < len(present_nums) and cur == present_nums[pres_idx]:
                    # keep existing line for cur
                    padded.append([l for l in lines if l.startswith(f"{cur},")][0])
                    pres_idx += 1
                else:
                    padded.append(f"{cur},0,0,0.00,0,0,0,0,0,0,0,0;\n")
                cur += 1
            out_data = "".join(padded)

        # Build C1 & S2
        if c1_first:
            c1_content = build_c1_content(c1_first, trace, lot_no, qty_out, qty_in,
                                          main_trace, sub_idx, content_C1)
            save_text(os.path.join(CFG["KIOMAGD"], f"C1_1_{trace}_{tester}_{end_time}.txt"), c1_content)

        if s2_first:
            s2_content = build_s2_content(s2_first, trace, lot_no, qty_out, qty_in,
                                          main_trace, sub_idx, out_data)
            s2_file = os.path.join(CFG["KIOMAGD"], f"S2_1_{trace}_{tester}_{end_time}.txt")
            save_text(s2_file, s2_content)
            # bin details to feed into DAT (like TSV_Summary.py)
            bin_details = "\n".join(s2_content.splitlines()[1:])
        else:
            bin_details = out_data  # fallback
        # Prepare DAT fields
        CUSTOM_LOT_NO = lot_no
        LOT_DCC = dcc
        LOT_ID = CUSTOM_LOT_NO if not LOT_DCC else f"{CUSTOM_LOT_NO}({LOT_DCC})"
        TEST_CODE = test_code
        RETEST_CODE = retestCode
        HANDLER_PARA_FILE = ''

        dat_text = summary_generate_kioxia.generate_summary(
            lot_id=LOT_ID, keyno=trace, device_name=device, customer_lot_no=CUSTOM_LOT_NO,
            recipe_name=recipe_name, oper_code=oper_code, test_step=test_step,
            test_code=TEST_CODE, retest_code=RETEST_CODE, tester_name=testerName,
            handler_no=handler, board_id=board, temperature=temperature,
            program_name=program, handler_para_file=HANDLER_PARA_FILE,
            lot_quantity=sum(totalBins.values()), operator_id=operator,
            STARTTIME=STARTTIME, ENDTIME=ENDTIME, hBin=hBin, swBin=SWBin,
            content_s2=bin_details
        )

        dat_name = f"KIOXIA_{testerName}_{program}_{CUSTOM_LOT_NO}_{trace}_{oper_code}_{TEST_CODE}_{RETEST_CODE}_{end_time}.DAT"
        summary_generate_kioxia.save_summary_file(
            file_path=CFG["TSV_SUMMARY"], file_name=dat_name, content=dat_text
        )

    # cleanup folder after done
    shutil.rmtree(folder)
    print(f"[{nowstr()}] Done & removed folder: {folder}")

def main():
    print(f"[{nowstr()}] ### START Advantest FCL ###")

    #Load config (same path as TSV_Summary.py)
    path_ini = '/home/testit/SRC/SRC/src/KIOXIA/Fullcombinelot/TSV_Parsing/path_config.ini'
    CFG = load_config(path_ini)

    #Split sub-lot first
    separate_sublot.generate_asc_file(
        in_asc_file_path=CFG["ASC_PROCESSED"],
        out_asc_file_path=CFG["SEPARATE_SUBLOT"]
    )

    #Process every sub-lot folder
    folders = list_folders(CFG["SEPARATE_SUBLOT"])
    if not folders:
        print(f"[{nowstr()}] Waiting ASC (no sub-lot folders).")
    else:
        for fol in folders:
            try:
                process_folder(fol, CFG)
            except Exception as ex:
                print(f"[{nowstr()}] ERROR processing {fol}: {ex}")
                try:
                    shutil.rmtree(fol)
                except Exception:
                    pass
    print(f"[{nowstr()}] ### END Advantest FCL ###")

if __name__ == "__main__":
    main()
