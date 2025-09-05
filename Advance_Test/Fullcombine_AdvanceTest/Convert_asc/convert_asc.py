#!/usr/bin/python
import csv
import shutil
import glob
import os
import pytz
import time
import pyodbc
from datetime import datetime
import multiprocessing
from functools import partial

from TSV_parsing import stored_procedure

Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
datetime_str = str(datetime.now(Vietnam_time))
datetime_str = datetime_str.split('.')[0]

# def move_file_processed(file, source_path):
#     file_name = os.path.basename(file)
#     timeYear = datetime_str.split('-')[0]
#     timeMonth = datetime_str.split('-')[1]
#     timeDate = datetime_str.split('-')[2][:3]
#     timeDate = timeDate.strip()
#     parsedPath = f"{source_path}/BACKUP/{timeYear}/{timeMonth}/{timeDate}/"
#     if not os.path.exists(parsedPath):
#         os.makedirs(parsedPath)
#     destination_file = os.path.join(parsedPath, file_name)
#     if os.path.exists(destination_file):
#         os.remove(destination_file)
#     shutil.move(file, destination_file)
#     print(f"Moved! -> {destination_file}")
#     # print("")

def move_file_processed(file, source_path):
    file_name = os.path.basename(file)
    timeYear = datetime_str.split('-')[0]
    timeMonth = datetime_str.split('-')[1]
    timeDate = datetime_str.split('-')[2][:3].strip()
    
    parsedPath = os.path.join(source_path, "BACKUP", timeYear, timeMonth, timeDate)
    os.makedirs(parsedPath, exist_ok=True)
    
    destination_file = os.path.join(parsedPath, file_name)
    
    # If file exists, remove it to allow overwrite
    if os.path.exists(destination_file):
        os.remove(destination_file)
    
    shutil.move(file, destination_file)
    print(f"Moved! -> {destination_file}")


# Convert a single digit string to its corresponding number
def convertDigitToNumber(digit):
    if "30" <= digit <= "39":
        return str(int(digit) - 30)
    else:
        return digit

def hex_to_char(hex):
    character = chr(int(hex, 16))
    return character

def get_recent_files(directory): #get_recent_files(directory, time_threshold=3600)
    recent_files = []
    current_time = time.time()
    inputFileAsc = os.listdir(directory)
    for filename in  inputFileAsc:
        if 'SETUP' in filename or 'Setup' in filename:
            continue
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if filepath.split('.')[-1] == 'asc':
                # creation_time = os.path.getmtime(filepath)
                # if current_time - creation_time <= time_threshold:
                recent_files.append(filepath)
    return recent_files

def check_csv_to_start(directory, time_threshold):
    current_time = time.time()
    creation_time = os.path.getmtime(directory)
    if current_time - creation_time <= time_threshold:
        return 1
    else: return 0

def check_tracecode(cursor, tracecode):
    
    data_list = ['get_lot_dcc', '', '', '', tracecode]

    result = stored_procedure.set_unit_data(data_list)

    if result == None:
        return 1
    else:
        return 0

def check_tester_status(cursor, endtime, machine):
    sql_query = f"EXEC [ATV_Common].[dbo].[FullCombineLot_Check_Testing_Status] @DATETIME_ASC = '{endtime}', @MACHINE = '{machine}'"
    cursor.execute(sql_query)
    info_message = cursor.fetchone()  # Move to the info message result set (if any)
    return str(info_message[0])

def get_all_tracecode(all_files):
    all_tracecode = []
    for file in all_files:
        file_name = os.path.basename(file)
        tracecode = str(file_name).split('_')[0]
        if tracecode not in all_tracecode:
            all_tracecode.append(tracecode)
    return all_tracecode

def parsingData(cursor, sourcePath, inputFileCsv, outputDir):
    csv_path = r"/kiomagd/737/Combine_lot_Assembly_conversion_data"
    check_csv_file = check_csv_to_start(csv_path, 600)
    if check_csv_file == 1:
        print('There is csv file. Waiting 10s...')
        time.sleep(5)
    inputFileAsc = get_recent_files(sourcePath)
    if not inputFileAsc:
        print("-----------------------Waiting file input!--------------------\t", datetime_str)
        return
    print("-----------------------Start_converting-----------------------\t", datetime_str)
    all_tracecode = get_all_tracecode(inputFileAsc)
    existed_tracecode = []
    for tracecode in all_tracecode:
        FCL = check_tracecode(cursor, tracecode)
        if FCL!= 1:
            print(f'{tracecode} is not for Full Combine Lot')
            for file in inputFileAsc:
                if tracecode in file:
                    move_file_processed(file, sourcePath)
        else: existed_tracecode.append(tracecode)
        # if FCL == 1:    existed_tracecode.append(tracecode)
    if existed_tracecode == []:
        return
    with multiprocessing.Pool() as pool:
        process_file_partial = partial(process_file, inputFileCsv=inputFileCsv, outputDir=outputDir, sourcePath=sourcePath)
        pool.map(process_file_partial, [f for f in inputFileAsc if any(t in f for t in existed_tracecode)])

    vietnam_timezone = str(datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')))
    vietnam_time = vietnam_timezone.split('.')[0]
    print("-----------------------Converting_Ended-----------------------\t", vietnam_time)

def process_file(fileName, inputFileCsv, outputDir, sourcePath):
    try:
        content = ""
        basename_file = os.path.basename(fileName)
        tracecode = str(basename_file).split('_')[0]
        # print(f"Processing -> {fileName}")
        with open(fileName, 'r', newline='') as inputAsc:
            reader = csv.reader(inputAsc)
            for line in reader:
                firstLine = []
                allLine = []
                temp = []
                if "STN" in line:
                    firstLine.extend(line[:5])
                    firstLine = ','.join(firstLine) 
                elif len(line) > 7:
                    allLine = [line[0], line[1], line[3], line[4], line[22]]
                    temp = list(map(hex_to_char, line[5:22]))
                    stringCompare = ''.join(temp)
                    temp.clear()

                    # Find .csv_temp
                    inputFile_csvTemp = glob.glob(os.path.join(inputFileCsv, '*.csv_temp'))
                    csv_temp_list = [csvtempPath for csvtempPath in inputFile_csvTemp if tracecode in os.path.basename(csvtempPath)]

                    if len(csv_temp_list) == 1:
                        keyNo_csvTemp_found = csv_temp_list[0]
                    elif len(csv_temp_list) == 0:
                        print(f"There is no trace code list : {tracecode} \nPlease check-> {inputFileCsv}")
                        move_file_processed(fileName, sourcePath)
                        return
                    else:
                        print(f"Duplicate code list. Please check: {inputFileCsv}")
                        move_file_processed(fileName, sourcePath)
                        return

                    with open(keyNo_csvTemp_found, 'r', newline='') as inputTemp:
                        for line2 in inputTemp:
                            varFind = line2.strip().split('=')
                            if len(varFind) == 2 and stringCompare == varFind[1]:
                                temp.append(varFind[0])
                                break

                    if temp:
                        allLine.append("".join(temp) + "000.00")
                    else:
                        allLine.append("unknown      ")
                        print(allLine)
                
                if firstLine:
                    content += firstLine + '\n'
                elif allLine:
                    allLine[3], allLine[5] = allLine[5], allLine[3]
                    allLine[4], allLine[5] = allLine[5], allLine[4]
                    allLine[2], allLine[3] = allLine[3], allLine[2]
                    content += ','.join(allLine) + '\n'

        output_file_name = os.path.splitext(basename_file)[0] + "_processed.asc"
        outputFileAsc = os.path.join(outputDir, output_file_name)
        with open(outputFileAsc, 'w', newline='') as outputAsc:
            outputAsc.write(content)
        print(f"Converted -> {outputFileAsc} ")
        move_file_processed(fileName, sourcePath)
    except Exception as e:
        print(f"Error processing file {fileName}: {str(e)}")
        # move_file_processed(fileName, sourcePath)

def main():
    time.sleep(2)
    start_time = time.time()
    
    # sourcepath = "/kiomagd/Full_Lot_Combine_result_file"
    # inputFileCsv = "/kiomagd/737/FULL_TEST"
    # outputFileAsc = "/kiomagd/Combine_lot_result_file"

    sourcepath = "./737/results/FCL_asc"
    inputFileCsv = "./737/csv/temp"
    outputFileAsc = "./737/asc_processed"

    ## cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.201.21.84,50150;DATABASE=TSV;UID=cimitar2;PWD=TFAtest1!2!")
    # cnxn = pyodbc.connect("DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.3.so.2.1;UID=cimitar2;PWD=TFAtest1!2!;Database=MCSDB;Server=10.201.21.84,50150;TrustServerCertificate=yes;")
    # cursor = cnxn.cursor()

    db_link = "DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1;UID=cimitar2;PWD=TFAtest1!2!;Database=MCSDB;Server=10.201.21.84,50150;TrustServerCertificate=yes;"
    with pyodbc.connect(db_link) as cnxn:  
        with cnxn.cursor() as cursor:  
            parsingData(cursor, sourcepath, inputFileCsv, outputFileAsc)
    end_time = time.time()
    print(f"Total script execution time: {end_time - start_time:.2f} seconds\n")

if  __name__ == "__main__":
    main()

