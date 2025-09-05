#!/usr/bin/python
import os
import glob
import time
import shutil
from datetime import datetime
import pytz

Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
datetime_str = str(datetime.now(Vietnam_time))
datetime_str = datetime_str.split('.')[0]

def move_file_processed(file, source_path):
    file_name = os.path.basename(file)
    timeYear = datetime_str.split('-')[0]
    timeMonth = datetime_str.split('-')[1]
    timeDate = datetime_str.split('-')[2][:3]
    timeDate = timeDate.strip()
    parsedPath = f"{source_path}/BACKUP/{timeYear}/{timeMonth}/{timeDate}/"
    if os.path.exists(parsedPath):
        destination_file = os.path.join(parsedPath, file_name)
        shutil.move(file, destination_file)
        print(f"Moved! -> {destination_file}")
        print("*" * len(str(file)))
    else:
        os.makedirs(parsedPath)
        destination_file = os.path.join(parsedPath, file_name)
        shutil.move(file, destination_file)
        print(f"Moved! -> {destination_file}")
        print("*" * len(str(file)))

def get_recent_files(directory, time_threshold):
    recent_files = []
    current_time = time.time()
    inputFileAsc = os.listdir(directory)
    for filename in  inputFileAsc:
        if 'SETUP' in filename or 'Setup' in filename:
            continue
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if filepath.split('.')[-1] == 'csv':
                creation_time = os.path.getctime(filepath)
                if current_time - creation_time <= time_threshold:
                    recent_files.append(filepath)
    return recent_files

def convert_csv_files(inputFilecsv_Path, outputFilecsv_Path, outputCsvtemp_Path):
    findInput_csvFile = get_recent_files(inputFilecsv_Path, 3600)
    allfilePath = []
    if findInput_csvFile != []:
        for filenameLog in findInput_csvFile:
            allfilePath.append(filenameLog)
        for fileName in findInput_csvFile:
            try:
                outputFilename = fileName.split('/')[-1]
                outputCsvname = os.path.join(outputFilecsv_Path,"C_" + outputFilename)
                tracecode = outputFilename.split('.')[0]
                csvTemp_name = 'C_' + tracecode + '.csv_temp'
                outputFile_Csvtemp = os.path.join(outputCsvtemp_Path, csvTemp_name)
                print(f"Processing : {fileName}")
                with open(fileName, 'r', newline='') as inputFile, open(outputCsvname, 'w', newline='') as outputFilecsv, open(outputFile_Csvtemp, 'w', newline='') as outputCsvtemp:
                    firstLine = True
                    allLine = []

                    outputFilecsv.write("Diff Lot No,Wafer No,DS_X,DS_Y,KEYNO\n")
        
                    for line in inputFile:
                        if firstLine:
                            firstLine = False
                            continue
                        allLine = line.strip().split(',')
                        #File .csv
                        if len(allLine) == 5:
                            fx = str(int(allLine[2]) + 5)
                            fy = str(int(allLine[3]) + 5)
                            allLine[2] = str(fx)
                            allLine[3] = str(fy)
                            
                        if allLine:
                            allLine[2], allLine[3] = allLine[3], allLine[2]
                            outputFilecsv.write(f"{allLine[0]},")
                            if len(allLine[1]) < 2:
                                outputFilecsv.write(f"{'0' + allLine[1]},")
                            else:
                                outputFilecsv.write(f"{allLine[1]},")
                            outputFilecsv.write(f"{allLine[2]},{allLine[3]},{allLine[4]}\n")

                            #File .csv_temp
                            outputCsvtemp.write(f"{allLine[4]}={allLine[0]}")
                            if len(allLine[1]) < 2:
                                outputCsvtemp.write(f"{'0' + allLine[1]}")
                            else:
                                outputCsvtemp.write(f"{allLine[1]}")
                            if len(allLine[2]) < 2:
                                outputCsvtemp.write(f"00{allLine[2]}")
                            elif len(allLine[2]) < 3:
                                outputCsvtemp.write(f"0{allLine[2]}")
                            if len(allLine[3]) < 2:
                                outputCsvtemp.write(f"00{allLine[3]}\n")
                            elif len(allLine[3]) < 3:
                                outputCsvtemp.write(f"0{allLine[3]}\n")        
                    print(f"Check csv file {outputCsvname}")
                    print(f"Check csv_temp file {outputFile_Csvtemp}")
            
                #Move file after converted
                move_file_processed(fileName, inputFilecsv_Path)

            except FileExistsError:
                print("File do not existed\n")
    
            except FileNotFoundError:
                print("File cannot found!\n")
    else: 
        print("....................Waiting input csv file....................")

def main():
    print(datetime_str,"\t____________________START CONVERTING____________________")

    # inputFilecsv_Path = "/kiomagd/737/Combine_lot_Assembly_conversion_data"
    # outputFilecsv_Path = "/kiomagd/737/Combine_lot_Assembly_converted_data"
    # outputCsvtemp_Path = "/kiomagd/737/FULL_TEST"

    # inputFilecsv_Path = "/kiomagd/Combine_lot_result_file/output/test/Convert_csv"
    # outputFilecsv_Path = "/kiomagd/Combine_lot_result_file/output/test/Convert_csv/CSV_Processed"
    # outputCsvtemp_Path = "/kiomagd/Combine_lot_result_file/output/test/Convert_csv/CSV_Processed"
    
    inputFilecsv_Path = "./737/csv/input"
    outputFilecsv_Path = "./737/csv/converted"
    outputCsvtemp_Path = "./737/csv/temp"

    convert_csv_files(inputFilecsv_Path, outputFilecsv_Path, outputCsvtemp_Path)

    print(datetime_str,"\t____________________CONVERTING ENDED____________________\n")

if __name__ == '__main__':
    main()


