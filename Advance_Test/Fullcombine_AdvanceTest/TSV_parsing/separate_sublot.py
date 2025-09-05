#!/home/testit/ETC/PythonEnv/01_Python312/bin/
import os, glob
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
        # print("")
    else:
        os.makedirs(parsedPath)
        destination_file = os.path.join(parsedPath, file_name)
        shutil.move(file, destination_file)
        print(f"Moved! -> {destination_file}")
        # print("")

def get_files(directory, file_type):
    """Get all files of a specific type in a directory"""
    os.chdir(directory)
    return glob.glob(f"*.{file_type}")

def generate_asc_file(in_asc_file_path: str, out_asc_file_path: str):
    # Usage
    directory = in_asc_file_path 
    out_directory = out_asc_file_path
    file_type = "asc"  # replace with your file type
    files = get_files(directory, file_type)
    files_content_test = []
    files_content_retest = []
    trace_code_list = []
    trace_code_list_retest = []

    header = "STN,DUT,Assemblylot#,BIN,CAT"
    file_format = "TA0005_TH1_MAGNUMV-1_INI_FTH1N3221A_20240606094705_1_processed"

    trace_code_full_list = []
    file_tail_list = {}

    for file in files:
        if '_processed.asc' in file:
            if file.split("_")[0] not in trace_code_full_list:
                trace_code_full_list.append(file.split("_")[0])
                break

    for trace_code_full in trace_code_full_list:
        for file in files: 
            if trace_code_full in file:
                test_type = ""
                file_asc_full_path = os.path.join(directory,file)
                with open(file_asc_full_path, 'r') as f:
                    file_data = f.readlines()
                    if "_INI_" in file:
                        files_content_test.extend(file_data)
                    elif "_RT1_" in file:
                        files_content_retest.extend(file_data)
                    f.close()
                file_tail_list.update({trace_code_full + "_" + file.split("_")[3]: "_".join(file.split("_")[1:-2])})
                #Move file after processed
                move_file_processed(file_asc_full_path, in_asc_file_path)  

                    
    files_content_test = [line for line in files_content_test if "STN,DUT,Assemblylot#,BIN,CAT" not in line]
    for line in files_content_test:
        if line.split(",")[2] not in trace_code_list:
            trace_code_list.append(line.split(",")[2])

    files_content_retest = [line for line in files_content_retest if "STN,DUT,Assemblylot#,BIN,CAT" not in line]
    for line in files_content_retest:
        if line.split(",")[2] not in trace_code_list_retest:
            trace_code_list_retest.append(line.split(",")[2])

    for trace_code_full in trace_code_full_list:     
        for trace_code in trace_code_list:
            output_data_test = ""
            output_data_test += header + "\n"
            if len(files_content_test) > 0:
                for line in files_content_test:
                    if trace_code in line:
                        output_data_test += line 
                if not os.path.exists(out_directory + f"/{trace_code_full}_{trace_code}_INI"):
                    os.makedirs(out_directory + f"/{trace_code_full}_{trace_code}_INI")
                with open(os.path.join(out_directory + f"/{trace_code_full}_{trace_code}_INI", 
                        f"{trace_code_full}_{file_tail_list[f'{trace_code_full}_INI']}_{trace_code}_{len(trace_code_list)}_processed.asc"), 'w') as file:
                    file.write(output_data_test)
        for trace_code in trace_code_list_retest:
            output_data_retest = ""
            output_data_retest += header + "\n"
            if len(files_content_retest) > 0:
                for line in files_content_retest:
                    if trace_code in line:
                        output_data_retest += line
                if not os.path.exists(out_directory + f"/{trace_code_full}_{trace_code}_RT1"):
                    os.makedirs(out_directory + f"/{trace_code_full}_{trace_code}_RT1")
                with open(os.path.join(out_directory + f"/{trace_code_full}_{trace_code}_RT1", 
                        f"{trace_code_full}_{file_tail_list[f'{trace_code_full}_RT1']}_{trace_code}_{len(trace_code_list)}_processed.asc"), 'w') as file:
                    file.write(output_data_retest)
                    
if __name__ == "__main__":
    # generate_asc_file(in_asc_file_path="/kiomagd/Combine_lot_result_file/",
    #                   out_asc_file_path="/kiomagd/Combine_lot_result_file/Separate_Sublot")
    
    generate_asc_file(in_asc_file_path="./737/results/FCL_asc",
                    out_asc_file_path="./737/asc_processed")
    
    # generate_asc_file(in_asc_file_path="/kiomagd/Combine_lot_result_file/output/test/Convert_asc/parsed",
    #                   out_asc_file_path="/kiomagd/Combine_lot_result_file/output/test/Convert_asc/parsed/Separate_Sublot")

