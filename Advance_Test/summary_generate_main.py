import os
import pyodbc
import summary_generate_kioxia
import glob
import shutil
import pytz
from datetime import datetime
from check_FCL import check_FCL_asc
from Fullcombine_AdvanceTest.TSV_parsing import stored_procedure

Vietnam_time = pytz.timezone('Asia/Ho_Chi_Minh')
datetime_str = str(datetime.now(Vietnam_time))
datetime_str = datetime_str.split('.')[0]

bins = {}
totalBins = {}
tBoard = []
dut = []
keyno = []
linBin = []
catData = []
SWBin = {}
sBin = {}
totalOutQty = 0
otherQty = 0

def normal_round(num, ndigits=0):
            if ndigits == 0:
                return int(num + 0.5)
            else:
                digit_value = 10 ** ndigits
                return int(num * digit_value + 0.5) / digit_value

def parseAllFiles(all_content,file,tBoard,dut,keyno,linBin,catData):
        testFlag = ""
        recipe = ""
        rtFlag = ""
        lines = str(all_content).strip().split('\n')
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

        testFlag = file.split("_")[3].strip()
        recipe = file.split("_")[4].strip()
        
        for line in lines:
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

def get_lot_dcc (lot_dcc: str):
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

def get_test_station(station: str):
    all_station = {
        "T1L1": {"name": "TEST1", "code": "801"},
        "TH1": {"name": "TEST2", "code": "805"},
        "TL1": {"name": "TEST3", "code": "807"},
        "TL2": {"name": "TEST4", "code": "850"},
        "TH3": {"name": "TEST5", "code": "852"}
# --"TH3": {"name": "TEST6", "code": "872"}
        
    }
    if station in all_station:
        return all_station[station]['name'], all_station[station]['code']
    return "", ""  # In case the station is not found

def move_file_processed(file, source_path):
        file_name = os.path.basename(file)
        timeYear = datetime_str.split('-')[0]
        timeMonth = datetime_str.split('-')[1]
        timeDate = datetime_str.split('-')[2][:2]
        parsedPath = f"{source_path}/BACKUP/{timeYear}/{timeMonth}/{timeDate}/"
        if os.path.exists(parsedPath):
            destination_file = os.path.join(parsedPath, file_name)
            shutil.move(file, destination_file)
            print(f"Moved! -> {destination_file}")
            # print("*" * len(str(file)))
        else:
            os.makedirs(parsedPath)
            destination_file = os.path.join(parsedPath, file_name)
            shutil.move(file, destination_file)
            print(f"Moved! -> {destination_file}")
            # print("*" * len(str(file)))

def summary_file(source_path):
    hBin = ""
    totalSbin = ""
    totalOutQty = 0
    otherQty = 0
    file_path = glob.glob(os.path.join(source_path, '*.asc'))
    if not file_path:
        return
    # file_path_list = os.listdir(source_path)
    filename = os.path.basename(file_path[0])
    tracecode = str(filename).split('_')[0]
    test_code = str(filename).split('_')[1]
    retest_code = str(filename).split('_')[3]
    testtime_asc = str(filename).split('_')[-2]
    all_content = ""
    for file in file_path:
        filename = os.path.basename(file)
        if tracecode in file and test_code in file and retest_code in file:
            file_name = filename
            with open(file, 'r') as input_file:
                lines = input_file.readlines()
                content = "\n".join(line.strip() for line in lines)
                all_content += '\n' + content
            input_file.close()
            #move file after processed
            move_file_processed(file, source_path)
    parseAllFiles(all_content,file_name,tBoard,dut,keyno,linBin,catData)

    totalbinperfile = sum(bins.values())
    for binno in bins.keys():
        qty = int(bins.get(binno,0))
        if binno == "001":
            # bin1Qty = int(bins.get(binno,0))
            hBin += binno+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
        else:
            hBin += binno+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
    for binno in sBin.keys():
        for name in sBin[binno].keys():
            qty = int(sBin[binno].get(name,0))
            if binno == "001":
                totalSbin += "0,"+binno+","+name+",1,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"
            else:
                sbinNo = name.split("_")[1]
                totalSbin += sbinNo+","+binno+","+name+",0,"+str(qty)+","+str(normal_round((qty/totalbinperfile) * 100,2)) + "/"

    tBoard.clear()
    dut.clear()
    keyno.clear()
    bins.clear()
    catData.clear()
    sBin.clear()
    totalSbin = ""
    bins.clear()
    totalOutQty += totalBins.get("001",0)
    otherQty += sum(totalBins.values())

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
    hBin = "/".join(hBin_list) + "/"

    S2_source_path = '/kiomagd'
    S2_file_name = 'S2_1_' + tracecode
    all_file_S2 = glob.glob(os.path.join(S2_source_path, '*.txt'))
    all_content_S2 = ""
    S2_found = False
    # cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.201.21.84,50150;DATABASE=TSV;UID=cimitar2;PWD=TFAtest1!2!")
    cnxn = pyodbc.connect("DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1;UID=cimitar2;PWD=TFAtest1!2!;Database=MCSDB;Server=10.201.21.84,50150;TrustServerCertificate=yes;")
    cursor = cnxn.cursor()
    for file in all_file_S2:
        if S2_file_name in file and testtime_asc in file:
            S2_found = True
            with open(file, 'r') as input_S2:
                first_line = input_S2.readline()
                half_first_line = first_line.split(':')[0]
                lot_and_dcc = half_first_line.split(',')[4]
                deviceName = half_first_line.split(',')[11]
                test_station_code = half_first_line.split(',')[12]
                recipeName = half_first_line.split(',')[16]
                retestCode = half_first_line.split(',')[-3]
                testerName = half_first_line.split(',')[13]
                handlerNo = half_first_line.split(',')[-4]
                boardID = half_first_line.split(',')[14]
                programName = half_first_line.split(',')[17]
                # lotQuantity = otherQty
                operatorID = half_first_line.split(',')[-6]
                startTime = half_first_line.split(',')[-7]
                endTime = half_first_line.split(',')[-8]
                lines = input_S2.readlines()
                for line in lines:
                    if 'S2' not in line:
                        all_content_S2 = "\n".join(line.strip() for line in lines)
            input_file.close()
           
            # tracecode_test = "AD0001"
            # program_test = "BK1MVK00"
            # tracecode_test = "WH1229"
            # program_test = "BK0MVKTD"
            # sql_summary_data = f"""SELECT WPLOT#, WPDCC, WPOPR, WPOPRN, WPCOND FROM OPENQUERY([DATA400], '
            #                     SELECT * FROM EMLIB.ECSRWP04
            #                     WHERE WPTDEV = ''{deviceName}''
			# 					AND WMISC1 = ''{tracecode}''
			# 					AND WPPGMR = ''{programName}''
            #                     ')"""

            # # sql_summary_data = f"""SELECT WPLOT#, WPDCC, WPOPR, WPOPRN, WPCOND 
            # #                         FROM OPENQUERY([DATA400], '
            # #                         SELECT * FROM EMLIB.ECSRWP04
            # #                         WHERE WMISC1 = ''{tracecode_test}''
            # #                         AND WPPGMR = ''{program_test}''
            # #                         ')"""

            # cursor.execute(sql_summary_data)
            # get_data_query = cursor.fetchall()
            # if len(get_data_query) == 0:
            #     print(f"Query has no data. Please check: {sql_summary_data}")
            #     break
            # elif len(get_data_query) == 1:
            #     lot_no = get_data_query[0][0]
            #     dcc = get_data_query[0][1]
            #     operationCode = str(get_data_query[0][2])
            #     testStep = get_data_query[0][3]
            #     get_temperature = str(get_data_query[0][4]).strip().split('+')[0]
            #     get_temperature += '.00'
            # else:
            #     sql_lotno_dcc = f"""SELECT DISTINCT WPLOT#, WPDCC FROM OPENQUERY([DATA400], '
            #                         SELECT * FROM EMLIB.ECSRWP04
            #                         WHERE WMISC1 = ''{tracecode}''
            #                         AND WPPGMR = ''{programName}'' 
            #                         ')"""
            #     cursor.execute(sql_lotno_dcc)
            #     get_lot_no_dcc = cursor.fetchone()
            #     lot_no = get_lot_no_dcc[0]
            #     dcc = get_lot_no_dcc[1]
            #     if dcc != "":
            #         lotno_dcc = f"{lot_no} / {dcc}"
            #     else:
            #         lotno_dcc = lot_no
            #     sql_get_test_step = f"""SELECT OPR FROM OPENQUERY ([DATA400], 'SELECT * FROM EMLIB.WIPTINVP
			# 						WHERE LOTNO = ''{lotno_dcc}''
			# 						')"""
            #     cursor.execute(sql_get_test_step)
            #     test_step = cursor.fetchone()
            #     # print(lotno_dcc)
            #     # print(f"testStep: {test_step}")
            #     testStep = test_step[0]
            # test_station, code_station = get_test_station(test_station_code)
            lot_no, dcc = get_lot_dcc(lot_and_dcc)
            # sql_opr_temp = f"""SELECT WPOPR, WPCOND, WPOPRN FROM OPENQUERY([DATA400], '
            #                 SELECT * FROM EMLIB.ECSRWP04 
            #                 WHERE WPLOT# = ''{lot_no}'' AND WPDCC = ''{dcc}''
            #                 AND WPPGMR = ''{programName}'' ')"""   
            # cursor.execute(sql_opr_temp)
            # get_opr_temp = cursor.fetchone()
            data_list = ['get_temperature', lot_no, dcc, programName,'']        # use stored procedure 
            get_opr_temp = stored_procedure.set_unit_data(data_list)
            if get_opr_temp:
                code_station = str(get_opr_temp.get('WPOPR'))
                test_station = str(get_opr_temp.get('WPOPRN'))
                get_temperature = str(get_opr_temp.get('WPCOND')).strip().split('+')[0] + '.00'
            else: 
                print(f"Cannot get temperature or code station or test station. Please check \n{data_list}")
                break
            
            #Create variable for summary file
            KEY_NO = tracecode
            DEVICE_NAME = deviceName
            CUSTOM_LOT_NO = lot_no
            LOT_DCC = dcc
            if LOT_DCC == "":
                LOT_ID = CUSTOM_LOT_NO
            else:
                LOT_ID = f"{CUSTOM_LOT_NO}({LOT_DCC})"
            RECIPE_NAME = recipeName
            OPER_CODE = code_station
            TEST_STEP = test_station
            TEST_CODE = test_code
            RETEST_CODE = retestCode
            # TESTER_NAME = testerName
            HANDLER_NO = handlerNo
            BOARD_ID = boardID
            TEMPERATURE = get_temperature
            PROGRAM_NAME = programName
            HANDLER_PARA_FILE = ''
            # Lot_Quantity = lotQuantity
            OPERATOR_ID = operatorID
            STARTTIME = f"{startTime[:4]}/{startTime[4:6]}/{startTime[6:8]} {startTime[8:10]}:{startTime[10:12]}:{startTime[12:]}"
            ENDTIME = f"{endTime[:4]}/{endTime[4:6]}/{endTime[6:8]} {endTime[8:10]}:{endTime[10:12]}:{endTime[12:]}"

            if  str(retestCode) == '0' and retest_code == 'INI':
                filename_dat = f"KIOXIA_{testerName}_{PROGRAM_NAME}_{CUSTOM_LOT_NO}_{tracecode}_{OPER_CODE}_{TEST_CODE}_{RETEST_CODE}_{startTime}.DAT"
            else:
                filename_dat = f"KIOXIA_{testerName}_{PROGRAM_NAME}_{CUSTOM_LOT_NO}_{tracecode}_{OPER_CODE}_{TEST_CODE}_{RETEST_CODE}_{endTime}.DAT"

            dat_file_content = summary_generate_kioxia.generate_summary(
                    lot_id=LOT_ID, keyno=KEY_NO, device_name=DEVICE_NAME, customer_lot_no=CUSTOM_LOT_NO,
                    recipe_name=RECIPE_NAME, oper_code=OPER_CODE, test_step=TEST_STEP, test_code=TEST_CODE,
                    retest_code=RETEST_CODE, tester_name=testerName, handler_no=HANDLER_NO, board_id=BOARD_ID,
                    temperature=TEMPERATURE, program_name=PROGRAM_NAME, handler_para_file=HANDLER_PARA_FILE,
                    lot_quantity=otherQty,operator_id=OPERATOR_ID, STARTTIME=STARTTIME, ENDTIME=ENDTIME,
                    hBin=hBin, swBin=SWBin, content_s2=all_content_S2
                )

            output_summary_path="/kiomagd"
            file_name_summary=filename_dat
            content=dat_file_content
            summary_generate_kioxia.save_summary_file(output_summary_path, file_name_summary, content)
            print(f"TSV Summary was generated successfully ! -> {output_summary_path}/{file_name_summary}")
    cursor.close()
    cnxn.close()

    if S2_found == False:
        print(f"Cannot find S2 file for {tracecode}")
    print(f"Retest code: {retestCode}")
    print(f"{tracecode}")
    print("[Binning]\n" +
    "All Bins: "+str(totalBins)+"\n"+
    "Out Qty: "+str(totalOutQty)+"\n"+
    "Total Qty: "+str(otherQty)+"\n"+
    "Soft Bin: "+str(SWBin)+"\n"+
    "Hard Bin: "+str(hBin)+"\n"+ 
    "[Binning End]\n")

def main():
    source_path = "/kiomagd/737/Full_Lot_Combine_result_file"
    print(f"{datetime_str} ____________________START____________________")
    check_FCL_asc(600)
    summary_file(source_path)
    print(f"{datetime_str} ____________________ENDED____________________\n")

if __name__ == '__main__':
    main()        

