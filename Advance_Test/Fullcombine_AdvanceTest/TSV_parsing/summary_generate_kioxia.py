#!/home/testit/ETC/PythonEnv/01_Python312/bin/
from datetime import datetime
import ast, os

def convert_softbin_format(soft_bin_data: list, lot_quantity: int, good_bin_no: list[str]) -> str:
    # convert_softbin_format(data, lot_quantity, ['001'])
    bin_summary = "Bin Summary:\n"
    bin_summary += "Site  P/F   SWBin  HWBin  Bin Name                            Count       Percent\n"   
    bin_summary += "----  ----  -----  -----  ----------------------------------  ----------  ----------\n"
    bin_summary += "{}\n"
    bin_summary += "------------------------------------------------------------------------------------"
    
    soft_bin = ""
    # soft_bin_data = soft_bin_str.replace(" ", "").split("SBIN:")[1]
    # soft_bin_data = ast.literal_eval(soft_bin_data)
    keys_descending = sorted(soft_bin_data.keys(), reverse=True)
    for key in keys_descending:
        sbin = soft_bin_data[key]
        sbin_keys = soft_bin_data[key].keys()
        if key in good_bin_no:
            soft_bin += f' ALL  PASS  -1  {int(key)}   Sort1                {int(sbin["good"])}   {round((float(sbin["good"])/float(lot_quantity))*100,4)}%\n'
        else:
            for sbin_key in sbin_keys:
                if sbin_key == 'good': continue
                soft_bin += f' ALL  FAIL  {str(sbin_key).split("_")[1]}     {int(key)}                     {int(sbin[sbin_key])}   {round((float(sbin[sbin_key])/float(lot_quantity))*100,4)}%\n'
    return bin_summary.format(soft_bin[0:-1])

def convert_hardbin_format(hard_bin_str: str, good_bin_no: list[str]) -> str:
    # convert_hardbin_format(data, ['1','3'])

    hard_bin_summary = "HardBin Summary:\n"
    hard_bin_summary += "Site  P/F   HWBin  HW Bin Name                         Count       Percent\n"   
    hard_bin_summary += "----  ----  -----  ----------------------------------  ----------  ----------\n"
    hard_bin_summary += "{}\n"
    hard_bin_summary += "------------------------------------------------------------------------------"

    hard_bin = ""
    hard_bin_data = hard_bin_str.split("/")
    hard_bin_data.pop()
    for value in hard_bin_data:
        value_split = value.split(",")
        if value_split[0] in good_bin_no:
            hard_bin += f' ALL  PASS  {value_split[0]}    Bin{value_split[0]}                   {value_split[-2]}      {value_split[-1]}%\n'
        else:
            hard_bin += f' ALL  FAIL  {value_split[0]}    Bin{value_split[0]}                   {value_split[-2]}      {value_split[-1]}%\n'
    return hard_bin_summary.format(hard_bin[0:-1])

def convert_datetime(datetime_str: str) -> str:
    input_format = "%Y%m%d%H%M%S"
    output_format = "%Y/%m/%d %H:%M:%S"
    input_time = datetime.strptime(datetime_str, input_format)
    return input_time.strftime(output_format)
        
def generate_summary(lot_id, keyno, device_name, customer_lot_no,
                     recipe_name, oper_code, test_step, test_code, retest_code,
                     tester_name, handler_no, board_id, temperature,
                     program_name, handler_para_file, lot_quantity,
                     operator_id, STARTTIME, ENDTIME, hBin, swBin, content_s2) -> str:
    detail_bin = ''
    hard_bin_list = ''
    bin_summary_list = ''

    STARTTIME = f"{STARTTIME[0].strip()}:{STARTTIME[1]}:{STARTTIME[2]}"
    ENDTIME = f"{ENDTIME[0].strip()}:{ENDTIME[1]}:{ENDTIME[2]}"
    hard_bin_list += convert_hardbin_format(hBin, ['1'])
    bin_summary_list += convert_softbin_format(swBin, lot_quantity, ['001'])
    detail_bin += content_s2
            
    output_data =f"""LOT ID            : {str(lot_id).strip()}
KEY NO            : {str(keyno).strip()}
DEVICE NAME       : {str(device_name).strip()}
CUSTOM LOT NO     : {str(customer_lot_no).strip()}
RECIPE NAME       : {str(recipe_name).strip()}
OPER CODE         : {str(oper_code).strip()}
TEST STEP         : {str(test_step).strip()}
TEST CODE         : {str(test_code).strip()}
RETEST CODE       : {str(retest_code).strip()}
TESTER NAME       : {str(tester_name).strip()}
HANDLER NO        : {str(handler_no).strip()}
BOARD ID          : {str(board_id).strip()}
TEMPERATURE       : {str(temperature).strip()}
PROGRAM NAME      : {str(program_name).strip()}
HANDLER PARA FILE : {str(handler_para_file).strip()}
Lot Quantity      : {str(lot_quantity).strip()}
OPERATOR ID       : {str(operator_id).strip()}
START TIME        : {str(STARTTIME).strip()}
END TIME          : {str(ENDTIME).strip()}


{bin_summary_list}


{hard_bin_list}


Site,Total,Pass,Yield,Bin1,Bin2,Bin3,Bin4,Bin5,Bin6,Bin7,Bin8
------------------------------------------------------------------------------
{detail_bin}
------------------------------------------------------------------------------"""

    return output_data

def save_summary_file(file_path: str, file_name: str, content: str):
    output_dat_path = os.path.join(file_path, file_name)
    with open(output_dat_path, 'w') as file:
        file.write(content)
        # output_TSV_dat = str(os.getcwd()) + (''.join(file_path.split('.')[1:])) + "/" + file_name
        print(f"TSV Summary was generated successfully ! -> {output_dat_path}")

if __name__ == '__main__':
    file_content = generate_summary()
    with open("./TEST_FILE.DAT", "w") as file:
    # with open("//10.201.21.12/kiomagd$/TEST_FILE.DAT", "a") as file:
        file.write(file_content)

  