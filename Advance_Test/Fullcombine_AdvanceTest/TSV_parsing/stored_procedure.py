import requests, json
import datetime

def request_store_procedure(sp_name: str, param_list: list[str], arg_list: list) -> requests.Response: 
    api_link = 'http://10.201.12.31:8004'
    validate_code = requests.post(f"{api_link}/Common/Data_Method/Get_Request_Validate_Code").text
    req_body = {
        "requestValidateCode": validate_code,
        "storeProcedureName": sp_name,
        "parametersList": param_list,
        "argumentList": arg_list
    }
    req_header = {'Content-Type': 'application/json'}
    data = requests.post(f"{api_link}/Common/Data_Method/DB/Call_Store_Procedure",
                        data=json.dumps(req_body), headers=req_header)
    return data

def set_unit_data(data_list: list):
    sql_getdata = request_store_procedure('ATV_Common.dbo.USP_Test_Kioxia_Fullcombinelot_Advantest', 
                    ["@CONDITION", "@LOT_NO", "@DCC", "@PROGRAM", "@TRACE_CODE"], data_list)
    result = sql_getdata.json()
    if len(result['spResult']) != 0:
        data_list_result = result['spResult'][0]['data']
        if not data_list_result:
            return None
        return result['spResult'][0]['data'][0]
    else:
        return ''

if __name__ == "__main__":
    data_list = ['get_temperature', 'J04JPY00', '04', 'BK02VKTT','']
    get_opr_temp = set_unit_data(data_list)
    print(get_opr_temp)
    if get_opr_temp:
        code_station = str(get_opr_temp.get('WPOPR'))
        test_station = str(get_opr_temp.get('WPOPRN'))
        get_temperature = str(get_opr_temp.get('WPCOND')).strip().split('+')[0] + '.00'

        print(code_station)
        print(test_station)
        print(get_temperature)
    else:
        print("rong")

    # tracecode = 'WH4037'
    # data_list = ['get_lot_dcc', '', '', '', tracecode]

    # result_exec = set_rel_unit_data(data_list)
    # print(result_exec)
    # lot_no = result_exec.get('WPLOT#')
    # dcc = result_exec.get('WPDCC')\
    
    # print(f'lot: {lot_no}')
    # print(f'dcc: {dcc}')

