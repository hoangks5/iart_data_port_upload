from fastapi import FastAPI, File, UploadFile, Form
import uvicorn
import os
import pandas as pd
from s3 import s3_client
import time
from dateutil import parser
from src.check_data_type import check_data_type




TYPE_REPORTS = os.listdir("schema")
TYPE_REPORTS = [report.split(".")[0] for report in TYPE_REPORTS]
REGIONS = os.listdir('data_sample')
REGIONS = [region.lower() for region in REGIONS]



app = FastAPI(title="API Iart Data", description="API xử lý file báo cáo", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}


# api endpoint post method to send the file data excel
@app.post("/uploadfile/")
async def uploadfile(file: UploadFile = File(...), team: str = Form('AWE'), platform: str = Form('AWZ')):
    """_summary_

    Args:\n
        file (UploadFile, optional): Upload file báo cáo, format tên file: <Thị trường>-<loại báo cáo>-<tên tài khoản>\n

    Returns:\n
        file_name (str): Tên file\n
        type_report (str): Loại báo cáo\n
        account_name (str): Tên tài khoản\n
        correct (str): Số tên cột trùng khớp với schema mẫu\n
        wrong_index (list): Danh sách tên cột không trùng khớp với schema mẫu\n
        index_file (list): Danh sách tên cột trong file\n
        schema (list): Danh sách tên cột trong schema mẫu\n
    """
    region = file.filename.split("-")[0].strip().lower()
    
    if region.lower().strip() not in REGIONS:
        return { 
            "status": "error",
            "message": "Vùng không hợp lệ. Vui lòng kiểm tra lại tên file phải có thị trường thuộc 1 trong các vùng sau: " + ", ".join(REGIONS).upper() + ". Tên file phải có định dạng: <Thị trường>-<loại báo cáo>-<tên tài khoản>"
            }
    
    type_report = None
    for report in TYPE_REPORTS:
        if report.lower() in file.filename.lower():
            type_report = report
            if type_report == 'date range report':
                # kiểm tra xem file có phải là .csv hay không nếu không phải thì trả về lỗi
                if file.filename.split(".")[-1] != 'csv':
                    return {
                        "status": "error",
                        "message": "Loại báo cáo Date Range Report phải là file .csv"
                    }
            break
    if type_report is None:
        return {
            "status": "error",
            "message": "Không tìm thấy loại báo cáo phù hợp. Vui lòng kiểm tra lại tên file thuộc 1 trong các loại sau: " + ", ".join(TYPE_REPORTS)
            }
    account_name = file.filename.split("-")[-1].split(".")[:-1]
    account_name = '.'.join(account_name).strip().lower()

    
    
    
    result = {
        "region": region,
        "file_name": file.filename,
        "type_report": type_report,
        "account_name": account_name
    }
    
    with open(f"./schema/{type_report}.csv", "r", encoding="utf-8") as f:
        schemas = f.read()
        schemas = schemas.split("\n")
        schemas = [schema.split(",") for schema in schemas]
        
        schema_ = []
        for schema in schemas:
            if schema[0].lower() == region.lower().strip():
                schema_ = schema[1:]
                break
    
    if type_report.lower() == 'date range report':
        try:
            df = pd.read_csv(file.file, skiprows=7, encoding='shift-jis')
            columns = list(df.columns)
        except:
            file.file.seek(0)
            df = pd.read_csv(file.file, skiprows=7, encoding='utf-8')
            columns = list(df.columns)
    else:
        df = pd.read_excel(file.file, engine='openpyxl')
        columns = list(df.columns)

    # đếm xem có bao nhiêu phần tử trong columns năm trong schema
    wrong_index = []
    count = 0
    for col in columns:
        if col in schema_:
            count += 1
        else:
            wrong_index.append(col)
            
    result["correct_index"] = f"{count}/{len(columns)}"
    result["wrong_index"] = wrong_index
    result["index_file"] = columns
    result["schema"] = schema_
    result["wrong_data_type"] = []
    # kiểm tra xem status có success hay không
    if result['wrong_index'] == [] or result['wrong_index'] == ['No Data Available']:
        result['status'] = 'success'
        # kiểm tra data type của file
        
        result['wrong_data_type'] = check_data_type(df, region.lower().strip())
        if result['wrong_data_type'] == []:
            result['status'] = 'success'
        else:
            result['status'] = 'error'


        
        
        
        
        
        
        
        
        
        
        
        
        # chuyển sang s3 bucket
        s3_client.upload_fileobj(file.file, "iart-data", f"{team}/{platform}/{account_name}/{region}/{time.time()}-{file.filename}")
    else:
        result['status'] = 'error'
    
    return result
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
    