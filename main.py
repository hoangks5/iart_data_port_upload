from fastapi import FastAPI, File, UploadFile
import uvicorn
import os
import pandas as pd
import io
import csv
TYPE_REPORTS = os.listdir("schema")
TYPE_REPORTS = [report.split(".")[0] for report in TYPE_REPORTS]
REGIONS = os.listdir('data_sample')
REGIONS = [region.lower() for region in REGIONS]



app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


# api endpoint post method to send the file data excel
@app.post("/uploadfile/")
async def uploadfile(file: UploadFile = File(...), region: str = None):
    if region.lower().strip() not in REGIONS:
        return { 
            "status": "error",
            "message": "Vùng không hợp lệ. Vui lòng kiểm tra lại vùng miền thuộc 1 trong các vùng sau: " + ", ".join(REGIONS).upper()
            }
    
    type_report = None
    for report in TYPE_REPORTS:
        if report.lower() in file.filename.lower():
            type_report = report
            break
    if type_report is None:
        return {
            "status": "error",
            "message": "Không tìm thấy loại báo cáo phù hợp. Vui lòng kiểm tra lại tên file thuộc 1 trong các loại sau: " + ", ".join(TYPE_REPORTS)
            }
    account_name = file.filename.split("-")[1].split(".")[0].strip()
    
    result = {
        "file_name": file.filename,
        "type_report": type_report,
        "account_name": account_name
    }
    
    with open(f"./schema/{type_report}.csv", "r", encoding="utf-8") as f:
        schemas = f.read()
        schemas = schemas.split("\n")
        schemas = [schema.split(",") for schema in schemas]
        
        for schema in schemas:
            if schema[0].lower() == region.lower().strip():
                schema_ = schema[1:]
                break
    
    
    if region.lower().strip() == "jp":
        encoding_str = 'shift-jis'
    else:
        encoding_str = 'utf-8'
    
    print('------------------------------')
    print(type_report)
    print('------------------------------')
    
    if type_report == 'Date Range Report':
        return file.filename
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
            
    result["correct"] = f"{count}/{len(columns)}"
    
    
    result["wrong_index"] = wrong_index
    result["index_file"] = columns
    result["schema"] = schema_
    return result
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
    