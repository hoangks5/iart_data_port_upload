from fastapi import FastAPI, File, UploadFile
import uvicorn
import os

# list file in directory schema
reports = os.listdir("schema")
reports = [report.split(".")[0] for report in reports]

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


# api endpoint post method to send the file data excel
@app.post("/uploadfile/")
async def uploadfile(file: UploadFile = File(...)):
    type_report = None
    for report in reports:
        if report.lower() in file.filename.lower():
            type_report = report
            break
    if type_report is None:
        return {"message": "Không tìm thấy loại báo cáo phù hợp. Vui lòng kiểm tra lại tên file thuộc 1 trong các loại sau: " + ", ".join(reports)}
    account_name = file.filename.split("-")[1].split(".")[0].strip()
    
    return {"filename": file.filename, "type_report": type_report, "account_name": account_name}
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
    