from fastapi import FastAPI, File, UploadFile
import uvicorn


app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


# api endpoint post method to send the file data excel
@app.post("/uploadfile/")
async def uploadfile(file: UploadFile = File(...)):
    return {"filename": file.filename}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
    