from fastapi import FastAPI, File, UploadFile, Form
import uvicorn
import os
import pandas as pd
from s3 import s3_client
import time
from src.check_data_type import check_data_type
import re
import mysql.connector
from dotenv import load_dotenv
load_dotenv()
import requests


ACCESS_TOKEN_ONELAKE = os.getenv('ACCESS_TOKEN_ONELAKE')


def get_conn():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        return conn
    except Exception as e:
        return None

def extract_emails(string):
    # Tìm tất cả email trong chuỗi
    emails = re.findall(r'[\w\.-]+@[\w\.-]+', string)
    
    result = []
    for email in emails:
        # Tách phần tên đăng nhập (login) và tên miền
        login, domain = email.split('@')
        # phàn login chứa đầu - thì xóa
        if login[0] == '-':
            login = login[1:]
        # phần domain chứa cuối xóa đuôi .csv hoặc .xlsx
        if domain[-4:] == '.csv':
            domain = domain[:-4]
        if domain[-5:] == '.xlsx':
            domain = domain[:-5]

        
        # Ghép lại với @ và thêm vào danh sách
        result.append(login + '@' + domain)
    
    return result

def patch_file(url, file_path, file_name):
    
    token_url = url + "?resource=file"
    token_headers={
        "Authorization" : "Bearer " + ACCESS_TOKEN_ONELAKE,
        "x-ms-file-name": file_name,
    }
    
    # Code to create file in lakehouse
    response = requests.put(token_url, data={}, headers=token_headers)
    if response.status_code != 201:
        return {
            "status": "error",
            "message": "Không thể tạo file trên lakehouse",
            "response": response.text
        }
    
    
    token_url = url + "?position=0&action=append&flush=true"
    token_headers={
        "Authorization" : "Bearer " + ACCESS_TOKEN_ONELAKE,
        "x-ms-file-name": 'item.csv',
        "content-length" : "0"
    }
    

    #Code to push Data to Lakehouse 
    with open(file_path, 'rb') as file:
        file_contents = file.read()
        response = requests.patch(token_url, data=file_contents, headers=token_headers)
        
    if response.status_code != 202:
        return {
            "status": "error",
            "message": "Không thể ghi dữ liệu vào file trên lakehouse",
            "response": response.text
        }
        
    return True




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
async def uploadfile(file: UploadFile = File(...), team: str = Form('awe'), platform: str = Form('amazon')):
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
    # kiểm tra chỉ chấp nhận file có đuôi .csv hoặc .xlsx
    file.filename = file.filename.lower()
    
    if file.filename.split(".")[-1] not in ['csv', 'xlsx']:
        return {
            "status": "error",
            "message": "Chỉ chấp nhận file có đuôi .csv hoặc .xlsx"
        }
    # kiểm tra chỉ cho upload file nhỏ hơn 25MB
    if file.file.__sizeof__() > 25000000:
        return {
            "status": "error",
            "message": "File upload quá lớn. Chỉ cho phép file nhỏ hơn 25MB"
        }

    # kiêm tra xem tên file có đúng định dạng không
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
    # kiểm tra xem tên tài khoản có trong tên file không
    account_name = extract_emails(file.filename)
    if len(account_name) == 0:
        return {
            "status": "error",
            "message": "Không tìm thấy tên tài khoản trong tên file. Vui lòng kiểm tra lại tên file phải có định dạng: <Thị trường>-<loại báo cáo>-<tên tài khoản>"
        }
    elif len(account_name) == 1:
        account_name = account_name[0]
    else:
        return {
            "status": "error",
            "message": f"Tìm thấy nhiều tên tài khoản trong tên file [ {' '.join(account_name)} ]. Vui lòng kiểm tra lại tên file phải có định dạng: <Thị trường>-<loại báo cáo>-<tên tài khoản>"
        }

    
    result = {
        "region": region,
        "file_name": file.filename,
        "type_report": type_report,
        "account_name": account_name
    }
    # kiểm tra xem file có đúng schema không
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
        
        if region.lower().strip() == 'us' and 'account type' not in columns:
            # thêm cột account type vào dataframe cuối cùng
            df['account type'] = ''
            columns = list(df.columns)
            
        # loại các row có tất cả cô giá trị là NaN
        df.dropna(how='all', inplace=True)
            
            
        # đếm xem có bao nhiêu phần tử trong columns năm trong schema
        wrong_index = []
        count = 0
        for col in columns:
            if col in schema_:
                count += 1
            else:
                wrong_index.append(col)
        
        # kiểm tra xem có bao nhiêu cột thiếu
        missing_index = []
        for schema in schema_:
            if schema not in columns:
                missing_index.append(schema)

                
        result["correct_index"] = f"{count}/{len(columns)}"
        result["wrong_index"] = wrong_index
        result["missing_index"] = missing_index
        result["index_file"] = columns
        result["schema"] = schema_
        result["wrong_data_type"] = []
        result["status"] = 'error'
        
        # kiểm tra wrong_index và missing_index nếu cả 2 đều rỗng thì mới kiểm tra data type
        if ( result['wrong_index'] == [] or result['wrong_index'] == ['No Data Available'] ) and result['missing_index'] == []:
            # kiểm tra data type của file
            result['wrong_data_type'] = check_data_type(df, region.lower().strip())
            if result['wrong_data_type'] == []:
                result['status'] = 'success'
            else:
                result['status'] = 'error'
                str_wrong_data_type = ''
                for wrong_data_type in result['wrong_data_type']:
                    s = f"Cột {wrong_data_type['column']} hàng {wrong_data_type['row']} có giá trị {wrong_data_type['value']} không đúng định dạng\n"
                    str_wrong_data_type += s
                result['message'] = 'Dữ liệu trong file không đúng định dạng\n' + str_wrong_data_type
                return result
        else:
            result['status'] = 'error'
            result['message'] = f'File đang chứa {len(result["wrong_index"])} cột không trùng khớp: {", ".join(result["wrong_index"])} và thiếu {len(result["missing_index"])} cột: {", ".join(result["missing_index"])}'
            return result
            

        
        # chỉ lấy cột thứ 4, 5
        df_1 = df.iloc[:, 3:5]
        df_1.columns = ['order_id', 'sku']
        df_1['account'] = account_name
        
        # đổi thứ tự cột
        df_1 = df_1[['account', 'order_id', 'sku']]
        df_1.dropna(inplace=True)
        list_order_id = df_1['order_id'].tolist()
        list_sku = df_1['sku'].tolist()
        
        
        conn = get_conn()
        if conn is None:
            return {
                "status": "error",
                "message": "Không thể kết nối với database"
            }
        cursor = conn.cursor()
        
            
        if len(list_order_id) == 0 or len(list_sku) == 0:
            # đấy là case file không có dữ liệu
            result['status'] = 'success'
            result['message'] = 'Upload file thành công'
            
        else:
            query = """
            SELECT account FROM amz_report WHERE order_id IN ({}) AND sku IN ({})
            """.format(','.join(['%s']*len(list_order_id)), ','.join(['%s']*len(list_sku)))
            
            cursor.execute(query, list_order_id + list_sku)
            result_query = cursor.fetchall()
        

            if result_query:
                account_ = result_query[0][0]
                if account_ != account_name:
                    return {
                        "status": "error",
                        "message": f"Dữ liệu upload bị trùng lặp order id hoặc sku với tài khoản {account_}"
                    }

            insert_query = """
    INSERT IGNORE INTO amz_report (account, order_id, sku)
    VALUES (%s, %s, %s)
    """
            data = df.values.tolist()
            cursor.executemany(insert_query, data)
            conn.commit()
            cursor.close()
            result['message'] = 'Upload file thành công'
            conn.close()
    
        df.to_csv('./archive/' + file.filename, index=False, encoding='utf-8')
      
      
        status = patch_file(f"https://onelake.dfs.fabric.microsoft.com/b8fa8dd8-6181-4d0a-a756-1cc3d08f1244/de223b30-d0a3-469c-94d6-cf7137fcae33/Files/iart/{team}/{platform}/{account_name}/{region}/{time.time()} - {file.filename}", './archive/' + file.filename, f'{time.time()} - {file.filename}')
        os.remove('./archive/' + file.filename)
        if status != True:
            return status
        return result
            
    else:
        return {
            "status": "error",
            "message": "Hiện tại chỉ hỗ trợ loại báo cáo Date Range Report. Các loại báo cáo khác sẽ được hỗ trợ trong tương lai"
        }
        #df = pd.read_excel(file.file, engine='openpyxl')
        #columns = list(df.columns)

    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
    