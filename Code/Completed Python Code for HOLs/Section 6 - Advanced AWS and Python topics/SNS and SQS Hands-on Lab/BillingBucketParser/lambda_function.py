# Import necessary modules 
# CSV for handling CSV files, boto3 for AWS SDK, datetime for date operations
import csv
import boto3
from datetime import datetime
import os
# # AWS Lambda 控制台 → 配置 → 环境变量，添加 DB_HOST 和 DB_USER。
# db_host = os.environ.get("DB_HOST", "default_host")
# db_user = os.environ.get("DB_USER", "default_user")

# return {
#     "DB_HOST": db_host,
#     "DB_USER": db_user
# }
# os.listdir(dir)  os.path.exists(file_path) 检查文件是否存在
#们经常需要使用 os 来访问环境变量、处理文件、获取系统路径 get the env and de
第三种输出是lgoggering, lambda中经常用的。

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.info("Lambda function started")
logger.warning("This is a warning")
logger.error("This is an error")

# Lambda 运行时如何加载 Layer
# 1 Lambda 从 S3 下载 Layer
# 2   解压到 /opt/ 目录
# 3   Python 自动查找 /opt/python/
# 4   requests 自动可用 你可以在 Lambda 代码里打印 sys.path 看看：

# Lambda 怎么知道 Layer 在 S3？	Terraform aws_lambda_layer_version 绑定 Layer ARN
# Lambda 怎么找到 requests？	Lambda 自动加载 /opt/python/ 里的库
为什么不需要 import sys ？	/opt/python 已经在 sys.path 里
import sys
print(sys.path) #默认输出（Layer 绑定后）：
#['/var/task', '/opt/python', '/opt/python/lib/python3.9/site-packages']

def get_international_taxes(valid_product_lines, billing_bucket, csv_file):
    try:
        raise Exception("API failure: Internation Taxes API is currently unavailable.")
    except Exception as error:
# 一个 except 子句可以同时处理多个异常，这些异常将被放在一个括号里成为一个元组，例如:
# except (RuntimeError, TypeError, NameError):
# raise 唯一的一个参数指定了要被抛出的异常。它必须是一个异常的实例或者是异常的类（也就是 Exception 的子类）。
        sns = boto3.client('sns')
        sns_topic_arn = 'YOUR-SNS-TOPIC-ARN'
        message = f"Lambda function failed to reach international taxes API for 
        '{billing_bucket}' bucket and file '{csv_file}'. Error: '{error}'."
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="Lambda API Call Failure"
            )
        
        print("Published failure to sns topic.")
        raise error
# 如果没有异常发生，忽略 except 子句，try 子句执行后结束。
# 如果在执行 try 子句的过程中发生了异常，那么 try 子句余下的部分将被忽略。
# 如果异常的类型和 except 之后的名称相符，那么对应的 except 子句将被执行。最后执行 try 语句之后的代码。

# 输出 有三种，print, str.format()    f f-string（推荐）
print(f"AWS Lambda ID: {lambda_id}, Memory: {memory} MB")


# logging —— 记录日志（比 print() 更专业）
# Python 的 logging 模块用于记录日志，在 AWS Lambda 或生产环境中更推荐使用 logging，因为：

# 日志级别（DEBUG、INFO、WARNING、ERROR、CRITICAL）


def lambda_handler(event, context):
    # 打印完整事件结构（调试用）
    print(json.dumps(event, indent=2))
#json.dumps() 是 Python json 模块的核心方法，用于将 Python 对象（如字典、列表）转换为 JSON 格式的字符串。
#event: 要转换的 Python 对象（通常是字典或列表）
# indent=2: 格式化选项，表示生成的 JSON 字符串使用 2 个空格缩进，使输出更易读  translate the dicti and list to the json string .

# 方法	作用	示例
# json.loads()	将 JSON 字符串 解析为 Python 对象	data = json.loads('{"name": "Alice"}')
# json.load()	从 文件对象 读取并解析 JSON	with open('data.json') as f: data = json.load(f)
# json.dump()	将 Python 对象写入 文件对象	with open('output.json', 'w') as f: json.dump(data, f)
#     print(type(event))
    # Initialize the s3 resource using boto3
    s3 = boto3.resource('s3')
    s3_clinet = boto3.client('s3')
    # Extract the bucket name and the CSV file name from the 'event' input
    billing_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']

    billing_bucket = event['Records'][0]['s3']['bucket']['name']   
    # Define the name of the error bucket where you want to copy the erroneous CSV files
    error_bucket = 'dct-billing-errors'
    processed_bucket = 'dct-billing-processed'
    
    # Download the CSV file from S3, read the content, decode from bytes to string, and split the content by lines
    obj = s3.Object(billing_bucket, csv_file)
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
    # by client
    try:
        response = s3_clinet.get_object(Bucket=billing_bucket, Key=csv_file)
        data = response['Body'].read().decode('utf-8'.splitlines)
    #     按换行符（\n、\r\n 等）将字符串分割成行的列表。
    # 直接读取流数据：避免下载大文件到本地磁盘，节省时间和空间。
    except Exception as e:
        print(f"Error reading file: {str(e)}")
    


    # Initialize a flag (error_found) to false. We'll set this flag to true when we find an error
    error_found = False
    
    # Define valid product lines and valid currencies
    valid_product_lines = ['Bakery', 'Meat', 'Dairy']
    valid_currencies = ['USD', 'MXN', 'CAD']
    
    get_international_taxes(valid_product_lines, billing_bucket, csv_file)
    
    # Read the CSV content line by line using Python's csv reader. Ignore the header line (data[1:])
    # 本地文件操作
# with open('data.csv', 'r') as f:
#     reader = csv.reader(f)
#     for row in reader:
#         print(row)

# csv.reader 不是列表，而是一个 迭代器（iterator）。
    for row in csv.reader(data[1:], delimiter=','):
        # For each row, extract the product line, currency, bill amount, and date from the specific columns
        date = row[6]
        product_line = row[4]
        currency = row[7]
        bill_amount  = float(row[8])    
        
        # Check if the product line is valid. If not, set error flag to true and print an error message
        if product_line not in valid_product_lines:
            error_found = True
            print(f"Error in record {row[0]}: Unrecognized product line: {product_line}.")
            break
        
        # Check if the currency is valid. If not, set error flag to true and print an error message
        if currency not in valid_currencies:
            error_found = True
            print(f"Error in record {row[0]}: Unrecognized currency: {currency}.")
            break
        # Check if the bill amount is negative. If so, set error flag to true and print an error message
        
        # Check if the date is in the correct format ('%Y-%m-%d'). If not, set error flag to true and print an error message
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            error_found = True
            print(f"Error in record {row[0]}: incorrect date format: {date}.")
            break

    for row in csv.reader(data[1:],delimiter=','):
        date = row[6]
        product_line = row[4]
        currency = row[7]
        bill_amount  = float(row[8])  
#         strptime（String Parse Time）  strftime（String Format Time）
    
# #     date_str = "2025-01-29 14:30:00"
# # date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    # After checking all rows, if an error is found, copy the CSV file to the error bucket and delete it from the original bucket
    if error_found:
        copy_source = {
            'Bucket': billing_bucket,
            'Key': csv_file            
            }
        try:        
            s3.meta.client.copy(copy_source, error_bucket, csv_file)
            print(f"Moved errenous file to: {error_bucket}.") 
            s3.Object(billing_bucket, csv_file).delete()
            print("Deleted original file from bucket.")         
        # Handle any exception that may occur while moving the file, and print the error message
        except Exception as e:    
            print(f"Error while movie file: {str(e)}.") 
            
    # If no errors were found, return a success message with status code 200 and a body message indicating that no errors were found
    else:    
        copy_source = {
            'Bucket': billing_bucket,
            'Key': csv_file            
            }
        try:        
            s3.meta.client.copy(copy_source, processed_bucket, csv_file)
            print(f"Moved processed file to: {processed_bucket}.") 
            s3.Object(billing_bucket, csv_file).delete()
            print("Deleted original file from bucket.")         
        # Handle any exception that may occur while moving the file, and print the error message
        except Exception as e:    
            print(f"Error while movie file: {str(e)}.")     
数据结构 dict
 knights = {'gallahad': 'the pure', 'robin': 'the brave'}
for k, v in knights.items():
    print(k, v)
for k, v in knights.items():
    print(k,v)