import json
import boto3
import time 
from parser import Parse

def lambda_handler(event, context):
    print("Started Job")
    jobid = startJob() 
    print("JOBID", jobid)
    stat = get_status(jobid)
    # print("Status",stat)
    while (stat['JobStatus'] == 'IN_PROGRESS'):
        time.sleep(5)
        print("Progress")
        stat = get_status(jobid)
    pages = getJobResults(jobid)
    # print("pages",pages)
    # Print detected text

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def get_status(job_id):
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=job_id)
    return response
    
def getJobResults(jobId):
    
    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    # print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
            
    keys, values = [], []
    text_key, text_value = [], []
    pages_block = []
    for page in pages:
        pages_block.extend(page["Blocks"])
        
    parse = Parse(
        page=pages_block, get_table=True, get_kv=True, get_text=True
    )
    
    table, final_map, text = parse.process_response()
    
    print("table",table)
    print("final_map",final_map)
    print("text",text)
    
    return pages

def startJob():
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': "oms3textract",
            'Name': "Renewal_scanned_2-5.pdf"
        }
    })
    
    return response["JobId"]