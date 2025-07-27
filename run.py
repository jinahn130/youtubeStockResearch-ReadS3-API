import json
from youtubeStockResearchReadS3 import lambda_handler

'''
POST
event = {
    "body": json.dumps({
        "channel_id": "UCnMn36GT_H0X-w5_ckLtlgQ"
    })
}
'''

#GET request
event = {
    "queryStringParameters": {
        "channel_id": "UCnMn36GT_H0X-w5_ckLtlgQ"
    },
    "headers": {
        "x-custom-gateway-secret": "sec-value-206" 
    }
}

print(lambda_handler(event, None))
