import sys, os
sys.path.append(os.path.dirname(__file__))
from forecast import main

def lambda_handler(event, context):
    try:
        main()
        return {
            "statusCode": 200,
            "message": "Weekly forecast successfully generated."
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }
