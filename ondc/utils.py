from datetime import date
def build_frequency(frequency:str,repeat: int,day_number: int) -> str:
    freq_map = {
        "daily": "P1D",
        "weekly": "P1W",
        "monthly": "P1M",
        "quarterly": "P3M",
        "yearly": "P1Y",
    }

    if frequency not in freq_map:
        raise ValueError("Invalid frequency selected")
    
    today = date.today()
    try:
        start_date = date(today.year, today.month, day_number).isoformat()
    except ValueError:
        raise ValueError("Invalid day number for current month")

    duration = freq_map[frequency]
    return f"R{repeat}/{start_date}/{duration}"

def get_client_ip(request):
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if x_forwarded_for:
        ip = x_forwarded_for.split(",")[0]
    else:
        ip = request.META.get("REMOTE_ADDR")
    return ip


import requests
import os
from typing import Dict
import logging

OBSERVABILITY_API_URL = os.getenv("OBSERVABILITY_API_URL", "")
OBSERVABILITY_TOKEN = os.getenv("OBSERVABILITY_TOKEN", "")


def push_observability_logs(context: Dict, message: Dict, status_code: int, log_type="search"):
    logs = [
        {
            "type": log_type,
            "data": {
                "context": context,
                "message": message
            }
        },
        {
            "type": f"{log_type}_response",
            "data": {
                "context": context,
                "message": {
                    "ack": {
                        "status": "ACK" if status_code == 200 else "NACK"
                    }
                }
            }
        }
    ]
    
    try:
        response = requests.post(
            OBSERVABILITY_API_URL,
            json=logs,
            headers={
                "Authorization": f"Bearer {OBSERVABILITY_TOKEN}",
                "Content-Type": "application/json"
            },
            timeout=10
        )
        if response.status_code == 200:
            logging.info("✅ Observability logs pushed.")
        else:
            logging.error(f"❌ Observability push failed: {response.status_code}")
            logging.error(response.text)
    except Exception as e:
        logging.exception("⚠️ Error while pushing observability logs.")



def send_to_analytics(schema_type, req_body):
    API_URL = 'https://analytics-api-pre-prod.aws.ondc.org/v1/api/push-txn-logs'
    TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJpbnZlc3RtZW50LnByZXByb2QudnlhYmxlLmluQGJ1eWVyIiwiZXhwIjoxODIyOTAwMDAwLCJmcmVzaCI6ZmFsc2UsImlhdCI6MTY1OTE1MTk1NiwianRpIjoiYTIwZTNiM2YwN2I3NDVhNjhmZWM5NmYwNDE4ODZlNWMiLCJuYmYiOjE2NTkxNTE5NTYsInR5cGUiOiJhY2Nlc3MiLCJlbWFpbCI6InRlY2hAb25kYy5vcmciLCJwdXJwb3NlIjoiZGF0YXNoYXJpbmciLCJwaG9uZV9udW1iZXIiOm51bGwsInJvbGVzIjpbImFkbWluaXN0cmF0b3IiXSwiZmlyc3RfbmFtZSI6Im5ldHdvcmsiLCJsYXN0X25hbWUiOiJvYnNlcnZhYmlsaXR5In0.OwwSQilwBC2H9jeFt4yqsnUf_PXK2EJHQpuCpCqewXs'  # Use from env variable ideally

    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }

    payload = {
        "type": schema_type,
        "data": req_body
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()

        print(f"Schema {schema_type} sent successfully:", response.json())
        return response.json()

    except Exception as e:
        
        print(f"Failed to send {schema_type} schema:", str(e))
        raise


