from fastapi import FastAPI, Request
import uvicorn
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import yfinance as yf
import json
import os
import datetime

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

FAST_INFO_FIELDS = [
    "currency",
    "dayHigh",
    "dayLow",
    "exchange",
    "fiftyDayAverage",
    "lastPrice",
    "lastVolume",
    "marketCap",
    "open",
    "previousClose",
    "quoteType",
    "regularMarketPreviousClose",
    "shares",
    "tenDayAverageVolume",
    "threeMonthAverageVolume",
    "timezone",
    "twoHundredDayAverage",
    "yearChange",
    "yearHigh",
    "yearLow"
]

def extract_fast_info(info_obj):
    out = {}
    for key in FAST_INFO_FIELDS:
        val = info_obj.get(key)
        if isinstance(val, (int, float)):
            out[key] = float(val)
        else:
            out[key] = val
    return out

@app.post("/")
async def publish_data(request: Request):
    body = await request.json()
    ticker = body.get("ticker", "SOYB")

    raw_info = yf.Ticker(ticker).fast_info
    info = extract_fast_info(raw_info)

    message = json.dumps({
        "ingest_timestamp": datetime.datetime.utcnow().isoformat(),
        "ticker": ticker,
        **info
    }).encode("utf-8")
    publisher.publish(topic_path, message)

    return {"status": "published"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
    )
