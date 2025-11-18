from fastapi import FastAPI, Request
import uvicorn
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import yfinance as yf
import json
import os

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.post("/")
async def publish_data(request: Request):
    body = await request.json()
    ticker = body.get("ticker", "SOYB")

    info = yf.Ticker(ticker).fast_info

    message = json.dumps({"ticker": ticker, "info": info}).encode("utf-8")
    publisher.publish(topic_path, message)

    return {"status": "published"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
    )
