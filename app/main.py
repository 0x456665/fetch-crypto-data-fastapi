from fastapi import FastAPI, Query
from fastapi.exceptions import HTTPException
from fastapi.responses import Response
import uvicorn
from app.config import get_settings
from app.validators import TableFieldsAndTickers, V2DownloadRequest
from app.db import init_db, add_ticker, get_tickers
from contextlib import asynccontextmanager
from app.utils import (
    get_token_symbols,
    fetch_crypto_data,
    build_crypto_table,
    zip_csv_and_xlsx,
)
from typing import Annotated

SETTINGS = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Hello Crypto-head"}


@app.get("/api/data/download")
async def get_data(filterParams: Annotated[TableFieldsAndTickers, Query()]):
    body = filterParams
    if body.tickers == SETTINGS.CP_SECRET:
        tickers = get_token_symbols(SETTINGS.DEFAULT_TOKENS_NEW)
    elif body.tickers  == None:
        tickers = "BTC,ETH,PI"
    else:
        tickers = body.tickers
    try:
        data = await fetch_crypto_data(tickers)
        data = build_crypto_table(data, body)
        zipfile = zip_csv_and_xlsx(data)
        return Response(
            # io.BytesIO(zipfile),
            content=zipfile,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=crypto_data.zip"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v2/data/download")
async def get_data_v2(filterParams: Annotated[V2DownloadRequest, Query()]):
    body = filterParams
    secret = body.secret
    
    # Logic to gather tickers
    final_tickers = []
    
    # 1. Defaults from Secret
    if secret == SETTINGS.CP_SECRET:
        final_tickers.extend(get_token_symbols(SETTINGS.DEFAULT_TOKENS_NEW))
    
    # 2. Persisted Tickers
    if secret:
        # If tickers provided in request, add to DB
        if body.tickers:
            # allow comma separated
            new_tickers = [t.strip() for t in body.tickers.split(",") if t.strip()]
            for t in new_tickers:
                add_ticker(secret, t)
        
        # Fetch all persisted
        persisted = get_tickers(secret)
        final_tickers.extend(persisted)
    
    # 3. Fallback / No Secret case
    if not secret:
        if body.tickers:
             final_tickers.extend([t.strip() for t in body.tickers.split(",") if t.strip()])
        else:
             final_tickers = ["BTC", "ETH", "PI"]

    # Deduplicate and clean
    # If final_tickers is empty at this point, maybe fallback? 
    # But if secret was used and had no tickers, effectively empty list.
    # Assuming user wants at least something if explicit tickers provided.
    
    unique_tickers = list(dict.fromkeys(final_tickers)) # preserve order
    if not unique_tickers:
        unique_tickers = ["BTC", "ETH", "PI"]
        
    tickers_str = ",".join(unique_tickers)

    try:
        data = await fetch_crypto_data(tickers_str)
        # Note: build_crypto_table takes 'body' which is TableFieldsAndTickers (or subclass)
        # It uses the fields to determine columns. V2DownloadRequest has them.
        data = build_crypto_table(data, body)
        zipfile = zip_csv_and_xlsx(data)
        return Response(
            content=zipfile,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=crypto_data_v2.zip"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_level="info", reload=True)
