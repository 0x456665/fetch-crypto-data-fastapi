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
from fastapi.middleware.cors import CORSMiddleware
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

origins = [
    "http://localhost",
    "http://localhost:5173",
]


SETTINGS = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Hello Crypto-head"}


@app.get("/api/data/download")
async def get_data(filterParams: Annotated[TableFieldsAndTickers, Query()]):
    from datetime import datetime
    
    logger.info("=" * 50)
    logger.info("API v1 /api/data/download called")
    body = filterParams
    logger.info(f"Request params: {body.model_dump()}")
    
    if body.tickers == SETTINGS.CP_SECRET:
        tickers = get_token_symbols(SETTINGS.DEFAULT_TOKENS_NEW)
        logger.info(f"Secret matched - using default tokens: {tickers}")
    elif body.tickers  == None:
        tickers = "BTC,ETH,PI"
        logger.info(f"No tickers provided - using default: {tickers}")
    else:
        tickers = body.tickers
        logger.info(f"Using provided tickers: {tickers}")
    
    try:
        # Generate timestamp for this download
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        data = await fetch_crypto_data(tickers)
        data = build_crypto_table(data, body)
        zipfile = zip_csv_and_xlsx(data, timestamp)
        logger.info("Successfully created zip file")
        
        filename = f"crypto_data_{timestamp}.zip"
        return Response(
            content=zipfile,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error in /api/data/download: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v2/data/download")
async def get_data_v2(filterParams: Annotated[V2DownloadRequest, Query()]):
    from datetime import datetime
    
    logger.info("=" * 50)
    logger.info("API v2 /api/v2/data/download called")
    body = filterParams
    secret = body.secret
    logger.info(f"Request params: {body.model_dump()}")
    logger.info(f"Secret provided: {secret is not None}")
    
    # Logic to gather tickers
    final_tickers = []
    
    # 1. Defaults from Secret
    if secret == SETTINGS.CP_SECRET:
        default_tokens = get_token_symbols(SETTINGS.DEFAULT_TOKENS_NEW)
        final_tickers.extend(default_tokens)
        logger.info(f"Secret matched - added default tokens: {default_tokens}")
    
    # 2. Persisted Tickers
    if secret:
        # If tickers provided in request, add to DB
        if body.tickers:
            # allow comma separated
            new_tickers = [t.strip() for t in body.tickers.split(",") if t.strip()]
            logger.info(f"Adding {len(new_tickers)} new tickers to DB for secret: {new_tickers}")
            for t in new_tickers:
                add_ticker(secret, t)
        
        # Fetch all persisted
        persisted = get_tickers(secret)
        logger.info(f"Retrieved {len(persisted)} persisted tickers for secret: {persisted}")
        final_tickers.extend(persisted)
    
    # 3. Fallback / No Secret case
    if not secret:
        if body.tickers:
             provided_tickers = [t.strip() for t in body.tickers.split(",") if t.strip()]
             final_tickers.extend(provided_tickers)
             logger.info(f"No secret - using provided tickers: {provided_tickers}")
        else:
             final_tickers = ["BTC", "ETH", "PI"]
             logger.info(f"No secret, no tickers - using defaults: {final_tickers}")

    # Deduplicate and clean
    # If final_tickers is empty at this point, maybe fallback? 
    # But if secret was used and had no tickers, effectively empty list.
    # Assuming user wants at least something if explicit tickers provided.
    
    unique_tickers = list(dict.fromkeys(final_tickers)) # preserve order
    if not unique_tickers:
        unique_tickers = ["BTC", "ETH", "PI"]
        logger.warning("No tickers found - using fallback defaults: BTC, ETH, PI")
        
    tickers_str = ",".join(unique_tickers)
    logger.info(f"Final unique tickers ({len(unique_tickers)}): {tickers_str}")

    try:
        # Generate timestamp for this download
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        data = await fetch_crypto_data(tickers_str)
        # Note: build_crypto_table takes 'body' which is TableFieldsAndTickers (or subclass)
        # It uses the fields to determine columns. V2DownloadRequest has them.
        data = build_crypto_table(data, body)
        zipfile = zip_csv_and_xlsx(data, timestamp)
        logger.info("Successfully created zip file for v2 endpoint")
        
        filename = f"crypto_data_v2_{timestamp}.zip"
        return Response(
            content=zipfile,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error in /api/v2/data/download: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))




if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_level="info", reload=True)
