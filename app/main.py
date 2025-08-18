from fastapi import FastAPI, Query
from fastapi.exceptions import HTTPException
from fastapi.responses import Response
import uvicorn
from app.config import get_settings
from app.validators import TableFieldsAndTickers
from app.utils import (
    get_token_symbols,
    fetch_crypto_data,
    build_crypto_table,
    zip_csv_and_xlsx,
)
from typing import Annotated

SETTINGS = get_settings()


app = FastAPI()

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
        raise 
    # HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, log_level="info", reload=True)
