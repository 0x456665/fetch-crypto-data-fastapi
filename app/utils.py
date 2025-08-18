import aiohttp
import asyncio
from app.validators import TableFieldsAndTickers
import io
import pandas as pd
import zipfile
from fastapi.exceptions import HTTPException

from .config import get_settings

SETTINGS = get_settings()
api_key = SETTINGS.API_KEY

def get_amount_abbrv(price: int) -> int:
    # if price is None:
    #     return None
    # if price > 1e12:
    #     return f"{price / 1e12:.2f}T"
    # elif price > 1e9:
    #     return f"{price / 1e9:.2f}B"
    # elif price > 1e6:
    #     return f"{price / 1e6:.2f}M"
    # elif price > 1e3:
    #     return f"{price / 1e3:.2f}K"
    # return str(price)
    # 
    # 
    # NO ABBREVIATION
    return price


def get_token_symbols(tokens_new) -> list[str]:
    return [
        token.split(" (")[1].replace("(", "").replace(")", "") for token in tokens_new
    ]


async def fetch_crypto_data(symbols: str | list[str])-> dict:
    async with aiohttp.ClientSession() as session:
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": api_key}
        if isinstance(symbols, list):
            params = {"symbol": ",".join(symbols)}
        else:
            params = {"symbol": symbols}
        async with session.get(url, headers=headers, params=params) as response:
            if response.status not in [200, 201]:
               raise HTTPException(
                   status_code=response.status,
                   detail=await response.json(),
               )
        
            return await response.json()


def build_crypto_table(data, model: TableFieldsAndTickers = TableFieldsAndTickers()):
    crypto_table = {
        "Name": [],
        "Symbol": [],
    }

    field_dictionary = model.model_dump(exclude_none=True)
    for key in field_dictionary.keys():
        if field_dictionary[key] != False and key != "tickers":
            if key == "supply_percent":
                crypto_table["Supply %"] = []
            elif key == "volume_change_24h":
                crypto_table["Volume Change(24h)"] = []
            elif key == "volume_24h":
                crypto_table["Volume(24h)"] = []
            else:
                formated_key = key.replace("_", " ").title()
                crypto_table[formated_key] = []
    for crypto in data["data"].values():
        if not model.price and crypto.get("quote", {}).get("USD", {}).get("price"):
            continue
        crypto_table["Name"].append(crypto.get("name"))
        crypto_table["Symbol"].append(crypto.get("symbol"))
        quote = crypto.get("quote", {}).get("USD", {})
        if model.price:
            crypto_table["Price"].append(quote.get("price"))
        if model.token_address:
            crypto_table["Token Address"].append(crypto.get("token_address"))
        if model.market_cap_abbrv:
            crypto_table["Market Cap Abbrv"].append(get_amount_abbrv(quote.get("market_cap")))
        if model.market_cap:
            crypto_table["Market Cap"].append(quote.get("market_cap"))
        if model.market_cap_dominance:
            crypto_table["Market Cap Dominance"].append(quote.get("market_cap_dominance"))
        if model.volume_24h:
            crypto_table["Volume(24h)"].append(quote.get("volume_24h"))
        if model.circulating_supply:
            crypto_table["Circulating Supply"].append(crypto.get("circulating_supply"))
        if model.total_supply:
            crypto_table["Total Supply"].append(crypto.get("total_supply"))
        if model.volume_change_24h:
            crypto_table["Volume Change(24h)"].append(quote.get("volume_change_24h"))
        if model.supply_percent:
            circ = crypto.get("circulating_supply", 0)
            total = crypto.get("total_supply", 0)
            crypto_table["Supply %"].append(
            round((circ / total) * 100, 2) if total else "N/A"
        )
    return pd.DataFrame(crypto_table)


def zip_csv_and_xlsx(dataframe: pd.DataFrame) -> bytes:
    zip_file = io.BytesIO()
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zip:
        
        # write dataframe to excel file
        xlsx_file = io.BytesIO()
        dataframe.sort_values(by="Name", ascending=False).to_excel(
            xlsx_file, index=False
        )
        
        #write dataframe to csv file
        csv_file = io.BytesIO()
        dataframe.sort_values(by="Name", ascending=False).to_csv(
            csv_file, index=False
        )
        
        zip.writestr("crypto_data.xlsx", xlsx_file.getvalue())
        zip.writestr("crypto_data.csv", csv_file.getvalue())
    
    return zip_file.getvalue()
