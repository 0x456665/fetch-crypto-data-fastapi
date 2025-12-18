import aiohttp
import asyncio
from app.validators import TableFieldsAndTickers
import io
import pandas as pd
import zipfile
from fastapi.exceptions import HTTPException
import logging

from .config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    logger.info(f"Fetching crypto data for symbols: {symbols}")
    async with aiohttp.ClientSession() as session:
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": api_key}
        if isinstance(symbols, list):
            params = {"symbol": ",".join(symbols)}
        else:
            params = {"symbol": symbols}
        
        logger.info(f"API Request params: {params}")
        async with session.get(url, headers=headers, params=params) as response:
            if response.status not in [200, 201]:
                error_detail = await response.json()
                logger.error(f"API Error (status {response.status}): {error_detail}")
                raise HTTPException(
                   status_code=response.status,
                   detail=error_detail,
               )
        
            result = await response.json()
            logger.info(f"API Response received. Data keys: {list(result.get('data', {}).keys())}")
            return result


def build_crypto_table(data, model: TableFieldsAndTickers = TableFieldsAndTickers()):
    logger.info("Building crypto table...")
    logger.info(f"Model fields: {model.model_dump()}")
    
    crypto_table = {
        "Name": [],
        "Symbol": [],
    }

    field_dictionary = model.model_dump(exclude_none=True)
    logger.info(f"Enabled fields: {[k for k, v in field_dictionary.items() if v != False and k not in ['tickers', 'secret']]}")
    
    for key in field_dictionary.keys():
        if field_dictionary[key] != False and key not in ["tickers", "secret"]:
            if key == "supply_percent":
                crypto_table["Supply %"] = []
            elif key == "volume_change_24h":
                crypto_table["Volume Change(24h)"] = []
            elif key == "volume_24h":
                crypto_table["Volume(24h)"] = []
            else:
                formated_key = key.replace("_", " ").title()
                crypto_table[formated_key] = []
    
    logger.info(f"Initialized table columns: {list(crypto_table.keys())}")
    logger.info(f"Number of cryptos in response: {len(data.get('data', {}))}")
    
    crypto_count = 0
    skipped_count = 0
    
    for symbol, crypto in data["data"].items():
        logger.debug(f"Processing crypto: {symbol} - {crypto.get('name')}")
        
        # FIXED: The bug was here - this condition would skip adding to some arrays but not all
        # Removing this faulty logic - if price is not requested, we just don't add it to the table
        # But we should still process the crypto entry
        # if not model.price and crypto.get("quote", {}).get("USD", {}).get("price"):
        #     continue
        
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
        
        crypto_count += 1
    
    logger.info(f"Processed {crypto_count} cryptos, skipped {skipped_count}")
    
    # Log array lengths before creating DataFrame
    array_lengths = {key: len(value) for key, value in crypto_table.items()}
    logger.info(f"Array lengths: {array_lengths}")
    
    # Check if all arrays have the same length
    lengths = set(array_lengths.values())
    if len(lengths) > 1:
        logger.error(f"CRITICAL: Array length mismatch detected! Lengths: {array_lengths}")
        raise ValueError(f"All arrays must be of the same length. Got: {array_lengths}")
    
    logger.info(f"Creating DataFrame with {crypto_count} rows and {len(crypto_table)} columns")
    return pd.DataFrame(crypto_table)


def zip_csv_and_xlsx(dataframe: pd.DataFrame, timestamp: str = None) -> bytes:
    """
    Create a zip file containing CSV and XLSX versions of the dataframe.
    
    Args:
        dataframe: The pandas DataFrame to export
        timestamp: Optional timestamp string for naming files (e.g., "2025-12-18_15-14-38")
    """
    from datetime import datetime
    
    # Generate timestamp if not provided
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    logger.info(f"Creating zip file with timestamp: {timestamp}")
    
    zip_file = io.BytesIO()
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zip:
        
        # Create folder name with timestamp
        folder_name = f"crypto_data_{timestamp}"
        
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
        
        # Write files inside timestamped folder
        zip.writestr(f"{folder_name}/crypto_data_{timestamp}.xlsx", xlsx_file.getvalue())
        zip.writestr(f"{folder_name}/crypto_data_{timestamp}.csv", csv_file.getvalue())
    
    logger.info(f"Zip created with folder: {folder_name}")
    
    return zip_file.getvalue()
