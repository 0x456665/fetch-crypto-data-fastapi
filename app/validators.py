from pydantic import BaseModel
from typing import Optional

class TableFieldsAndTickers(BaseModel):
    tickers: Optional[str] = None
    price: Optional[bool] = True
    market_cap: Optional[bool] = False
    market_cap_abbrv: Optional[bool] = True
    volume_24h: Optional[bool] = True
    total_supply: Optional[bool] = False
    circulating_supply: Optional[bool] = False
    supply_percent: Optional[bool] = True
    volume_change_24h: Optional[bool] = False
    token_address: Optional[bool] = False
    market_cap_dominance: Optional[bool]= False