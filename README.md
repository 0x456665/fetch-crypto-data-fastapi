# Crypto Data API

A FastAPI-based service that fetches cryptocurrency data from CoinMarketCap and provides downloadable reports in CSV and Excel formats.

## Features

- Fetch real-time cryptocurrency data from CoinMarketCap API
- Customizable data fields through query parameters
- Download data in both CSV and Excel formats (zipped)
- Supports multiple cryptocurrencies in a single request
- Built with FastAPI for high performance

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <project-directory>
   ```

2. Create and activate a virtual environment (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   venv\Scripts\activate    # Windows
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the root directory with your CoinMarketCap API key:

   ```env
   API_KEY=your_coinmarketcap_api_key_here
   CP_SECRET=your_secret_key_here
   ```

## Usage

### Running the Server

```bash
python3 -m app.main
```

The server will start at `http://localhost:8000`

### API Endpoints

1. **Root Endpoint**

   ```
   GET /
   ```

   Returns a simple greeting message.

2. **Data Download Endpoint**

   ```
   GET /api/data/download
   ```

   Parameters:
   - `tickers`: Comma-separated cryptocurrency symbols (e.g., `BTC,ETH`) or special secret key
   - Other boolean parameters to control which fields to include (all optional)

### Example Requests

1. Get data for Bitcoin and Ethereum:

   ```bash
   curl "{{BASE_URL}}api/data/download?tickers=BTC,ETH&price=true&market_cap_abbrv=true"
   ```

2. Get all default tokens (using secret key):

   ```bash
   curl "{{BASE_URL}}/api/data/download?tickers=your_secret_key_here"
   ```

## Query Parameters

| Parameter               | Type    | Default | Description                          |
|-------------------------|---------|---------|--------------------------------------|
| `tickers`               | string  | None    | Comma-separated crypto symbols       |
| `price`                 | boolean | true    | Include price data                   |
| `market_cap`            | boolean | false   | Include full market cap              |
| `market_cap_abbrv`      | boolean | true    | Include abbreviated market cap       |
| `volume_24h`            | boolean | true    | Include 24h volume                   |
| `total_supply`          | boolean | false   | Include total supply                 |
| `circulating_supply`    | boolean | false   | Include circulating supply           |
| `supply_percent`        | boolean | true    | Include supply percentage            |
| `volume_change_24h`     | boolean | false   | Include 24h volume change            |
| `token_address`         | boolean | false   | Include token addresses              |
| `market_cap_dominance`  | boolean | false   | Include market cap dominance         |

## Response

The API returns a ZIP file containing two files:

1. `crypto_data.csv` - CSV format
2. `crypto_data.xlsx` - Excel format

## Development

### Project Structure

```
app/
├── config.py       # Configuration settings
├── main.py         # FastAPI application and routes
├── utils.py        # Utility functions and data processing
└── validators.py   # Pydantic models for request validation
```

### Dependencies

- FastAPI
- Uvicorn
- Pydantic
- pandas
- aiohttp
- python-dotenv

All dependencies are listed in `requirements.txt`

## License

[MIT License](LICENSE)
