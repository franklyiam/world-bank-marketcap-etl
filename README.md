# World Bank Market Capitalization ETL

This project implements an automated ETL (Extract, Transform, Load) pipeline that gathers information about the top 10 largest banks in the world by market capitalization (USD), converts the data into GBP, EUR, and INR using exchange rates from a CSV file, and stores the processed output in both CSV and SQLite database formats.

## 📌 Features

- **Web Scraping:** Extracts bank names and market capitalization (USD) from a Wikipedia page.
- **Data Transformation:** Converts market cap data into GBP, EUR, and INR using exchange rates from a local CSV.
- **Data Storage:** Saves processed data into:
  - CSV file (`Largest_banks_data.csv`)
  - SQLite database (`Banks.db`)
- **Logging:** Captures the progress of ETL stages in a log file (`code_log.txt`)
- **SQL Queries:** Runs predefined queries to demonstrate data access.

## 🧪 Technologies Used

- Python 3.x
- BeautifulSoup (bs4)
- Pandas
- NumPy
- SQLite3
- Requests

## 🗃️ Project Structure

```bash
.
├── exchange_rate.csv            # Exchange rates to convert USD to GBP, EUR, and INR
├── Largest_banks_data.csv       # Output file with converted market caps
├── Banks.db                     # SQLite database with processed data
├── code_log.txt                 # Log file for ETL process
└── etl_pipeline.py              # Main ETL script (code provided in this repo)
```

## ⚙️ How to Run

1. **Clone the repository:**

```bash
git clone https://github.com/yourusername/world-bank-marketcap-etl.git
cd world-bank-marketcap-etl
```

2. **Install dependencies:**

```bash
pip install -r requirements.txt
```

> **Note:** You can manually create `requirements.txt` with the following lines:
> ```
> beautifulsoup4
> pandas
> numpy
> requests
> ```

3. **Prepare exchange rate file:**  
Create a file named `exchange_rate.csv` in the same directory with the following format:

```csv
Currency,Rate
GBP,0.76
EUR,0.88
INR,82.12
```

4. **Run the ETL script:**

```bash
python etl_pipeline.py
```

## 🧾 Example SQL Queries Executed

- Print entire table
- Average market capitalization in GBP
- Top 5 banks by name

## 📅 Use Case

This tool is ideal for financial analysts and institutions that need a repeatable and automated way to retrieve, process, and store market cap data of global banks every financial quarter.

## ✅ Unit Testing

Unit tests are included to validate the ETL pipeline functions such as extraction, transformation, and data loading.

### Run Tests

To execute unit tests:

```bash
python -m unittest test_etl_pipeline.py
```

These tests verify:
- Web scraping returns a non-empty DataFrame
- Currency conversion columns are correctly added
- Data is written to CSV and database without issues

Ensure the test file `test_etl_pipeline.py` is located in the same directory as your main ETL script.

## 📝 License

This project is licensed under the MIT License.
