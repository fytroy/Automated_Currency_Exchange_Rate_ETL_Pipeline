# Financial Data Pipeline

This project is a data pipeline that extracts, transforms, and loads financial data from two main sources:

1. **Web Scraping:** It scrapes financial news articles related to cybersecurity from American Banker.
2. **API Extraction:** It fetches the latest USD exchange rates from the Frankfurter API.

The collected data is then processed and can be loaded into a data warehouse, such as Google BigQuery, for further analysis.

## Features

- Scrapes article headlines, summaries, authors, and dates.
- Extracts daily currency exchange rates against the USD.
- Transforms the data into a clean, structured format using Pandas.
- Loads the transformed data into Google BigQuery.

## Getting Started

### Prerequisites

- Python 3.x
- Pip (Python package installer)
- Google Cloud SDK (if using BigQuery)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://your-repository-url.com
   ```
2. **Navigate to the project directory:**
   ```bash
   cd your-project-directory
   ```
3. **Install the required Python packages:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Set up your Google Cloud credentials:**
   - Follow the official documentation to create a service account and download the JSON key file.
   - Place the key file in the root of the project directory and name it `service_account_key.json`.

### Usage

To run the entire data pipeline, execute the `run_pipeline.bat` script:

```bash
run_pipeline.bat
```

This will:

1. Run the `api_extractor.py` script to fetch and load exchange rate data into BigQuery.
2. Run the `scraper.py` script to scrape the latest financial articles and save them to `fraud_articles.csv`.

## Project Structure

- `api_extractor.py`: Script to extract data from the Frankfurter API and load it into BigQuery.
- `scraper.py`: Script to scrape financial articles.
- `run_pipeline.bat`: Batch script to execute the data pipeline.
- `exchange_rates_2025-10-31.csv`: Sample CSV file with exchange rate data.
- `fraud_articles.csv`: CSV file where the scraped articles are stored.
- `requirements.txt`: A file listing the Python packages required for this project.
