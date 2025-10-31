import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time # We need 'time' to tell the browser to wait

# --- NEW: Import Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- Class names (these are still correct) ---
URL = "https://www.americanbanker.com/cyber-security"
ARTICLE_CARD_TAG = "div"
ARTICLE_CARD_CLASS = "PromoSmall-wrapper"
HEADLINE_TAG = "h3"
HEADLINE_CLASS = "PromoSmall-title"
SUMMARY_TAG = "p"
SUMMARY_CLASS = "PromoSmall-description"
AUTHOR_TAG = "a"
AUTHOR_CLASS = "PromoSmall-author"
DATE_TAG = "div"
DATE_CLASS = "PromoSmall-created"

# --- NEW: Setup Selenium ---
print("Setting up Selenium browser...")
options = Options()
options.add_argument("--headless") # Run in the background, no browser window pops up
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")

# This is the magic: it automatically downloads the correct driver for your Chrome
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

print(f"Fetching webpage from {URL} with Selenium...")
try:
    # 1. Tell Selenium to get the page
    driver.get(URL)

    # 2. THIS IS THE KEY: Wait for the JavaScript to run
    print("Waiting 5 seconds for dynamic content to load...")
    time.sleep(5) # Wait 5 seconds

    # 3. Get the page's HTML *after* the JavaScript has run
    html = driver.page_source
    print("Page fetched successfully!")

finally:
    # 4. Always close the browser
    driver.quit()
    print("Browser closed.")

# --- The rest of our script is almost the same ---

# 5. Parse the HTML (which is now complete)
soup = BeautifulSoup(html, "html.parser")

# 6. Find all article "cards"
article_cards = soup.find_all(ARTICLE_CARD_TAG, class_=ARTICLE_CARD_CLASS)

print(f"Found {len(article_cards)} article cards.")

if len(article_cards) == 0:
    print("No articles found. The website structure may have changed.")

article_list = []

# 7. Loop through each article card and extract data
for card in article_cards:

    # Helper function to find text safely
    def get_text(element, tag, class_):
        found = element.find(tag, class_=class_)
        if found:
            return found.text.strip()
        return "N/A"

    # Get Headline
    headline = get_text(card, HEADLINE_TAG, HEADLINE_CLASS)

    # Get Summary
    summary = get_text(card, SUMMARY_TAG, SUMMARY_CLASS)

    # Get Author
    author = get_text(card, AUTHOR_TAG, AUTHOR_CLASS)
    if author != "N/A":
        author = author.replace("By ", "")

        # Get Date
    date = get_text(card, DATE_TAG, DATE_CLASS)
    if date != "N/A":
        date = re.sub(r'\s+', ' ', date)

    article_list.append({
        "Headline": headline,
        "Summary": summary,
        "Author": author,
        "Date": date
    })

# 8. Convert list to a Pandas DataFrame
df = pd.DataFrame(article_list)

print("\n--- Scraping Results ---")
print(df.head()) # Print the first 5 articles

# Save to a CSV file to check our work
df.to_csv("fraud_articles.csv", index=False)
print("\nSuccessfully saved data to fraud_articles.csv")