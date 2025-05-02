#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import yfinance as yf
import datetime
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- BREAKOUT CRITERIA ----------
soft_breakout_pct = 0.005
proximity_threshold = 0.05
volume_threshold = 1.2
lookback_days = 252


# ---------- UNIVERSE SELECTION ----------
# Screening the Russell 1000/3000 requires a CSV file with all the underlying tickers

print("\n=== Universe Options ===")
print("0 - SPY (S&P 500)")
print("1 - S&P1500")
print("2 - Russell 1000")
print("3 - Russell 3000 (CSV required)")
print("4 - TSX Composite")

choice = int(input("Select Universe [0/1/2/3/4]: "))

if choice == 0:
    universe_name = "SPY"
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    universe = df["Symbol"].tolist()

elif choice == 1:
    universe_name = "S&P1500"
    df1 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    df2 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_400_companies")[0]
    df3 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies")[0]
    universe = df1["Symbol"].tolist() + df2["Symbol"].tolist() + df3["Symbol"].tolist()

elif choice == 2:
    universe_name = "Russell 1000"
    driver = webdriver.Chrome()
    driver.get("https://en.wikipedia.org/wiki/Russell_1000_Index")
    try:
        rows_xpath = "//table[4]//tbody/tr"
        rows = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, rows_xpath))
        )
        universe = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if cells and len(cells) > 1:
                ticker = cells[1].text.strip()
                if ticker:
                    universe.append(ticker)
        print(f"Extracted {len(universe)} tickers: {universe}")
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        driver.quit()

elif choice == 3:
    universe_name = "Russell 3000"
    try:
        universe = pd.read_csv("russell3000.csv")["Symbol"].tolist()
    except FileNotFoundError:
        print("Error: CSV file not found.")
        exit()
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        exit()

elif choice == 4:
    universe_name = "TSX Composite"
    driver = webdriver.Chrome()
    driver.get(
        "https://topforeignstocks.com/indices/the-components-of-the-sptsx-composite-index/"
    )
    try:
        rows_xpath = '//*[@id="tablepress-5032"]/tbody/tr'
        rows = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, rows_xpath))
        )

        universe = []
        for i, row in enumerate(rows):
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells or len(cells) <= 1:
                print(f"Row {i + 1} skipped: Not enough cells")
                continue

            ticker = cells[2].text.strip()
            if (
                ticker
            ):  # Relaxed validation to allow valid tickers with special characters
                universe.append(ticker)
            else:
                print(f"Row {i + 1} skipped: Invalid ticker {ticker}")

        # Append '.TO' for all tickers if necessary
        universe = [
            ticker + ".TO" if not ticker.endswith(".TO") else ticker
            for ticker in universe
        ]
        print(f"Extracted {len(universe)} tickers: {universe}")

    except Exception as e:
        print(f"Error during scraping TSX Composite tickers: {e}")

    finally:
        driver.quit()

else:
    valid_choices = [0, 1, 2, 3, 4]
    raise ValueError(f"Invalid Choice: {choice}. Please select from {valid_choices}.")

if choice in [0, 1, 2, 3]:
    universe = [ticker.replace(".", "-") for ticker in universe]
elif choice == 4:
    # Process tickers only if there is more than one dot, otherwise leave them unchanged
    universe = [
        ticker.replace(".", "-", 1) if ticker.count(".") > 1 else ticker
        for ticker in universe
    ]
    universe = [ticker for ticker in universe if ticker != "CWB.TO"]


print(f"\n Universe Loaded: {universe_name} ({len(universe)} tickers)")

# ---------- DOWNLOAD DATA ----------
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=lookback_days)
data = yf.download(
    universe, start=start_date, end=end_date, group_by="ticker", auto_adjust=True
)

# ---------- CLEAN DATA ----------
downloaded_tickers = [col for col in data.columns.get_level_values(0).unique()]
universe_cleaned = downloaded_tickers

print(f"\n Downloaded tickers: {len(universe_cleaned)}")
missing_tickers = list(set(universe) - set(universe_cleaned))
if missing_tickers:
    print(f" Missing tickers skipped: {missing_tickers}")

# ---------- PRICE & VOLUME ----------
close_prices = pd.DataFrame(
    {ticker: data[ticker]["Close"] for ticker in universe_cleaned}
)
high_prices = pd.DataFrame(
    {ticker: data[ticker]["High"] for ticker in universe_cleaned}
)
volume_dict = {ticker: data[(ticker, "Volume")] for ticker in universe_cleaned}
avg_vol_50_dict = {
    ticker: vol.rolling(50).mean() for ticker, vol in volume_dict.items()
}

# ---------- FIXED ROLLING HIGH (Using Intraday Highs) ----------
rolling_high = high_prices.rolling(lookback_days, min_periods=1).max()

# ---------- USE TODAY'S DATA (AFTER MARKET CLOSE) ----------
current_close = close_prices.iloc[-1]
rolling_high_today = rolling_high.iloc[-1]

# ---------- VOLUME RATIO ----------
latest_volume = pd.Series({ticker: vol.iloc[-1] for ticker, vol in volume_dict.items()})
latest_avg_volume = pd.Series(
    {ticker: avg.iloc[-1] for ticker, avg in avg_vol_50_dict.items()}
)
volume_ratio = latest_volume / latest_avg_volume

# ---------- PROXIMITY ----------
proximity = (rolling_high_today - current_close) / rolling_high_today

# ---------- BREAKOUTS & NEAR BREAKOUTS ----------

# Breakouts = within 0.5% and volume surge
high_breakers = (proximity <= soft_breakout_pct) & (volume_ratio > volume_threshold)

# Near Breakouts = within 5% (no volume requirement)
near_highs = (
    (proximity <= proximity_threshold) & (~high_breakers) & (rolling_high_today > 0)
)

# ---------- DATAFRAMES ----------

breakout_df = (
    pd.DataFrame(
        {
            "Price": current_close[high_breakers],
            "52-Week High": rolling_high_today[high_breakers],
            "Distance to High (%)": (proximity[high_breakers] * 100).round(2),
            "Volume Ratio": volume_ratio[high_breakers],
        }
    )
    .dropna()
    .sort_values(by="Distance to High (%)")
)

near_breakout_df = (
    pd.DataFrame(
        {
            "Price": current_close[near_highs],
            "52-Week High": rolling_high_today[near_highs],
            "Distance to High (%)": (proximity[near_highs] * 100).round(2),
            "Volume Ratio": volume_ratio[near_highs],
        }
    )
    .dropna()
    .sort_values(by="Distance to High (%)")
)

# ---------- EXPORT TO EXCEL ----------
today = datetime.datetime.today().strftime("%Y-%m-%d")
os.makedirs("outputs", exist_ok=True)
output_path = f"outputs/{universe_name}_Breakout_Screener_{today}.xlsx"

with pd.ExcelWriter(output_path) as writer:
    breakout_df.to_excel(writer, sheet_name="Breakouts")
    near_breakout_df.to_excel(writer, sheet_name="Near Breakouts")

print(f"\n Excel saved to: {output_path}")

# Output Summary

print("\n=== Summary ===")
print(f"Universe: {universe_name} | Date: {today}")
print(f"Breakouts Found: {len(breakout_df)}")
print(f"Near Breakouts Found: {len(near_breakout_df)}")

print("\n```")
if breakout_df.empty:
    print("Breakouts: None")
else:
    print(
        "Breakouts:\n",
        breakout_df[["Price", "Distance to High (%)", "Volume Ratio"]]
        .round(2)
        .to_string(),
    )

if near_breakout_df.empty:
    print("\nNear Breakouts: None")
else:
    print(
        "\nNear Breakouts:\n",
        near_breakout_df[["Price", "Distance to High (%)", "Volume Ratio"]]
        .round(2)
        .to_string(),
    )

print("```")


# In[ ]:
