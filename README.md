# wsb_report

## Last update 2021-05-26

## About

This project aims to retrieve user generated data on [r/WallStreetBets](https://www.reddit.com/r/wallstreetbets/)
to determine the popular stock picks and the sentiments behind them.

## Setup

`pip install -r requirements.txt`

## Files

* `main.py`:

    Running this script will query the PushShift API.

    By default, it will locate all submissions in r/WallStreetBets, scan for ticker symbols with three or more characters via Regex pattern matching; within the last 7 days.

    To remove any false-ticker symbols; the list of retrieved symbols are checked against the NASDAQ/NYSE listed stocks.
    There are also tickers symbols that coincide with everyday language on r/wallstreetbets. *e.g. CEO, NOW, BIG. etc*. These are also filtered out with a dedicated STOPWORDS list found in the `constant.py` file.

    This will then save all ticker symbols mentioned in posts along with the time the post was made.

* `constant.py`:

    This is a python file which lists some constant values like the link to the list of US stock exchange ticker symbols, the stop words and the maximum query size that the PushShift API allows.

    **Current constants are:**

    * `API_QUERY_SIZE`: Max API query size.
    
    * `NASDAQ`: Link to NASDAQ listed stocks

    * `OTHERS`: Link to non-NASDAQ US stocks

    * `STOPWORDS`: Words that commonly appear on WSB that represent a ticker symbol but in the context of everyday conversation in CAPSLOCK.

* `vis.py`:

    Running this script will find the stored data (default: csv file), and output some visualizations for the retrieved data.

    Currently a horizontal bar chart is generated to show the number of post mentions for the top20 stocks within the last 7 days.

## Example output

![plots/2021-05-25.png]