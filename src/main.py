import pandas as pd
import re
import requests as req
from datetime import datetime
from datetime import timedelta
from typing import Union
from stop_words import stop_words

NASDAQ = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
OTHERS = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'

nasdaq = pd.read_csv(NASDAQ, sep='|')
nasdaq.drop(nasdaq.shape[0]-1, inplace=True)
nasdaq_tickers = nasdaq[nasdaq['ETF'] == 'N'].Symbol.tolist()

# other = pd.read_csv(OTHERS, sep='|')
# other.drop(other.shape[0]-1, inplace=True)
# other_tickers = other[other['ETF'] == 'N'].Symbol.tolist()


def convert_epoch_to_utc(epoch_time: str):
    """
    Converts epoch time to a datetime.datetime object.
    """
    ts = int(epoch_time)
    return datetime.utcfromtimestamp(ts)


def convert_utc_to_epoch(
    utc_time: Union[datetime, str],
    time_format: str = '%Y-%m-%d %H:%M:%S'
):
    """
    Converts a string or a datetime.datetime object
    to a epoch timestamp.
    """
    if type(utc_time) == str:
        dt = datetime.strptime(str, time_format)
        dt_epoch = int(dt.timestamp())
        dt_epoch_str = str(dt_epoch)
    elif type(utc_time) == datetime:
        dt_epoch = int(utc_time.timestamp())
        dt_epoch_str = str(dt_epoch)
    else:
        raise TypeError("""Only datetime.datetime class and str
        utc_time is accepted as a valid parameter.""")
    return dt_epoch_str


def get_pushshift_data(
    comment: bool = True,
    show_uri=False, **kwargs
):
    """
    Uses the pushshift api to search Reddit posts using specific
    query parameters.
    See https://github.com/pushshift/api for more info.

    Parameters
    ---------
        comment (bool):
            When set to `True`, we search for all the comments related
            to the search query. When `False` search submissions.
            Default `True`.
        show_uri (bool):
            The resulting url used to GET from the pushshift
            is printed when set to `True`. When `False` the printing
            is silenced.
        **kwargs:
            See https://github.com/pushshift/api and view the
            parameter tables.
            Where the `parameter` is the keyword.
            Use the `Accepted Values` column as a guide to setting
            the parameter values.
            e.g. get_pushshift_data(
                comment=True, show_uri=False,
                q='Stocks',
                size=25,
                sort='desc',
                subreddit='WallStreetBets'
            )
    Returns
    -------
        dict:
            The data retrieved from the API.
    """
    if comment:
        api_uri = 'https://api.pushshift.io/reddit/search/comment/?'
    else:
        api_uri = 'https://api.pushshift.io/reddit/search/submission/?'

    search_params = ['='.join([k, str(v)]) for k, v in kwargs.items()]
    search_param_str = '&'.join(search_params)
    search_api_uri = api_uri + search_param_str
    if show_uri:
        print(search_api_uri)
    res = req.get(search_api_uri)
    return res.json()


def parse_pushshift_data(json_data: dict):
    df = pd.DataFrame(json_data['data'])
    cash_tag_pattern = r'(?:\$|\#)([A-Za-z]+)'
    caps_pattern = r'(?<![A-Z])([A-Z]{3,4}?)(?![A-Za-z])'
    df['CASH_TAG'] = df.title.apply(lambda x: re.findall(cash_tag_pattern, x))
    df['POTENTIAL_CASH_TAG'] = df.title.apply(lambda x: re.findall(caps_pattern, x))
    df['CREATED_UTC'] = df.created_utc.apply(lambda x: convert_epoch_to_utc(x))
    df.drop(df[(df['CASH_TAG'].str.len() == 0)
            & (df['POTENTIAL_CASH_TAG'].str.len() == 0)].index, inplace=True)

    def make_upper(word_list):
        return [tag.upper() for tag in word_list]
    df.reset_index(drop=True, inplace=True)

    df['CASH_TAG'] = df.CASH_TAG.apply(lambda x: make_upper(x))

    def combine_tags(row):
        return list(set(row['CASH_TAG'] + row['POTENTIAL_CASH_TAG']))

    df['TAG_MERGE'] = df.apply(lambda x: combine_tags(x), axis=1)

    def remove_stopwords(word_list):
        global stop_words
        return [word for word in word_list if word not in stop_words]

    df['TAG_MERGE'] = df['TAG_MERGE'].apply(lambda x: remove_stopwords(x))
    df.drop(df[df['TAG_MERGE'].str.len() == 0].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df = df[[
         'CREATED_UTC'
         , 'TAG_MERGE'
    ]].explode('TAG_MERGE')
    return df


def main(*args, **kwargs):
    ts = datetime.now()  # The current time
    td = timedelta(days=7)  # A time difference of 7 days
    t = ts - td  # The time 7 days ago.
    t_epoch_str = convert_utc_to_epoch(t)
    json_data = get_pushshift_data(
        comment=False,
        show_uri=True,
        metadata='false',
        size=500,
        fields='created_utc,retrieved_on,selftext,title,upvote_ratio,total_awards_received',
        subreddit='wallstreetbets',
        after=t_epoch_str
    )

    df = parse_pushshift_data(json_data)
    d_pattern = '%y%m%d'
    path_str = f'./data/{ts.strftime(d_pattern)}_{t.strftime(d_pattern)}.csv'
    df.to_csv(path_str, index=False)


if __name__ == '__main__':
    main()
