import pandas as pd
import re
import time
import requests as req
from datetime import datetime
from datetime import timedelta
from typing import Union
import constant

nasdaq = pd.read_csv(constant.NASDAQ, sep='|')
nasdaq.drop(nasdaq.shape[0]-1, inplace=True)
nasdaq_tickers = nasdaq[nasdaq['ETF'] == 'N'].Symbol.tolist()

other = pd.read_csv(constant.OTHERS, sep='|')
other.drop(other.shape[0]-1, inplace=True)
other_tickers = other[other['ETF'] == 'N']['NASDAQ Symbol'].tolist()

nasdaq_tickers.extend(other_tickers)

def create_folders():
    """
    Creates folders to organise our project tree.
    """
    import os
    raw_data_path = os.path.join('data', 'raw')
    if not os.path.isfile(raw_data_path):
        try:
            os.makedirs(raw_data_path)
        except Exception as e:
            print(e)
    else:
        pass
    
    pro_data_path = os.path.join('data', 'processed')
    if not os.path.isfile(pro_data_path):
        try:
            os.makedirs(pro_data_path)
        except Exception as e:
            print(e)

    plot_path = os.path.join('plots')
    if not os.path.isfile(plot_path):
        try:
            os.makedirs(plot_path)
        except Exception as e:
            print(e)


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
    """
    Take the raw PushShift data and parses it into a pandas.DataFrame
    
    Parameters
    ----------
    json_data (dict):
        The raw json data retrieved from PushShift API as a dictionary object.
        The json data must contain the fields 'title' and 'created_utc'.

    Returns
    -------
    df (pandas.DataFrame):
        A DataFrame object.
    """
    df = pd.DataFrame(json_data['data'])
    cash_tag_pattern = r'(?:\$)([A-Za-z]+)'
    caps_pattern = r'(?<![A-Z_])([A-Z]{3,4}?)(?![A-Za-z_])'
    # print(df.shape)
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

    def remove_nontickers(word_list):
        return [word for word in word_list if word in nasdaq_tickers]

    def remove_stopwords(word_list):
        return [word for word in word_list if word not in constant.STOPWORDS]

    df['TAG_MERGE'] = df['TAG_MERGE'].apply(lambda x: remove_nontickers(x))
    df['TAG_MERGE'] = df['TAG_MERGE'].apply(lambda x: remove_stopwords(x))

    df.drop(df[df['TAG_MERGE'].str.len() == 0].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df = df[[
         'CREATED_UTC'
         , 'TAG_MERGE'
    ]].explode('TAG_MERGE')
    return df


def get_start_date(end_day: datetime = datetime.now(),
     timeframe_days: int = 7, str_fmt_date: bool = False):
    """
    Finds the start day given a timeframe and the end day.
    Args:
        end_day (datetime, optional): 
            The ending date. Defaults to datetime.now().
        timeframe_days (int, optional): 
            The time difference in days between end date and start date. Defaults to 7.
        str_fmt_date (bool, optional):
            When true, returns the end date as a datetime object,
            or when false, a string with the format '%y-%m-%d'. Defaults to True.

    Returns:
        datetime, str: the start date.
    """
    time_difference = timedelta(days=timeframe_days)
    start_day = end_day - time_difference
    if str_fmt_date == True:
        return convert_utc_to_epoch(start_day)
    else:
        return start_day
    

def iterate_get_pushshift_data(comment: bool = False, show_uri: bool = True,
                             timeframe_days: int = 7, **kwargs):
    """
    The PushShift API only allows retriving 100 submissions at a time.
    This function iterates through chunks (100 in size) of data retrieved from the API then
    concatenates the results into a single DataFrame.
    Parameters:
    -----------
        comment (bool, optional): 
            When true, comments are retrieved.
            When false, submission are retrieved. Defaults to False.
        show_uri (bool, optional): 
            When true, prints the URI to console.
            When false, URI is silenced. Defaults to True.
        timeframe_days (int, optional):
            How far back (in days) we start retrieving our data.
    Returns:
        pandas.DataFrame: The result table after concatenation.
    """
    start_date = get_start_date(timeframe_days=timeframe_days)
    start_date_str = convert_utc_to_epoch(start_date)
    max_date = start_date_str

    # Make a container to store all the DataFrames.
    dfs = []

    # Maximum query size is 100
    q_size = constant.API_QUERY_SIZE
    
    # Iteratively make the query; if API error response, retry in 10 seconds
    while(q_size==constant.API_QUERY_SIZE):
        while True:
            try:
                json_data = get_pushshift_data(comment=comment, show_uri=show_uri,
                                            after=max_date, **kwargs) 
                if len(json_data['data']) != 0:
                    max_date = max([sub['created_utc'] for sub in json_data['data']])
                    q_size = len(json_data['data'])
                    df = parse_pushshift_data(json_data)
                    dfs.append(df)
                else:
                    q_size = 0
            except Exception as e:
                print(e)
                print('Retry in 5 seconds...')
                time.sleep(5)
                continue
            break
    
    return pd.concat(dfs, ignore_index=True)


def main(*args, **kwargs):
    timeframe = constant.TIMEFRAME
    create_folders()

    df = iterate_get_pushshift_data(
        comment=False,
        show_uri=True,
        timeframe_days=timeframe,
        metadata='false',
        size=100,
        fields='created_utc,retrieved_on,selftext,title',
        subreddit='wallstreetbets',
        sort='asc',
        sort_type='created_utc'
    )

    start_date = get_start_date(timeframe_days=timeframe)
    end_date = datetime.now()
    d_pattern = '%y%m%d'
    path_str = f'./data/raw/{start_date.strftime(d_pattern)}_{end_date.strftime(d_pattern)}_raw.csv'
    df.to_csv(path_str, index=False)


if __name__ == '__main__':
    main()
