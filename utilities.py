
import os
import requests
import json
import plaid
import plotly
import plotly.graph_objects as go
from plaid.errors import APIError, ItemError
from plaid import Client
from datetime import datetime, timedelta, date
import pandas as pd
from flatten_json import flatten
import numpy as np
import re
import math
from scipy import stats

############### Credentials and Keys ###############
def plaidClient():
    client = plaid.Client(os.getenv('PLAID_CLIENT_ID'),
                          os.getenv('PLAID_SECRET'),
                          os.getenv('PLAID_PUBLIC_KEY'),
                          os.getenv('PLAID_ENV'),
                          api_version='2018-05-22')
    return client

def plaidTokens():
    tokens = {}
    tokens['Chase'] = {'access_token': os.getenv('ACCESS_TOKEN_Chase'), 'item_id': os.getenv('ITEM_ID_Chase'), 'sandbox':os.getenv('ACCESS_TOKEN_Chase_SANDBOX')}
    tokens['Schwab'] = {'access_token': os.getenv('ACCESS_TOKEN_Schwab'), 'item_id': os.getenv('ITEM_ID_Schwab'), 'sandbox':os.getenv('ACCESS_TOKEN_Schwab_SANDBOX')}
    tokens['Great_Lakes'] = {'access_token': os.getenv('ACCESS_TOKEN_Lakes'), 'item_id': os.getenv('ITEM_ID_Lakes'), 'sandbox': os.getenv('ACCESS_TOKEN_Lakes_SANDBOX')}
    tokens['Capital_One'] = {'access_token': os.getenv('ACCESS_TOKEN_Cap1'), 'item_id': os.getenv('ITEM_ID_Cap1'), 'sandbox': os.getenv('ACCESS_TOKEN_Cap1_SANDBOX')}
    return tokens

############### API Interactions ###############
def getTransactions(client, token, start_date, end_date):
    try:
        account_ids = [account['account_id'] for account in client.Accounts.get(token)['accounts']]
        response = client.Transactions.get(token, start_date, end_date, account_ids=account_ids)

        num_available_transactions= response['total_transactions']
        print("{} Transactions Recieved from Plaid".format(num_available_transactions))
        num_pages = math.ceil(num_available_transactions / 500)
        transactions = []

        for page_num in range(num_pages):
            print("{}% Complete".format(page_num/num_pages * 100))
            transactions += [transaction for transaction in client.Transactions.get(token, start_date, end_date, account_ids=account_ids, offset=page_num * 500, count=500)['transactions']]

        return transactions

    except plaid.errors.PlaidError as e:
        print(json.dumps({'error': {'display_message': e.display_message, 'error_code': e.code, 'error_type': e.type } }))
        print('full error: ', e)
        transactions = {'result': e.code}
        balance = {'result': e.code}

        return transactions, balance


def cap1_lakes_get(client, token, start_date, end_date):
    account_ids = [account['account_id'] for account in client.Accounts.get(token)['accounts']]
    response = client.Transactions.get(token, start_date, end_date, account_ids=account_ids)
    return response


def getData(environment, exclusions, start_date):
    master_data = {}
    tokens = plaidTokens()
    today_str = str(date.today())

    if environment == 'testing' or environment == 'local_testing':
        client = plaidClient()
        trnsx_chase = getTransactions(client, tokens['Chase']['access_token'], start_date, today_str)
        trnsx_schwab = getTransactions(client, tokens['Schwab']['access_token'], start_date, today_str)
        master_data['all_trnsx'] = trnsx_chase + trnsx_schwab

    # elif environment == 'production':
    #     client = plaidClient()
    #     start_date = date.today().replace(year = date.today().year - 2).strftime('%Y-%m-%d')
    #     trnsx_chase, master_data['balance_chase'] = getTransactions(client, tokens['Chase']['access_token'], start_date, today_str)
    #     trnsx_schwab, master_data['balance_schwab'] = getTransactions(client, tokens['Schwab']['access_token'], start_date, today_str)
    #     cap1_response = cap1_lakes_get(client, tokens['Capital_One']['access_token'], start_date, today_str)
    #     lakes_response = cap1_lakes_get(client, tokens['Great_Lakes']['access_token'], start_date, today_str)


        # master_data['chase_total'] = pandaSum(json2pandaClean(trnsx_chase, exclusions))
        # master_data['schwab_total'] = pandaSum(json2pandaClean(trnsx_schwab, exclusions))
        # master_data['lakes_balance'], master_data['lakes_total'] = lakesData(lakes_response)
        # master_data['cap1_balance'], master_data['cap1_total'] = lakesData(cap1_response)



    # except Exception as e:

    #     master_data['chase_total'] = 0
    #     master_data['schwab_total'] = 0
    #     master_data['cap1_total'] = 0
    #     master_data['lakes_total'] = 0
    #     master_data['cap1_balance'] = 0
    #     master_data['lakes_balance'] = 0
    #     master_data['all_trnsx'] = {'error': e}
    #     print('Exception!!', master_data['all_trnsx'])
    return master_data

############### Data Scrubbing ###############

def json2pandaClean(data, exclusions):
  count = 0
  flat_list = []
  for d in data:
    count += 1
    try:
        dic_flattened = flatten(d)
        flat_list.append(dic_flattened)
        print("{}% Flattened".format(count/len(data) * 100))
    except Exception as e:
        print(e)
  df = pd.DataFrame(flat_list)
  df = df.loc[df.pending == False]
  df = drop_columns(df)
  df["date"] = pd.to_datetime(df['date'])
  df = df.sort_values(by='date', ascending=False)
  # df['date'] = df['date'].dt.strftime('%m/%d/%y')
  df = df.set_index('date')
  df = df[~df['category_id'].isin(exclusions)]

  # df['name'] = df['name'].str.capitalize()
  # df.columns = map(str.capitalize, df.columns)
  return df

def drop_columns(df):
    df = df.drop(columns=['location_address', 'location_city', 'location_lat',
       'location_lon', 'location_state', 'location_store_number','account_owner','payment_meta_by_order_of',
       'payment_meta_payee', 'payment_meta_payer',
       'payment_meta_payment_method', 'payment_meta_payment_processor',
       'payment_meta_ppd_id', 'payment_meta_reason',
       'payment_meta_reference_number','pending_transaction_id', 'transaction_id', 'pending'])
    return df


def currencyConvert(x):
    return '${:,.2f}'.format(x)

def monthStart():
    todayDate = date.today()
    if todayDate.day < 15 and todayDate.month == 1:
        start_year = todayDate.year -1
        month_start = str(start_year) + '-' + str(12) + '-' + str(15)
    elif todayDate.day < 15 and todayDate.month != 1:
        month_start = str(todayDate.year) + '-' + str(todayDate.month - 1) + '-' + str(15)
    else:
        month_start = str(todayDate.year) + '-' + str(todayDate.month) + '-' + str(15)
    print('Start Date of Current Billing Period: ', datetime.strptime(month_start, '%Y-%m-%d').strftime('%m/%d/%y'))

    return month_start

def dataPrep(json, exclusions):
    clean = json2pandaClean(json, exclusions)
    print(clean.columns)
    clean['account_id'] = clean['account_id'].map({'LOgERxzqrNFLPZdyNx7oFb9JwX39wzU05vVvd': 'Chase', 'vqmBXOzaoOuxNRe533YbhrV4r0NqELCmZr5vX': 'Schwab'})
    tdf=clean.rename(columns = {'account_id':'account'})
    print(tdf.columns)
    tdfst = tdf[['account', 'amount', 'name', 'category_0', 'category_1', 'category_2']]
    return tdfst

def bubbleData(data):
    bd = data.loc[monthStart():]
    hover_text = []
    bubble_size = []

    for index, row in bd.iterrows():
        hover_text.append((
                          'Name: {name}<br>'+
                          'Amount: {amount}<br>'+
                          'Category: {category_0}<br>'+
                          'Sub-Category: {category_1}<br>'+
                          'Account: {account}').format(
                                                name=row['name'],
                                                amount=row['amount'],
                                                category_0=row['category_0'],
                                                category_1=row['category_1'],
                                                account=row['account']))
        if row['amount'] < 0:
            bubble_size.append(math.sqrt(row['amount'] * -1))
        else:
            bubble_size.append(math.sqrt(row['amount']))

    bd['text'] = hover_text
    bd['size'] = bubble_size
    sizeref = 2.*max(bd['size'])/(100**2)

    # Dictionary with dataframes for each continent
    categories = bd['category_1'].unique()
    category_data = {cat:bd.query("category_1 == '%s'" %cat) for cat in categories}

    return category_data, sizeref

def bubbleFig(data, size):
  fig = go.Figure()

  for category_name, info in data.items():
      fig.add_trace(go.Scatter(
          x=info.index, y=info['category_0'],
          name=category_name, text=info['text'],
          marker_size=info['size'],
          ))

  fig.update_traces(mode='markers', marker=dict(sizemode='area',
                                                sizeref=size, line_width=2))
  fig.update_layout(
      title='Transactions',
      xaxis=dict(
          title='Date',
          gridcolor='white',
          gridwidth=2,
      ),
      yaxis=dict(
          title='Amount',
          gridcolor='white',
          gridwidth=2,
      ),
      paper_bgcolor='rgb(243, 243, 243)',
      plot_bgcolor='rgb(243, 243, 243)',
  )
  return fig

def stackData(data):
  dt = data.reset_index()
  grouped = dt.groupby([dt['date'].dt.strftime('%b %Y'),'category_1']).sum().reset_index()
  pt = grouped.pivot_table(index='category_1', columns='date', values='amount', fill_value=0)
  pt = pt.sort_index(axis='columns', level='date', ascending=False)

  return pt

def stackFig(data):
  traces = [go.Bar(x=row.keys(),
             y=row.values,
             name=index)
      for index, row in data.iterrows()]

  stack_fig = go.Figure(data=traces, layout=go.Layout(title=go.layout.Title(text="Monthly Stack")))
  stack_fig.update_layout(barmode='stack', xaxis={'categoryorder':'trace'}, bargap=0.15,)

  return stack_fig

def felineData(data):
  df_name_sum = data.groupby('category_1').resample('M', loffset=pd.Timedelta(-16, 'd')).sum()
  reset_df = df_name_sum.reset_index()

  categories = reset_df['category_1'].unique()
  line_data = {cat:reset_df.query("category_1 == '%s'" %cat) for cat in categories}

  return line_data

def felineFig(data):
  line_fig = go.Figure()
  for k, v in data.items():
      v['date'] = v['date'].dt.strftime('%b %Y')
      line_fig.add_trace(go.Scatter(x=v['date'].values, y=v['amount'].values, mode='lines+markers', name=k))

  return line_fig

def nameLineData(data):
  df_name_sum = data.groupby('name').resample('M', loffset=pd.Timedelta(-16, 'd')).sum()
  reset_df = df_name_sum.reset_index()
  reset_df['name'] = reset_df['name'].str.replace(r'[^\w\s]', '')
  names = reset_df['name'].unique()

  rng = reset_df['date'].unique()
  filled_df = (reset_df.set_index('date')
              .groupby('name')
              .apply(lambda d: d.reindex(rng))
              .drop('name', axis=1)
              .reset_index('name')
              .fillna(0))
  filled_df = filled_df.reset_index()
  name_data = {n:filled_df.query("name == '%s'" %n) for n in names}

  return name_data

def nameLineFig(data):
  name_line_fig = go.Figure()
  for k, v in data.items():
      v = v.sort_values(by='date', ascending=True)
      v['date'] = v['date'].dt.strftime('%b %Y')
      name_line_fig.add_trace(go.Scatter(x=v['date'].values, y=v['amount'].values, mode='lines+markers', name=k, connectgaps=False))

  return name_line_fig

def relativeData(data):
    df_mean = data.groupby([pd.Grouper(freq='M'), 'category_1'])['amount'].mean().unstack().mean(axis=0)

    df_current = data.loc[monthStart():]
    df_current = df_current.groupby('category_1')['amount'].mean()

    combined = pd.concat([df_current, df_mean], axis=1, sort=True).dropna()
    combined.columns = ['This Month', 'Average']
    combined = combined.sort_values('This Month')
    z = np.abs(stats.zscore(combined))
    comb = combined[(z < 4).all(axis=1)].sort_values('This Month')

    return comb

def relativeFig(data):
  rel_fig = go.Figure()
  rel_fig.add_trace(go.Bar(x=data.index.tolist(),
                  y=data['This Month'].tolist(),
                  name='This Month'
                  ))
  rel_fig.add_trace(go.Bar(x=data.index.tolist(),
                  y=data['Average'].tolist(),
                  name='Average'
                  ))

  rel_fig.update_layout(
      title='Relative Category Spending',
      xaxis=dict(title='Category', tickangle=90, tickfont=dict(size=12)
      ),
      yaxis=dict(title='Spent'
      ),
      legend=dict(x=0, y=1.0, bgcolor='rgba(255, 255, 255, 0)', bordercolor='rgba(255, 255, 255, 0)'
      ),
      barmode='group'
  )
  rel_fig.update_yaxes(automargin=True)

  return rel_fig

def transactionTables(data, date, exclusions, hawk_mode):
    print('Date: ', datetime.strptime(date, '%Y-%m-%d').strftime('%m/%d/%y'))
    df = json2pandaClean(data, exclusions)
    df = df.reset_index()
    print(df.columns)
    df_current_posted = df.loc[df['date'] >= datetime.strptime(date, '%Y-%m-%d').strftime('%m/%d/%y')]
    df_current_posted = df_current_posted.fillna('-')
    print('*** Posted ***')
    print(df_current_posted.head())
    print('Min: ', df_current_posted.date.min())
    print('Max: ', df_current_posted.date.max())

    return df_current_posted
