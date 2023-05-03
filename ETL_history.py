import pandas as pd
import time
import datetime
import openpyxl
import requests
import web3
from web3_input_decoder import decode_function
import urllib.request
import json
from wsgiref.util import request_uri
from requests import Request
import os
import warnings

################################################
# Modify the following lines

# Desired path to save final file
path_export = 'Data/test'

# Input the lower and upper bound of the desired interval of analysis
# it has to be in unix timestamp. Use the online converter to convert a specific 
# date: https://www.unixtimestamp.com/


lower_bound = 1665586800
upper_bound = 1665586800+5000

#API KEYS -> you need an API key on https://optimistic.etherscan.io/
# store it in a txt file and specify in filename the path
filename = 'api.txt'


#####################################################
# From here do not modify 

with open(filename, 'r') as f:
    myAPIkey = f.read().strip()
 
 # Create directory
if not os.path.exists(path_export):
    os.makedirs(path_export)


#################
# Market constants
#################
perpetualFuturesID = "0xf86048DFf23cF130107dfB4e6386f574231a5C65"
marketID = '0xaE55F163337A2A46733AA66dA9F35299f9A46e9e'
market_key = '0x7345544800000000000000000000000000000000000000000000000000000000'

f = urllib.request.urlopen("https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address="+ 
                            marketID + '&apikey='+myAPIkey)
MARKET_ABI = json.loads(json.load(f)["result"])

alchemy_url = "https://opt-mainnet.g.alchemy.com/v2/0y9-DH0IguLrsfhK6W2Zk6Eutfa0ML8q"
w3 = web3.Web3(web3.HTTPProvider(alchemy_url))
mc = w3.eth.contract(address=marketID, abi=MARKET_ABI)

imax = mc.caller().maxFundingRate(market_key)/10**18
wmax = mc.caller().skewScaleUSD(market_key)/10**18


###################
# Retrieve input data
###################
f = urllib.request.urlopen("https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address="+ 
                            perpetualFuturesID + '&apikey='+myAPIkey)
FUTETH_ABI = json.loads(json.load(f)["result"])

    #-----------------------------------------
    # Kwenta transactions (events on the contract)
    #-----------------------------------------

starting_block = requests.get("https://api-optimistic.etherscan.io/api?module=block"+ '&address='+ \
    perpetualFuturesID +"&action=getblocknobytime&timestamp="+ str(lower_bound) + "&closest=before"+ '&apikey='+myAPIkey).json()['result']
lastBlock = requests.get("https://api-optimistic.etherscan.io/api?module=block"+ '&address='+ \
    perpetualFuturesID +"&action=getblocknobytime&timestamp="+ str(upper_bound) + "&closest=before"+ '&apikey='+myAPIkey).json()['result']


response = requests.get(
    'https://api-optimistic.etherscan.io/api?'+ \
    'module=account&action=txlist'+ '&address='+ perpetualFuturesID + \
    '&startblock='+str(starting_block)+'&endblock='+str(lastBlock)+'&sort=asc'+ '&apikey='+myAPIkey)

transactions = pd.DataFrame.from_dict(response.json()['result'])[['timeStamp','from',\
                                        'functionName','input','to', 'isError','hash','blockNumber']]

transactions = transactions.drop_duplicates().reset_index(drop=True)

   # Convert to human date
transactions['unixtime'] = transactions['timeStamp']
transactions['timeStamp'] = transactions['timeStamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))
transactions['unixtime'] = transactions['unixtime'].apply(int)

    # Filter for interval
transactions = transactions[transactions['unixtime'] >= lower_bound]
transactions = transactions[transactions['unixtime'] <= upper_bound]

transactions = transactions.set_index('timeStamp')

    # Excluding aborted transactions
transactions = transactions[transactions['isError'] == '0']


    # Fix strings
transactions['input'] = transactions['input'].apply(str)
transactions['functionName'] = transactions['functionName'].apply(str)

    # Decoding input from bytes32 with contract ABI specs
def decode_input(X, N):
    if 'sizeDelta' in N:
        J = decode_function(FUTETH_ABI, X)[0][2]/10**18
    elif 'marginDelta' in N:
        J = decode_function(FUTETH_ABI, X)[0][2]/10**18
    else : J = None
    
    return J
    
transactions['input'] = transactions.apply(lambda x: decode_input(x.input, x.functionName), axis = 1)

    # Simplify function names for readability
transactions['functionName'] = transactions['functionName'].apply(lambda x: x.split('(')[0])


    #-----------------------------------------
    # Retrieve market stats
    #-----------------------------------------
c = w3.eth.contract(address=perpetualFuturesID, abi=FUTETH_ABI)

 #-----------------------------------------
 # Fund rate sequence
 #-----------------------------------------
subgraph = "https://api.thegraph.com/subgraphs/name/kwenta/optimism-main"

    # Time series of the cumulative funding sequence (needed to compute accrued funding 
    # for a position)
payload_fundingrate = {"query":
    "{fundingRateUpdates ( \
        orderDirection: asc,\
        where: {\
          timestamp_gt: \""+str(lower_bound-1)+"\",\
          timestamp_lt: \""+str(upper_bound+1)+"\",\
          market: \"0xf86048dff23cf130107dfb4e6386f574231a5c65\"}\
      ){\
        timestamp\
        funding\
        sequenceLength}\
    }"}

r = requests.post(subgraph, json=payload_fundingrate)

fund_rate_series = pd.DataFrame.from_dict(r.json()['data']['fundingRateUpdates'])[['timestamp','funding','sequenceLength']]
try:
    while int(fund_rate_series['timestamp'].iloc[-1]) <= upper_bound+1:
        payload_fundingrate = {"query":
        "{fundingRateUpdates ( \
            orderDirection: asc,\
            where: {\
            timestamp_gt: \""+fund_rate_series['timestamp'].iloc[-1]+"\",\
            timestamp_lt: \""+str(upper_bound+1)+"\",\
            market: \"0xf86048dff23cf130107dfb4e6386f574231a5c65\"}\
        ){\
            timestamp\
            funding\
            sequenceLength}\
        }"}

        r = requests.post(subgraph, json=payload_fundingrate)

        fund_rate_series = pd.concat([fund_rate_series, pd.DataFrame.from_dict(r.json()['data']['fundingRateUpdates'])[['timestamp','funding','sequenceLength']]])
except: 
    print('no other data')

fund_rate_series = fund_rate_series[fund_rate_series['timestamp'] <= str(upper_bound)]

fund_rate_series['dateHuman'] = fund_rate_series['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))
fund_rate_series['funding'] = fund_rate_series['funding'].apply(lambda x: int(x)/10**18)

fund_rate_series = fund_rate_series.sort_values(by='timestamp',ascending= True)


    # Prices and all events
ETHPERP_prices = pd.DataFrame(columns = ('DateHuman','dateUnix','price'))
warnings.filterwarnings("ignore")

starting_block = int(transactions['blockNumber'].iloc[0])-1
closing_block = int(transactions['blockNumber'].iloc[-1])+1
n = 10000
firstblock = starting_block+n

payload = "https://api-optimistic.etherscan.io/api?\
module=logs&action=getLogs\
&fromBlock="+str(starting_block)+"\
&toBlock="+str(firstblock)+"\
&address=0xf86048dff23cf130107dfb4e6386f574231a5c65\
&topic0=0x930fd93131df035ac630ef616ad4212af6370377bf327e905c2724cd01d95097\
&apikey=YJJ24VBS3BVWC3MT6DZX2Q7SWFE3UM4R8X"

logs = pd.DataFrame.from_dict(requests.get(payload).json()['result'])

while firstblock <= closing_block:
    payload = "https://api-optimistic.etherscan.io/api?\
module=logs&action=getLogs\
&fromBlock="+str(firstblock)+"\
&toBlock="+str(firstblock+n)+"\
&address=0xf86048dff23cf130107dfb4e6386f574231a5c65\
&topic0=0x930fd93131df035ac630ef616ad4212af6370377bf327e905c2724cd01d95097\
&apikey=YJJ24VBS3BVWC3MT6DZX2Q7SWFE3UM4R8X"
    newL = pd.DataFrame.from_dict(requests.get(payload).json()['result'])
    logs = pd.concat([logs,newL])
    firstblock = firstblock+n


logs = logs.reset_index(drop = True)

def decodeLog(X):
  receipt = w3.eth.get_transaction_receipt(X)
  abi_events = [abi for abi in c.abi if abi["type"] == "event"]
  for event in abi_events:
      # Get event signature components
      name = event["name"]
      inputs = [param["type"] for param in event["inputs"]]
      inputs = ",".join(inputs)
      decoded_logs = c.events[event["name"]]().processReceipt(receipt)

  I = pd.DataFrame(columns=({'lastPrice','size','fundingIndex'}))
  n = len(decoded_logs)
  for i in range(0,n):
    I = pd.concat([I, pd.DataFrame({'lastPrice': [decoded_logs[i]['args']['lastPrice']/10**18],\
                                 'tradeSize': [decoded_logs[i]['args']['tradeSize']/10**18],\
                                 'size': [decoded_logs[i]['args']['size']/10**18],\
                                'fundingIndex': decoded_logs[i]['args']['fundingIndex']})])
  return I

I = pd.DataFrame(columns=({'lastPrice','tradeSize','size','fundingIndex'}))

for i in range(0, len(logs)):

  I = pd.concat([I,decodeLog(logs['transactionHash'].iloc[i])])

I['fundingIndex'] = I['fundingIndex'].apply(str)
P = pd.merge(I, fund_rate_series, left_on = 'fundingIndex', right_on = 'sequenceLength', how ='inner')[['lastPrice','timestamp']]
P = P.drop_duplicates()
P['DateHuman'] = P['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))
P.rename(columns=({'lastPrice': 'price','timestamp':'dateUnix'}),inplace=True)

ETHPERP_prices = pd.concat([P,ETHPERP_prices]).drop_duplicates().reset_index(drop=True)

needed_timepoints = transactions.reset_index(drop=False)
for i in range(0, len(needed_timepoints)):
 try:
    if (needed_timepoints['functionName'].iloc[i] == 'executeNextPriceOrder') or (needed_timepoints['functionName'].iloc[i] == 'submitNextPriceOrderWithTracking'):
        receipt = w3.eth.get_transaction_receipt(needed_timepoints['hash'].iloc[i])
        log = receipt['logs'][-2]
        receipt_event_signature_hex = w3.toHex(log["topics"][0])
        abi_events = [abi for abi in c.abi if abi["type"] == "event"]
        for event in abi_events:
            # Get event signature components
            name = event["name"]
            inputs = [param["type"] for param in event["inputs"]]
            inputs = ",".join(inputs)
            # Hash event signature
            event_signature_text = f"{name}({inputs})"
            event_signature_hex = w3.toHex(w3.keccak(text=event_signature_text))
            # Find match between log's event signature and ABI's event signature
            if event_signature_hex == receipt_event_signature_hex:
                # Decode matching log
                decoded_logs = c.events[event["name"]]().processReceipt(receipt)
        assetPrice = decoded_logs[0]['args']['lastPrice']/10**18
        dateUnix = needed_timepoints['unixtime'].iloc[i]
        T = needed_timepoints['timeStamp'].iloc[i]
        newdata = pd.DataFrame({"DateHuman":[T],"price":[assetPrice], 'dateUnix':[dateUnix]})
        ETHPERP_prices = pd.concat([ETHPERP_prices, newdata])

    else:
        receipt = w3.eth.get_transaction_receipt(needed_timepoints['hash'].iloc[i])
        log = receipt['logs'][-1]
        receipt_event_signature_hex = w3.toHex(log["topics"][0])
        abi_events = [abi for abi in c.abi if abi["type"] == "event"]
        for event in abi_events:
            # Get event signature components
            name = event["name"]
            inputs = [param["type"] for param in event["inputs"]]
            inputs = ",".join(inputs)
            # Hash event signature
            event_signature_text = f"{name}({inputs})"
            event_signature_hex = w3.toHex(w3.keccak(text=event_signature_text))
            # Find match between log's event signature and ABI's event signature
            if event_signature_hex == receipt_event_signature_hex:
                # Decode matching log
                decoded_logs = c.events[event["name"]]().processReceipt(receipt)
        assetPrice = decoded_logs[0]['args']['lastPrice']/10**18
        dateUnix = needed_timepoints['unixtime'].iloc[i]
        T = needed_timepoints['timeStamp'].iloc[i]
        newdata = pd.DataFrame({"DateHuman":[T],"price":[assetPrice], 'dateUnix':[dateUnix]})
        ETHPERP_prices = pd.concat([ETHPERP_prices, newdata])
    
 except: print('Index not working: '+str(i) +'. Event: '+ needed_timepoints['functionName'].iloc[i])

     # Select latest price available per second
V = ETHPERP_prices.groupby(['DateHuman','dateUnix'])['price'].cumcount() + 1
ETHPERP_prices = pd.concat([ETHPERP_prices, V], axis = 1)
ETHPERP_prices.rename(columns=({0:'cumcount'}),inplace=True)
latestprices = ETHPERP_prices.groupby(['DateHuman','dateUnix'])['cumcount'].max().reset_index()
ETHPERP_prices = pd.merge(ETHPERP_prices,latestprices).drop('cumcount', axis = 1)

ETHPERP_prices['dateUnix'] = ETHPERP_prices['dateUnix'].apply(int)
ETHPERP_prices = ETHPERP_prices.sort_values('dateUnix').drop_duplicates().reset_index(drop = True)

 # In case of missing price propagate the previous one
missingPrice = pd.DataFrame(columns = ({'price','dateUnix'}))
for i in range(0, len(ETHPERP_prices)-1):
    n = ETHPERP_prices['dateUnix'].iloc[i] + 1
    while n < ETHPERP_prices['dateUnix'].iloc[i+1]:
        newdate = n.copy()
        repeatedPrice = ETHPERP_prices['price'].iloc[i]
        missingPrice = pd.concat([missingPrice,pd.DataFrame({'price':[repeatedPrice],'dateUnix':[newdate]})])
        n = n+1

missingPrice['DateHuman'] = missingPrice['dateUnix'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))

ETHPERP_prices = pd.concat([ETHPERP_prices, missingPrice]).sort_values('dateUnix').drop_duplicates().reset_index(drop = True)


    # Get list of all Events 
S = pd.merge(I, fund_rate_series, left_on = 'fundingIndex', right_on = 'sequenceLength', how ='inner')[['size','tradeSize','timestamp']]
S['DateHuman'] = S['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))
S = S.sort_values('timestamp').reset_index(drop = True)

    # Get data about the positions under analysis

subgraph = "https://api.thegraph.com/subgraphs/name/kwenta/optimism-main"
positions = transactions[['from','unixtime','input','hash']].drop_duplicates()

positions_metrics = pd.DataFrame(columns = ['account','position', 'pnl','initialMargin','margin','netFunding',\
    'openTimestamp','closeTimestamp','entryPrice','lastPrice','feesPaid', 'id'])

for n in range(0,len(positions)):
    account = positions['from'][n]
    unixtime = str(positions['unixtime'][n])
    size = positions['input'][n]

    payload = {"query":"{futuresPositions (orderBy:timestamp,\
        orderDirection:desc,where:{account:\""+account+"\",\
        \nmarket:\"0xf86048dff23cf130107dfb4e6386f574231a5c65\",\n openTimestamp: "+unixtime+"\n})\
        {pnl\ninitialMargin\nmargin\nnetFunding\nopenTimestamp\
            \ncloseTimestamp\nentryPrice\nlastPrice\nfeesPaid\nid}\n}"}
        
    position = pd.DataFrame.from_dict(requests.post(subgraph, json=payload).json()['data']['futuresPositions'])

    if len(position) > 0:
        position['account'] = account
        position['position'] = size
        position['initialMargin'] = position['initialMargin'].apply(lambda x: int(x)/10**18)
        position['pnl'] = position['pnl'].apply(lambda x: int(x)/10**18)
        position['netFunding'] = position['netFunding'].apply(lambda x: int(x)/10**18)
        position['margin'] = position['margin'].apply(lambda x: int(x)/10**18)
        position['entryPrice'] = position['entryPrice'].apply(lambda x: int(x)/10**18)
        position['lastPrice'] = position['lastPrice'].apply(lambda x: int(x)/10**18)
        position['feesPaid'] = position['feesPaid'].apply(lambda x: int(x)/10**18)

        positions_metrics = pd.concat([positions_metrics, position])

for n in range(0,len(positions)):
    account = positions['from'][n]
    unixtime = str(positions['unixtime'][n])
    size = positions['input'][n]

    payload = {"query":"{futuresPositions (orderBy:timestamp,\
        orderDirection:desc,where:{account:\""+account+"\",\
        \nmarket:\"0xf86048dff23cf130107dfb4e6386f574231a5c65\",\n closeTimestamp: "+unixtime+"\n})\
        {pnl\ninitialMargin\nmargin\nnetFunding\nopenTimestamp\
            \ncloseTimestamp\nentryPrice\nlastPrice\nfeesPaid\nid}\n}"}
        
    position = pd.DataFrame.from_dict(requests.post(subgraph, json=payload).json()['data']['futuresPositions'])

    if len(position) > 0:
        position['account'] = account
        position['position'] = size
        position['initialMargin'] = position['initialMargin'].apply(lambda x: int(x)/10**18)
        position['pnl'] = position['pnl'].apply(lambda x: int(x)/10**18)
        position['netFunding'] = position['netFunding'].apply(lambda x: int(x)/10**18)
        position['margin'] = position['margin'].apply(lambda x: int(x)/10**18)
        position['entryPrice'] = position['entryPrice'].apply(lambda x: int(x)/10**18)
        position['lastPrice'] = position['lastPrice'].apply(lambda x: int(x)/10**18)
        position['feesPaid'] = position['feesPaid'].apply(lambda x: int(x)/10**18)

        positions_metrics = pd.concat([positions_metrics, position])

size = list()
for i in range(0,len(positions_metrics)):
  if positions_metrics['closeTimestamp'].iloc[i] != None:
    payload = {"query":"\
    {futuresTrades( \
        orderBy:timestamp,\
        orderDirection:desc,\
        where:{\
        positionId: \""+ str(positions_metrics['id'].iloc[i]) +"\",\
        timestamp: "+ str(positions_metrics['closeTimestamp'].iloc[i]) +"})\
        { size\
        positionSize\
        timestamp\
        positionId\
        }}"
    }
    try:
        size.append(-int(pd.DataFrame.from_dict(requests.post(subgraph, json=payload).json()['data']['futuresTrades'])['size'])/10**18)
    except:
            payload = {"query":"{futuresTrades(orderBy:timestamp,orderDirection:desc,where:{positionId: \""+ str(positions_metrics['id'].iloc[i]) +"\",\
timestamp: "+ str(positions_metrics['closeTimestamp'].iloc[i]) +"}){size,positionSize,timestamp,positionId}}"}
            size.append(-int(pd.DataFrame.from_dict(requests.post(subgraph, json=payload).json()['data']['futuresTrades'])['size'])/10**18)
  else:
    size.append(None)

positions_metrics['position'] = size

positions_metrics = positions_metrics.sort_values(by='openTimestamp',ascending= False).drop_duplicates()

    # Convert to human date
positions_metrics['openTimestampH'] = positions_metrics['openTimestamp']
positions_metrics['openTimestampH'] = positions_metrics['openTimestampH'].apply(lambda x: datetime.datetime.utcfromtimestamp(int(x)+7200).strftime("%Y-%m-%d %H:%M:%S"))
positions_metrics['closeTimestampH'] = positions_metrics['closeTimestamp']
def convertclosedate(X):
    try: 
        X = datetime.datetime.utcfromtimestamp(int(X)+7200).strftime("%Y-%m-%d %H:%M:%S")
    except: 
        X = None
    return X

positions_metrics['closeTimestampH'] = positions_metrics['closeTimestampH'].apply(convertclosedate)





 # Retrieve the initial skew

if fund_rate_series['timestamp'].iloc[1] != fund_rate_series['timestamp'].iloc[0]:
    i = 0
else:
    i = 1

payload_fundingrate = {"query":
    "{fundingRateUpdates ( \
        orderDirection: asc,\
        where: {\
          sequenceLength:"+ str(int(fund_rate_series['sequenceLength'].iloc[0])-1)+"\
          market: \"0xf86048dff23cf130107dfb4e6386f574231a5c65\"}\
      ){\
        timestamp\
        funding\
        sequenceLength}\
    }"}

r = requests.post(subgraph, json=payload_fundingrate)
tDiff = int(fund_rate_series['timestamp'].iloc[0]) - int(r.json()['data']['fundingRateUpdates'][0]['timestamp'])
Fadd = fund_rate_series['funding'].iloc[0] - float(r.json()['data']['fundingRateUpdates'][0]['funding'])/10**18

# tDiff = int(fund_rate_series['timestamp'].iloc[i+1]) - int(fund_rate_series['timestamp'].iloc[i])
# Fadd = fund_rate_series['funding'].iloc[i+1] - fund_rate_series['funding'].iloc[i]
# ETHPERP_prices = ETHPERP_prices.reset_index()
ETHPERP_prices = ETHPERP_prices.set_index('DateHuman')[['price','dateUnix']]
initPrice = ETHPERP_prices.loc[fund_rate_series['dateHuman'].iloc[i],'price']
 # initPrice = ETHPERP_prices['price'].iloc[0]

marketSkew = -Fadd*wmax*86400/(tDiff*imax*initPrice*initPrice)

T = fund_rate_series['dateHuman'].iloc[i]
t = fund_rate_series['timestamp'].iloc[i]

marketData = pd.DataFrame({"DateHuman":[T],'dateUnix':[t],'skew':[marketSkew], \
                        'imax':[imax], 'wmax':[wmax],'marketID':[perpetualFuturesID]})



 #-----------------------------------------
 # Preparing data for Vadalog and export
 #-----------------------------------------

if not os.path.exists(path_export+'/vadalog_input'):
    os.makedirs(path_export+'/vadalog_input')

path_export_input = path_export+'/vadalog_input'

    # Prepare Events file to be used for fundng rate update -> union all events in one file and add 
    # position size in closePosition events (in case the position was opened before the interval under analysis)

kwenta = transactions.copy().reset_index()

closing = kwenta[kwenta['functionName'] == 'closePositionWithTracking']
closerep = positions_metrics[positions_metrics['account'].isin(kwenta['from'])][['account','position','closeTimestamp']]
closing.loc[:,'unixtime'] = closing['unixtime'].apply(lambda x: int(x))
closerep = closerep[~closerep['closeTimestamp'].isnull()]
closerep.loc[:,'closeTimestamp'] = closerep['closeTimestamp'].apply(lambda x: int(x))

fixclose = pd.merge(closing, closerep, left_on=['from','unixtime'], right_on = ['account','closeTimestamp']).drop(columns =['input','account','closeTimestamp'])
fixclose.rename(columns = {'position':'input'}, inplace=True)
fixclose['input'] = fixclose['input'].apply(lambda x: -x)
kwenta = pd.concat([kwenta[kwenta['functionName'] != 'closePositionWithTracking'],fixclose]).reset_index(drop=True)

kwenta.loc[kwenta['functionName'] == 'transferMargin','input'] = kwenta[kwenta['functionName'] == 'transferMargin']['input'].apply(lambda x: 0)
kwenta.loc[kwenta['functionName'] == 'withdrawAllMargin','input'] = kwenta[kwenta['functionName'] == 'withdrawAllMargin']['input'].apply(lambda x: 0)
kwenta.loc[kwenta['functionName'] == 'submitNextPriceOrderWithTracking','input'] = kwenta[kwenta['functionName'] == 'submitNextPriceOrderWithTracking']['input'].apply(lambda x: 0)

kwenta['input'] = kwenta['input'].apply(lambda x: float(x))
kwenta['unixtime'] = kwenta['unixtime'].apply(lambda x: str(x))
kwenta.loc[kwenta['input'].isnull(),'input'] = 0

S.rename(columns=({'tradeSize':'input','timestamp':'unixtime','DateHuman':'timeStamp'}), inplace=True)
events = pd.concat([S[['timeStamp','input','unixtime']],kwenta[['timeStamp','input','unixtime']]])
events.loc[events['input'].isnull(),'input'] = 0
events['unixtime'] = events['unixtime'].apply(str)
events = events.drop_duplicates().sort_values('unixtime').reset_index(drop=True)
events[['timeStamp','unixtime','input']].to_csv(path_export_input+'/events.csv', index= False)

excl = kwenta[kwenta['functionName'] == 'transferMargin']['from'].drop_duplicates().to_list()
kwenta = kwenta[kwenta['from'].isin(excl)]

kwenta = transactions.copy().reset_index()
transferMargin = kwenta[kwenta['functionName'] == 'transferMargin']
modifyPos = kwenta[kwenta['functionName'] == 'modifyPositionWithTracking']
withdraw = kwenta[kwenta['functionName'] == 'withdrawAllMargin']
closePos = kwenta[kwenta['functionName'] == 'closePositionWithTracking']

transferMargin = transferMargin[['timeStamp','from','input','unixtime']]
modifyPos = modifyPos[['timeStamp','from', 'input','unixtime']]
withdraw = withdraw[['timeStamp', 'from','unixtime']]
closePos = closePos[['timeStamp', 'from','unixtime']]

transferMargin.to_csv(path_export_input+'/transferMargin.csv', sep=',', index=None)
modifyPos.to_csv(path_export_input+'/modifyPos.csv', sep=',', index=None)
withdraw.to_csv(path_export_input+'/withdraw.csv', sep=',', index=None)
closePos.to_csv(path_export_input+'/closePos.csv', sep=',', index=None)
ETHPERP_prices.to_csv(path_export_input+'/prices.csv', sep=',')

cum_events = events[['input','unixtime']].groupby('unixtime').sum().reset_index()

marketData['periodTemporal'] = "@temporal("+ marketData['DateHuman'].iloc[0] +','+ fund_rate_series['dateHuman'].iloc[-1]+")."
marketData['DateHuman'] = events['timeStamp'].iloc[0]
marketData['initialDate'] = "@["+fund_rate_series['dateHuman'].iloc[0] + ',' + fund_rate_series['dateHuman'].iloc[0]+"]."
marketData['initialUnix'] = fund_rate_series['timestamp'].iloc[0]
marketData['skew'] = 'marketSkew('+str(marketData['skew'].iloc[0]+cum_events['input'].iloc[0])+','+ str(fund_rate_series['timestamp'].iloc[0])+')'+marketData['initialDate'].iloc[0]
marketData['time'] = 'time('+ marketData['initialUnix'].iloc[0]+')'+ marketData['initialDate'].iloc[0]
marketData['initialSequence'] = 'fundRateSequenceInit('+str(fund_rate_series['funding'].iloc[0])+')'+marketData['initialDate']

# Excelwriter function
with pd.ExcelWriter(path_export+'/kwentaData.xlsx', engine="openpyxl") as writer:
    
    ETHPERP_prices.to_excel(writer, sheet_name='prices')
    marketData.transpose().to_excel(writer, sheet_name='marketDataInit')
    transactions.drop(columns=(['isError','to'])).to_excel(writer, sheet_name='transactions')
    fund_rate_series.to_excel(writer, sheet_name='fundRateSeries', index = False)
    positions_metrics.to_excel(writer, sheet_name='benchmarkOutput', index = False)

