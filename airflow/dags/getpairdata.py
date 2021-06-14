# get past pair data
# todo: react frontend?
# todo: more than 10k transactions (all?)

import sys, time, os, requests

api_key="1WK767J68ZVJVYEZVJHPADXA6YHDTQ56NJ"

addr_cache = "/opt/airflow/dags/pools.txt"
nft_addr = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
vol_tick = 60*15*4 # 15 minutes
to_rate = {"1": 1.0, "3": 0.3, "5": 0.05}
div18 = 1000.0*1000.0*1000.0*1000.0*1000.0*1000.0

# needs addr, start, end, apikey
tokentx="https://api.etherscan.io/api?module=account&action=tokentx&address={}&startblock={}&endblock={}&sort=asc&apikey={}"

# needs timestamp (unix sec), apikey
blocknm="https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={}&closest=before&apikey={}"

def getDataFromBlock(addr, sb):
  start_block=sb
  end_block="999999999"
  resp = requests.get(tokentx.format(addr, start_block, end_block, api_key))
  data = (resp.json())['result']
  return data

# gets to the closest block # for 'num' days ago
def getPastDayBlockNum(num):
  day_secs = 24*60*60*num
  cur_time = int(time.time())
  day_before = cur_time-day_secs

  resp = requests.get(blocknm.format(str(day_before), api_key))
  data = resp.json()
  return data['result']

def transformRaw(rawTx):

    print("tx count: ", str(len(rawTx)))
    last_block = 0
    if len(rawTx) != 0:
        last_block = rawTx[-1]['blockNumber']
    txL = list()

    past_price = 0.0

    tx = 0
    while tx < len(rawTx):
      tx1 = rawTx[tx]

      # one-sided add/ remove
      if tx == len(rawTx)-1 or tx1['hash'] != rawTx[tx+1]['hash']:
          vol = float(tx1['value']) / div18
          if ('ETH' not in tx1['tokenSymbol']) and ('WETH' in tx1['tokenSymbol']):
              vol /= past_price
          if tx1['to'] == nft_addr: # remove vol = eth
              txL.append(["remove", int(tx1['timeStamp']), 0.0, vol])
          else:
              txL.append(["add", int(tx1['timeStamp']), 0.0, vol])
          tx += 1
          continue

      tx2 = rawTx[tx+1]

      # get token values
      tk1val, tk2val = 0.0, 0.0
      if 'ETH' in tx1['tokenSymbol'] or 'WETH' in tx1['tokenSymbol']:
        tk1val = float(tx2['value'])
        tk2val = float(tx1['value'])
      else:
        tk1val = float(tx1['value'])
        tk2val = float(tx2['value'])

      # add or remove?
      if any(nft_addr in x for x in [tx1['to'], tx1['from'], tx2['from'], tx2['to']]):
        vol = (tk1val / div18) / past_price
        vol += (tk2val / div18)
        if any(nft_addr in x for x in [tx1['from'], tx2['from']]):
            txL.append(["add", int(tx1['timeStamp']), 0.0, vol])
        else:
            txL.append(["remove", int(tx1['timeStamp']), 0.0, vol])
        tx += 2
        continue

      # swaps
      pair_val = float(tk1val) / float(tk2val)
      tx_vol = float(tk2val) / (1000.0*1000.0*1000.0*1000.0*1000.0*1000.0)
      txL.append(["swap", int(tx1['timeStamp']), pair_val, tx_vol])
      past_price = pair_val
      tx += 2

    return txL, last_block

def getPools():
    retter = list()
    fo = open(addr_cache, 'r')
    while True:
        line = fo.readline()
        if not line: break
        line2 = fo.readline()
        adder = [line[:-1], line2[:-1]]
        retter.append(adder)
    fo.close()
    return retter
