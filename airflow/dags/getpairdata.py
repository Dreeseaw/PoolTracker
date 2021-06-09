# get past pair data
# todo: react frontend?
# todo: more than 10k transactions (all?)

import sys, time, os, requests
import matplotlib.pyplot as mpl
import matplotlib as matpl

api_key="1WK767J68ZVJVYEZVJHPADXA6YHDTQ56NJ"

nft_addr = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
addr_cache = "pools.txt"
vol_tick = 60*15*4 # 15 minutes
to_rate = {"1": 1.0, "3": 0.3, "5": 0.05}

# needs addr, start, end, apikey
tokentx="https://api.etherscan.io/api?module=account&action=tokentx&address={}&startblock={}&endblock={}&sort=asc&apikey={}"

# needs timestamp (unix sec), apikey
blocknm="https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={}&closest=before&apikey={}"

# needs addr
uniswap="https://info.uniswap.org/#/pools/{}"

def getPastMinuteData(addr):
  tot_secs = 60
  cur_time = int(time.time())
  min_before = cur_time-tot_secs

  resp = requests.get(blocknm.format(str(min_before), api_key))
  data = resp.json()

  start_block=str(data['result'])
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

# return list of swap times and respective prices/ vols
def getPastTokenTx(addr, days):
  start_block=str(getPastDayBlockNum(days))
  end_block="999999999"

  resp = requests.get(tokentx.format(addr, start_block, end_block, api_key))
  data = (resp.json())['result']

  print("tx count: ", str(len(data)))
  swap_times, swap_price, swap_vols = list(), list(), list()
  start_time_pos = 0

  while any(nft_addr in x for x in [data[start_time_pos]['from'], data[start_time_pos]['to'], data[start_time_pos+1]['from'], data[start_time_pos+1]['to']]):
    start_time_pos += 1
  start_time = int(data[start_time_pos]['timeStamp'])

  past_price = 0

  tx = start_time_pos
  while tx < len(data):
    tx1 = data[tx]
    tx2 = data[tx+1]

    if any(nft_addr in x for x in [tx1['from'], tx1['to'], tx2['from'], tx2['to']]):
      tx += 1
      continue

    tk1val, tk2val = "",""
    if 'ETH' in tx1['tokenSymbol'] or 'WETH' in tx1['tokenSymbol']:
      tk1val = tx2['value']
      tk2val = tx1['value']
    else:
      tk1val = tx1['value']
      tk2val = tx2['value']

    pair_val = float(tk1val) / float(tk2val)
    tx_vol = float(tk2val) / (1000.0*1000.0*1000.0*1000.0*1000.0*1000.0)

    if past_price != 0 and abs(pair_val-past_price)/past_price > 0.05:
      tx += 2
      continue
    else: past_price = pair_val

    swap_times.append(int(tx1['timeStamp'])-start_time)
    swap_price.append(pair_val)
    swap_vols.append(tx_vol)
    tx += 2

  return swap_times, swap_price, swap_vols

# shows charts given lists of times w/ correlated prices/vols
def showChart(swap_times, swap_prices, swap_vols):
  cur_time_pos = 0
  cur_time = 0
  cur_price = swap_prices[0]

  zero_times = [x for x in range(0,swap_times[-1])]
  zero_price = [0 for _ in range(0,swap_times[-1])]
  zero_vols  = [0 for _ in range(0,swap_times[-1])]
  tick_vols  = [0 for _ in range(0,(swap_times[-1] // vol_tick)+1)]

  for zp in range(0, len(swap_times)-1):
    ender = swap_times[cur_time_pos+1]
    tick_vols[swap_times[zp] // vol_tick] += swap_vols[zp]
    for zero_pos in range(cur_time, ender):
      zero_price[zero_pos] = swap_prices[zp]
    cur_time = ender
    cur_time_pos += 1

  # tick_vols -> zero_vols
  for zp in range(0, len(zero_vols)):
    zero_vols[zp] = tick_vols[zp // vol_tick]

  matpl.use('tkagg')
  mpl.figure(1)
  mpl.subplot(211)
  mpl.plot(zero_times, zero_price)
  mpl.ylabel('Pair Price ')
  mpl.subplot(212)
  mpl.plot(zero_times, zero_vols)
  mpl.ylabel('Pair Vol')
  mpl.show()

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

if __name__=="__main__":
  days = int(sys.argv[1])
  addr = str(sys.argv[2])

  if "0x" not in addr:
    fo = open(addr_cache, 'r')
    while True:
      line = fo.readline()
      if not line: break
      if line[:-1] == addr:
        addr = str(fo.readline())
        addr = addr[:-1]
        print(addr)
        break
  if "0x" not in addr:
    print("Given name has no saved address")
    os.exit(1)
  swap_times, swap_prices, sv = getPastTokenTx(addr, days)
  showChart(swap_times, swap_prices, sv)
