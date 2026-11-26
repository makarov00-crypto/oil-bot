import argparse, sys, json, runpy
from types import SimpleNamespace
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
parser = argparse.ArgumentParser(description='Offline comparison of calendar and candle-count regime gates; no orders.')
parser.add_argument('--candles', required=True)
parser.add_argument('--baseline-core', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
import pandas as pd
import bot_oil_main as m
from strategies.reversal_core import classify_multiday_direction
old = runpy.run_path(args.baseline_core)['classify_multiday_direction']
data = json.loads(Path(args.candles).read_text())
cfg = SimpleNamespace()
events = []
for symbol, payload in data.items():
    raw = pd.DataFrame(payload['candles'])
    raw['time'] = pd.to_datetime(raw['time'], utc=True)
    raw = raw.sort_values('time').reset_index(drop=True)
    inst = m.InstrumentConfig(**payload['instrument'])
    previous = None
    for i in range(100, len(raw)):
        end = raw.iloc[i]['time'] + pd.Timedelta(hours=1)
        if end < pd.Timestamp('2026-11-01', tz='Europe/Moscow'):
            continue
        frame = raw.iloc[:i+1]
        frame = frame[frame['time'] >= end-pd.Timedelta(hours=241)]
        df = m.add_indicators(frame, include_ema200=False)
        signal, reason, strategy = m.evaluate_signal(df, cfg, inst, '')
        if signal == 'HOLD':
            previous = None
            continue
        a = old(df, signal); b = classify_multiday_direction(df, signal)
        aok = a.get('regime') in {'trend','emerging'}
        bok = b.get('regime') in {'trend','emerging'}
        key = (signal, aok, bok)
        if key == previous:
            continue
        previous = key
        if end.tz_convert('Europe/Moscow').hour < 9:
            continue
        row = dict(symbol=symbol, at=end.isoformat(), signal=signal, price=float(df.iloc[-1]['close']), old=a, new=b, old_allowed=aok, new_allowed=bok, reason=reason)
        sign = 1 if signal=='LONG' else -1
        for hours in [4,8]:
            target=end+pd.Timedelta(hours=hours)
            future=raw[(raw['time']+pd.Timedelta(hours=1)>=target)&(raw['time']+pd.Timedelta(hours=1)<=target+pd.Timedelta(hours=1))]
            if len(future):
                row[f'move_{hours}h_atr']=round(sign*(float(future.iloc[0]['close'])-row['price'])/float(df.iloc[-1]['atr']),3)
        events.append(row)
    print(symbol, 'done', flush=True)
Path(args.output).write_text(json.dumps(events,ensure_ascii=False,indent=2))
for group, rows in [('prior', [x for x in events if x['at']<'2026-11-25']), ('recent',[x for x in events if x['at']>='2026-11-25'])]:
    for typ, subset in [('new',[x for x in rows if x['new_allowed'] and not x['old_allowed']]),('removed',[x for x in rows if x['old_allowed'] and not x['new_allowed']])]:
        vals=[x['move_4h_atr'] for x in subset if 'move_4h_atr' in x]
        print(group,typ,'episodes',len(subset),'4h_samples',len(vals),'positive',sum(v>0 for v in vals),'mean_atr',round(sum(vals)/len(vals),3) if vals else None)
        for x in subset: print(json.dumps({k:x[k] for k in ['symbol','at','signal','price','move_4h_atr','move_8h_atr'] if k in x}))
print('BR25',json.dumps([x for x in events if x['symbol']=='BRZ6' and x['at'].startswith('2026-11-25')],ensure_ascii=False))
