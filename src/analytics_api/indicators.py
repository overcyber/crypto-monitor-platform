from __future__ import annotations
import math
import statistics
from typing import Iterable
from .models import Candle

INTERVAL_SECONDS = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,"4h":14400,"6h":21600,"12h":43200,"1d":86400}

def _vals(v: Iterable[float]) -> list[float]: return [float(x) for x in v]

def ema_series(values: Iterable[float], period: int) -> list[float]:
    xs=_vals(values)
    if period<=0 or not xs: return []
    a=2/(period+1); out=[xs[0]]
    for x in xs[1:]: out.append(a*x+(1-a)*out[-1])
    return out

def rsi_wilder(values: Iterable[float], period:int=14)->float|None:
    xs=_vals(values)
    if len(xs)<period+1:return None
    gains=[]; losses=[]
    for a,b in zip(xs,xs[1:]):
        d=b-a; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    for g,l in zip(gains[period:],losses[period:]):
        ag=(ag*(period-1)+g)/period; al=(al*(period-1)+l)/period
    if al==0:return 100.0 if ag>0 else 50.0
    rs=ag/al; return 100-100/(1+rs)

def macd(values: Iterable[float],fast:int=12,slow:int=26,signal:int=9)->dict:
    xs=_vals(values)
    if len(xs)<slow:return {"macd":None,"signal":None,"histogram":None}
    f=ema_series(xs,fast); s=ema_series(xs,slow); m=[a-b for a,b in zip(f,s)]; sig=ema_series(m,signal)
    return {"macd":m[-1],"signal":sig[-1],"histogram":m[-1]-sig[-1]}

def bollinger(values:Iterable[float],period:int=20,stddevs:float=2)->dict:
    xs=_vals(values)
    if len(xs)<period:return {"middle":None,"upper":None,"lower":None,"bandwidth_pct":None,"percent_b":None}
    w=xs[-period:]; mid=statistics.fmean(w); sd=statistics.pstdev(w); up=mid+stddevs*sd; lo=mid-stddevs*sd; width=up-lo
    return {"middle":mid,"upper":up,"lower":lo,"bandwidth_pct":width/mid*100 if mid else None,"percent_b":(xs[-1]-lo)/width if width else .5}

def atr_wilder(candles:list[Candle],period:int=14)->float|None:
    if len(candles)<period+1:return None
    trs=[]; prev=candles[0].close
    for c in candles[1:]: trs.append(max(c.high-c.low,abs(c.high-prev),abs(c.low-prev))); prev=c.close
    out=sum(trs[:period])/period
    for tr in trs[period:]: out=(out*(period-1)+tr)/period
    return out

def log_returns(values:Iterable[float])->list[float]:
    xs=_vals(values); return [math.log(b/a) for a,b in zip(xs,xs[1:]) if a>0 and b>0]

def _annual(interval:str)->float|None:
    sec=INTERVAL_SECONDS.get(interval); return 365*24*3600/sec if sec else None

def realized_volatility(values:Iterable[float],interval:str="1h",window:int=20)->float|None:
    xs=_vals(values)
    if len(xs)<window+1 or interval not in INTERVAL_SECONDS:return None
    r=log_returns(xs[-window-1:]); sd=statistics.stdev(r) if len(r)>1 else 0
    return sd*math.sqrt(_annual(interval) or 1)*100

def ewma_volatility(values:Iterable[float],interval:str="1h",span:int=20)->float|None:
    r=log_returns(values)
    if len(r)<max(3,span//2) or interval not in INTERVAL_SECONDS:return None
    a=2/(span+1); mean=r[0]; var=0.0
    for x in r[1:]:
        old=mean; mean=a*x+(1-a)*mean; var=(1-a)*(var+a*(x-old)**2)
    return math.sqrt(max(var,0))*math.sqrt(_annual(interval) or 1)*100

def parkinson(candles:list[Candle],interval:str="1h",window:int=20)->float|None:
    if len(candles)<window:return None
    vals=[math.log(c.high/c.low)**2 for c in candles[-window:] if c.low>0 and c.high>=c.low]
    if not vals:return None
    return math.sqrt(statistics.fmean(vals)/(4*math.log(2))*(_annual(interval) or 1))*100

def garman_klass(candles:list[Candle],interval:str="1h",window:int=20)->float|None:
    if len(candles)<window:return None
    vals=[]
    for c in candles[-window:]:
        if min(c.open,c.high,c.low,c.close)<=0:continue
        hl=math.log(c.high/c.low); co=math.log(c.close/c.open)
        vals.append(.5*hl*hl-(2*math.log(2)-1)*co*co)
    if not vals:return None
    return math.sqrt(max(statistics.fmean(vals),0)*(_annual(interval) or 1))*100

def rogers_satchell(candles:list[Candle],interval:str="1h",window:int=20)->float|None:
    if len(candles)<window:return None
    vals=[]
    for c in candles[-window:]:
        if min(c.open,c.high,c.low,c.close)<=0:continue
        vals.append(math.log(c.high/c.close)*math.log(c.high/c.open)+math.log(c.low/c.close)*math.log(c.low/c.open))
    if not vals:return None
    return math.sqrt(max(statistics.fmean(vals),0)*(_annual(interval) or 1))*100

def volume_anomaly(volumes:Iterable[float],window:int=20)->dict:
    xs=_vals(volumes)
    if len(xs)<window+1:return {"zscore":None,"robust_zscore":None,"ratio":None}
    base=xs[-window-1:-1]; cur=xs[-1]; mean=statistics.fmean(base); sd=statistics.pstdev(base); med=statistics.median(base); mad=statistics.median(abs(x-med) for x in base)
    return {"zscore":(cur-mean)/sd if sd else None,"robust_zscore":.6744897501960817*(cur-med)/mad if mad else None,"ratio":cur/mean if mean else None}

def volatility_suite(candles:list[Candle],interval:str="1h",window:int=20)->dict:
    closes=[c.close for c in candles]
    return {"realized_pct_annualized":realized_volatility(closes,interval,window),"ewma_pct_annualized":ewma_volatility(closes[-max(window*3,window+1):],interval,window),"parkinson_pct_annualized":parkinson(candles,interval,window),"garman_klass_pct_annualized":garman_klass(candles,interval,window),"rogers_satchell_pct_annualized":rogers_satchell(candles,interval,window)}

def pearson(a:Iterable[float],b:Iterable[float])->float|None:
    x=_vals(a); y=_vals(b); n=min(len(x),len(y))
    if n<2:return None
    x=x[-n:];y=y[-n:];mx=statistics.fmean(x);my=statistics.fmean(y);dx=[v-mx for v in x];dy=[v-my for v in y];sx=sum(v*v for v in dx);sy=sum(v*v for v in dy)
    if sx<=1e-24 or sy<=1e-24:return None
    return sum(i*j for i,j in zip(dx,dy))/math.sqrt(sx*sy)

def spearman(a:Iterable[float],b:Iterable[float])->float|None:
    x=_vals(a); y=_vals(b); n=min(len(x),len(y))
    if n<2:return None
    def ranks(vals:list[float])->list[float]:
        order=sorted(range(len(vals)),key=lambda i:vals[i]); out=[0.0]*len(vals); i=0
        while i<len(order):
            j=i+1
            while j<len(order) and vals[order[j]]==vals[order[i]]:j+=1
            r=(i+j-1)/2+1
            for k in range(i,j):out[order[k]]=r
            i=j
        return out
    return pearson(ranks(x[-n:]),ranks(y[-n:]))

def beta(asset:Iterable[float],benchmark:Iterable[float])->float|None:
    y=_vals(asset);x=_vals(benchmark);n=min(len(x),len(y))
    if n<2:return None
    x=x[-n:];y=y[-n:];mx=statistics.fmean(x);my=statistics.fmean(y);var=sum((v-mx)**2 for v in x)
    return sum((i-mx)*(j-my) for i,j in zip(x,y))/var if var>1e-24 else None

def correlation(a:list[Candle],b:list[Candle])->dict:
    am={c.open_time_ms:c.close for c in a}; bm={c.open_time_ms:c.close for c in b}; ts=sorted(set(am)&set(bm)); ar=[];br=[]
    for t0,t1 in zip(ts,ts[1:]):
        if min(am[t0],am[t1],bm[t0],bm[t1])>0: ar.append(math.log(am[t1]/am[t0]));br.append(math.log(bm[t1]/bm[t0]))
    return {"pearson":pearson(ar,br),"spearman":spearman(ar,br),"beta":beta(br,ar),"samples":len(ar),"rolling":{"20":pearson(ar[-20:],br[-20:]) if len(ar)>=20 else None,"60":pearson(ar[-60:],br[-60:]) if len(ar)>=60 else None,"120":pearson(ar[-120:],br[-120:]) if len(ar)>=120 else None}}

def classify_regime(candles:list[Candle],interval:str="1h")->dict:
    closes=[c.close for c in candles]
    if len(closes)<60:
        available=len(closes); required=60
        return {
            "regime":"insufficient_data",
            "reason":"need_at_least_60_candles",
            "available_candles":available,
            "required_candles":required,
            "missing_candles":max(0,required-available),
            "interval":interval,
            "hint":"Use interval=1m while history is accumulating, or wait for 60 bars at the selected interval.",
        }
    e20=ema_series(closes,20)[-1];e50=ema_series(closes,50)[-1];trend=(e20/e50-1)*100 if e50 else 0;v20=realized_volatility(closes,interval,20);v60=realized_volatility(closes,interval,59);vr=v20/v60 if v20 is not None and v60 not in (None,0) else None;rsi=rsi_wilder(closes);hist=macd(closes)["histogram"]
    volatile=vr is not None and vr>=1.35;up=trend>=.6 and (hist is None or hist>=0) and (rsi is None or rsi>=50);down=trend<=-.6 and (hist is None or hist<=0) and (rsi is None or rsi<=50)
    regime="volatile_uptrend" if volatile and up else "volatile_downtrend" if volatile and down else "uptrend" if up else "downtrend" if down else "high_volatility" if volatile else "range"
    return {"regime":regime,"trend_pct":trend,"volatility_ratio":vr,"rsi14":rsi,"macd_histogram":hist}
