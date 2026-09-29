from __future__ import annotations
import statistics
from dataclasses import dataclass, field
from .indicators import log_returns, classify_regime
from .models import Candle

@dataclass(slots=True)
class PageHinkley:
    delta:float=.005; threshold:float=8.; min_instances:int=20
    n:int=field(init=False,default=0); mean:float=field(init=False,default=0.); cum:float=field(init=False,default=0.); min_cum:float=field(init=False,default=0.); max_cum:float=field(init=False,default=0.)
    def update(self,x:float)->str|None:
        self.n+=1;self.mean+=(x-self.mean)/self.n;self.cum+=x-self.mean-self.delta;self.min_cum=min(self.min_cum,self.cum);self.max_cum=max(self.max_cum,self.cum)
        direction=None
        if self.n>=self.min_instances:
            if self.cum-self.min_cum>self.threshold:direction="up"
            elif self.max_cum-self.cum>self.threshold:direction="down"
        if direction:self.n=0;self.mean=self.cum=self.min_cum=self.max_cum=0.
        return direction

def _std(values:list[float])->list[float]:
    if len(values)<2:return values[:]
    m=statistics.fmean(values);sd=statistics.pstdev(values)
    return [(x-m)/sd for x in values] if sd>1e-15 else [0. for _ in values]

def change_points(values:list[float])->dict:
    z=_std(values);ph=PageHinkley();phc=[];pos=neg=0.;cus=[]
    for i,x in enumerate(z):
        d=ph.update(x)
        if d:phc.append({"index":i,"direction":d})
        pos=max(0.,pos+x-.25);neg=min(0.,neg+x+.25)
        if pos>5:cus.append({"index":i,"direction":"up"});pos=neg=0.
        elif neg<-5:cus.append({"index":i,"direction":"down"});pos=neg=0.
    out={"page_hinkley":phc[-10:],"cusum":cus[-10:]}
    try:
        import numpy as np, ruptures as rpt
        if len(values)>=40:
            arr=np.asarray(values,float); sd=float(arr.std()) or 1.;x=((arr-arr.mean())/sd).reshape(-1,1);algo=rpt.Pelt(model="rbf",min_size=10,jump=1).fit(x);out["pelt_rbf"]=[int(v) for v in algo.predict(p=max(2.,3.*__import__('math').log(len(values)))) if int(v)<len(values)]
    except Exception: pass
    try:
        from river import drift
        det=drift.ADWIN(); hits=[]
        for i,x in enumerate(values):
            det.update(x)
            if det.drift_detected:hits.append(i)
        out["adwin"]=hits[-10:]
    except Exception: pass
    return out

def analyze_regime(candles:list[Candle],interval:str)->dict:
    returns=log_returns([c.close for c in candles])
    return {"observed":classify_regime(candles,interval),"change_points":change_points(returns),"note":"Descriptive state/change detection; not a price forecast."}
