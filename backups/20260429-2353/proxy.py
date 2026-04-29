#!/usr/bin/env python3
"""Options Pro Ultra v6 Proxy — Port 5010"""
import http.server, json, os, ssl, urllib.request, urllib.parse, urllib.error
from pathlib import Path
from datetime import datetime

PORT = 5010
BASE = Path("/Applications/OptionsPro.app/Contents/Resources")
HTML = BASE / "options_pro_ultra.html"
KEY_FILE = BASE / "api_key.txt"
JOURNAL = Path.home() / "Desktop" / "autotrade_journal.csv"

def get_key():
    try: return KEY_FILE.read_text().strip()
    except: return ""

def fetch(url, method="GET", headers=None, body=None, timeout=25, verify=True):
    req = urllib.request.Request(url, method=method)
    if headers:
        for k,v in headers.items(): req.add_header(k,v)
    if body: req.data = json.dumps(body).encode() if isinstance(body,dict) else body
    ctx = None
    if not verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            d = r.read()
            try: return json.loads(d)
            except: return {"raw": d.decode("utf-8","replace")}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detail": e.read().decode("utf-8","replace")[:500]}
    except Exception as e:
        return {"error": str(e)}

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self,f,*a): print(f"[{datetime.now():%H:%M:%S}] {a[0] if a else f}")
    def _json(self,d,s=200):
        r=json.dumps(d).encode()
        self.send_response(s);self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*");self.send_header("Content-Length",len(r))
        self.end_headers();self.wfile.write(r)
    def _body(self):
        n=int(self.headers.get("Content-Length",0))
        return json.loads(self.rfile.read(n)) if n else {}
    def _resolve_trades(self, trades):
        # Find closed ticker+strike combos and remove their matching open entries
        closed_keys=set()
        for t in trades:
            if t['status']=='closed':
                closed_keys.add(f"{t['ticker']}-{t['strike']}")
        # Keep: open trades not closed, plus closed trades (for history)
        result=[]
        for t in trades:
            if t['status']=='open' and f"{t['ticker']}-{t['strike']}" in closed_keys:
                continue  # This open was later closed
            result.append(t)
        return result
    def do_OPTIONS(self):
        self.send_response(204)
        for h in["Access-Control-Allow-Origin","Access-Control-Allow-Methods","Access-Control-Allow-Headers"]:
            self.send_header(h,"*")
        self.end_headers()
    def do_GET(self):
        p=self.path.split("?")[0];q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if p in("/","/index.html"):
            try:
                h=HTML.read_bytes();self.send_response(200)
                self.send_header("Content-Type","text/html; charset=utf-8")
                self.send_header("Content-Length",len(h));self.end_headers();self.wfile.write(h)
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p=="/api-key": self._json({"key":get_key()});return
        if p=="/api/journal":
            try:
                if not JOURNAL.exists(): self._json({"trades":[]});return
                lines=JOURNAL.read_text().strip().split('\n')
                if len(lines)<2: self._json({"trades":[]});return
                # Auto-detect: if first line looks like a header (contains letters in most fields)
                first_cols=lines[0].split(',')
                has_header=sum(1 for c in first_cols[:6] if any(ch.isalpha() for ch in c.strip()))>=4
                start=1 if has_header else 0
                # Build header map if present
                hmap={}
                if has_header:
                    for i,h in enumerate(first_cols):
                        hmap[h.strip().lower().replace(' ','_')]=i
                trades=[]
                for line in lines[start:]:
                    cols=[c.strip() for c in line.split(',')]
                    if len(cols)<10: continue
                    # Try header-based mapping first, then known autotrade positional format:
                    # pos 0:datetime, 1:status, 2:ticker, 3:type, 4:strike, 5:expiry, 6:qty, 7:premium, 8:delta, 9:iv, 10:dte, 11:order_status, 12:pnl, 13+:notes
                    def g(names,pos):
                        if hmap:
                            for n in names:
                                if n in hmap and hmap[n]<len(cols): return cols[hmap[n]]
                        return cols[pos] if pos<len(cols) else ''
                    status_val=g(['action'],1).lower().strip()
                    # Map action values: OPEN -> open, CLOSE_PROFIT/CLOSE -> closed
                    if status_val in ('close_profit','close','closed','close_loss','close_roll'):
                        status_val='closed'
                    elif status_val=='open':
                        status_val='open'
                    ticker_val=g(['ticker','symbol'],2).upper().strip()
                    if not ticker_val: continue
                    try: strike_f=float(g(['strike'],4))
                    except: strike_f=0
                    try: premium_f=float(g(['premium','credit'],7))
                    except: premium_f=0
                    try: delta_f=float(g(['delta'],8))
                    except: delta_f=0
                    iv_raw=g(['iv'],9).replace('%','')
                    try: iv_f=float(iv_raw)
                    except: iv_f=0
                    try: dte_i=int(g(['dte'],10))
                    except: dte_i=0
                    try: pnl_f=float(g(['pnl'],12))
                    except: pnl_f=0
                    trades.append({
                        "date":g(['date','datetime','timestamp'],0),
                        "status":status_val,
                        "ticker":ticker_val,
                        "strategy":g(['type','strategy'],3),
                        "strike":strike_f,
                        "expiry":g(['expiry','exp'],5),
                        "contracts":int(g(['qty','contracts'],6) or 1),
                        "premium":premium_f,
                        "delta":delta_f,
                        "iv":iv_f,
                        "dte":dte_i,
                        "pnl":pnl_f,
                        "notes":','.join(cols[13:]) if len(cols)>13 else ''
                    })
                self._json({"trades":self._resolve_trades(trades)})
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p=="/api/yahoo":
            t=q.get("ticker",["SPY"])[0];r=q.get("range",["1y"])[0]
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range={r}&interval=1d&includePrePost=false"
            self._json(fetch(url,headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}))
            return
        if p.startswith("/api/ibkr/"):
            route=p.replace("/api/ibkr/","")
            try:
                if route=="status":
                    r=fetch("https://localhost:5000/v1/api/iserver/auth/status",method="POST",verify=False)
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        if isinstance(a,dict) and "accounts" in a: r["accounts"]=a["accounts"]
                    except: pass
                    self._json(r);return
                aid=q.get("accountId",[None])[0]
                if not aid:
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                    except: pass
                if route=="positions": self._json(fetch(f"https://localhost:5000/v1/api/portfolio/{aid}/positions/0",verify=False));return
                if route=="account": self._json(fetch(f"https://localhost:5000/v1/api/portfolio/{aid}/summary",verify=False));return
                if route=="orders":
                    r=fetch("https://localhost:5000/v1/api/iserver/account/orders",verify=False)
                    self._json(r.get("orders",[]) if isinstance(r,dict) else r if isinstance(r,list) else []);return
                if route=="trades": self._json(fetch("https://localhost:5000/v1/api/iserver/account/trades",verify=False));return
            except Exception as e: self._json({"error":str(e)},500);return
        self._json({"error":"Not found"},404)
    def do_POST(self):
        p=self.path.split("?")[0];b=self._body()
        if p=="/api/claude":
            k=get_key()
            if not k: self._json({"error":{"message":"No API key in api_key.txt"}},401);return
            req=urllib.request.Request("https://api.anthropic.com/v1/messages",method="POST")
            for h,v in{"Content-Type":"application/json","x-api-key":k,"anthropic-version":"2023-06-01"}.items():
                req.add_header(h,v)
            req.data=json.dumps(b).encode()
            try:
                with urllib.request.urlopen(req,timeout=60) as r:
                    d=r.read();self.send_response(200)
                    self.send_header("Content-Type","application/json")
                    self.send_header("Access-Control-Allow-Origin","*")
                    self.send_header("Content-Length",len(d));self.end_headers();self.wfile.write(d)
            except urllib.error.HTTPError as e:
                self._json({"error":{"message":f"API {e.code}: {e.read().decode()[:300]}"}},e.code)
            except Exception as e: self._json({"error":{"message":str(e)}},500)
            return
        if p=="/api/notify":
            os.system(f'''osascript -e 'display notification "{b.get("body","")}" with title "{b.get("title","Options Pro")}"' ''')
            self._json({"ok":True});return
        if p=="/api/email":
            try:
                import smtplib;from email.mime.text import MIMEText;from email.mime.multipart import MIMEMultipart
                msg=MIMEMultipart("alternative");msg["Subject"]=b.get("subject","Options Pro Test")
                msg["From"]=b.get("smtp_user","");msg["To"]=b.get("to","")
                html="<h2>✅ Test OK</h2>" if b.get("type")=="test" else f"<h2>🌅 Briefing</h2><p>{b.get('regime','')}</p><p>{b.get('opps','')}</p>"
                msg.attach(MIMEText(html,"html"))
                with smtplib.SMTP(b.get("smtp_host","smtp.gmail.com"),int(b.get("smtp_port",587))) as s:
                    s.ehlo();s.starttls();s.login(b["smtp_user"],b["smtp_pass"]);s.sendmail(b["smtp_user"],b["to"],msg.as_string())
                self._json({"ok":True})
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p.startswith("/api/ibkr/"):
            route=p.replace("/api/ibkr/","")
            if route=="order":
                aid=b.get("accountId","")
                if not aid:
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                    except: pass
                r=fetch(f"https://localhost:5000/v1/api/iserver/account/{aid}/orders",method="POST",
                    headers={"Content-Type":"application/json"},body={"orders":[{
                        "acctId":aid,"orderType":"LMT","side":"SELL",
                        "quantity":b.get("qty",1),"price":b.get("limit",0),"tif":"DAY"
                    }]},verify=False)
                self._json(r);return
        self._json({"error":"Not found"},404)
    def do_DELETE(self):
        p=self.path.split("?")[0]
        if "/api/ibkr/order/" in p:
            oid=p.split("/")[-1]
            try:
                a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                self._json(fetch(f"https://localhost:5000/v1/api/iserver/account/{aid}/order/{oid}",method="DELETE",verify=False))
            except Exception as e: self._json({"error":str(e)},500)
            return
        self._json({"error":"Not found"},404)

if __name__=="__main__":
    print(f"\n🚀 Options Pro Ultra v6 Proxy\n   http://localhost:{PORT}\n   HTML: {HTML}\n   Key: {'✓' if get_key() else '⚠ missing'}\n")
    http.server.HTTPServer(("",PORT),H).serve_forever()
