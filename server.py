#!/usr/bin/env python3
"""OpenLens live multi-publisher news search — standard library only."""
import concurrent.futures, datetime, email.utils, html, json, os, random, re, threading, time
import urllib.parse, urllib.request, xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=[
("Reuters","reuters.com","International","News agency"),
("Associated Press","apnews.com","US","News agency"),
("BBC News","bbc.com","UK","Public broadcaster"),
("CNN","cnn.com","US","News network"),
("Fox News","foxnews.com","US","News network"),
("NBC News","nbcnews.com","US","News network"),
("CBS News","cbsnews.com","US","News network"),
("ABC News US","abcnews.go.com","US","News network"),
("The New York Times","nytimes.com","US","Newspaper"),
("The Washington Post","washingtonpost.com","US","Newspaper"),
("The Wall Street Journal","wsj.com","US","Newspaper"),
("Bloomberg","bloomberg.com","US","Business news"),
("CNBC","cnbc.com","US","Business news"),
("Politico","politico.com","US","Politics news"),
("The Guardian","theguardian.com","UK","Newspaper"),
("Financial Times","ft.com","UK","Business news"),
("Sky News","news.sky.com","UK","News network"),
("The Telegraph","telegraph.co.uk","UK","Newspaper"),
("The Independent","independent.co.uk","UK","Newspaper"),
("France 24","france24.com","France","Public broadcaster"),
("Le Monde","lemonde.fr","France","Newspaper"),
("Le Figaro","lefigaro.fr","France","Newspaper"),
("DW","dw.com","Germany","Public broadcaster"),
("Al Jazeera","aljazeera.com","Qatar","News network"),
("Euronews","euronews.com","Europe","News network"),
("NPR","npr.org","US","Public media"),
("ABC News Australia","abc.net.au","Australia","Public broadcaster"),
("The Straits Times","straitstimes.com","Singapore","Newspaper"),
("NHK World","www3.nhk.or.jp","Japan","Public broadcaster"),
("The Times of India","timesofindia.indiatimes.com","India","Newspaper"),
("The Hindu","thehindu.com","India","Newspaper"),
("CBC News","cbc.ca","Canada","Public broadcaster")]
CACHE={}; LOCK=threading.Lock(); SLOTS=threading.BoundedSemaphore(3)
def clean(text):
    return re.sub(r"\s+"," ",html.unescape(re.sub(r"<[^>]+>"," ",text or ""))).strip()
def feed(source,query,days):
    name,domain,country,kind=source
    q=urllib.parse.urlencode({"q":f"{query} site:{domain} when:{days}d","hl":"en-US","gl":"US","ceid":"US:en"})
    url="https://news.google.com/rss/search?"+q
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 OpenLensEducational/1.0"})
    with urllib.request.urlopen(req,timeout=7) as resp: raw=resp.read(1200000)
    root=ET.fromstring(raw); out=[]
    for item in root.findall("./channel/item")[:7]:
        title=clean(item.findtext("title")); link=(item.findtext("link") or "").strip()
        if not title or not link.startswith("https://"):continue
        try: date=email.utils.parsedate_to_datetime(item.findtext("pubDate")).isoformat()
        except Exception:date=None
        out.append({"title":title,"url":link,"date":date,"source":name,"domain":domain,"country":country,"type":kind})
    return out
def search(query,days,limit):
    key=(query.casefold(),days,limit); now=time.time()
    with LOCK:
        cached=CACHE.get(key)
        if cached and now-cached[0]<180:return cached[1]
    results=[]; errors=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        futures=[pool.submit(feed,s,query,days) for s in SOURCES]
        for fut in concurrent.futures.as_completed(futures):
            try: results.extend(fut.result())
            except Exception:errors+=1
    buckets={s[1]:[] for s in SOURCES}; seen=set()
    for a in sorted(results,key=lambda x:x.get("date") or "",reverse=True):
        k=(a["domain"],re.sub("[^a-z0-9]","",a["title"].lower())[:70])
        if k not in seen: seen.add(k);buckets[a["domain"]].append(a)
    rotated=SOURCES[:];random.shuffle(rotated);out=[]
    for i in range(max((len(b) for b in buckets.values()),default=0)):
        for src in rotated:
            b=buckets[src[1]]
            if len(b)>i:out.append(b[i])
    payload={"articles":out[:limit],"query":query,"publishers_checked":len(SOURCES),"feed_errors":errors,"total":len(out),"source_count":len({a["domain"] for a in out}),"live":True,"note":"Articles retrieved from public Google News RSS; editorial perspectives and claims have not been independently verified."}
    with LOCK:
        if len(CACHE)>100:CACHE.clear()
        CACHE[key]=(time.time(),payload)
    return payload
class Handler(BaseHTTPRequestHandler):
    def json(self,value,status=200):
        b=json.dumps(value,ensure_ascii=False).encode("utf-8");self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Cache-Control","no-store")
        self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b)
    def do_GET(self):
        url=urllib.parse.urlsplit(self.path);path=url.path
        if path=="/api/health":return self.json({"ok":True,"service":"OpenLens","publishers":len(SOURCES)})
        if path=="/api/publishers":return self.json({"publishers":[{"name":s[0],"domain":s[1],"country":s[2],"type":s[3]} for s in SOURCES]})
        if path=="/api/search":
            params=urllib.parse.parse_qs(url.query);q=(params.get("q",[""])[0] or "").strip()
            try: days=int(params.get("days",["30"])[0]);limit=int(params.get("limit",["48"])[0])
            except ValueError:return self.json({"error":"Invalid search options"},400)
            if not q or len(q)>130 or days not in (7,30,90) or not 1<=limit<=60:return self.json({"error":"Invalid search parameters"},400)
            if not SLOTS.acquire(blocking=False):return self.json({"error":"Server busy, retry"},429)
            try:return self.json(search(q,days,limit))
            except Exception:return self.json({"error":"News search currently unavailable"},503)
            finally:SLOTS.release()
        if path not in ("/","/index.html","/favicon.svg"):return self.send_error(404)
        f=ROOT/("index.html" if path in ("/","/index.html") else "favicon.svg")
        if not f.is_file():return self.send_error(404)
        data=f.read_bytes();self.send_response(200);self.send_header("Content-Type","image/svg+xml" if f.suffix==".svg" else "text/html; charset=utf-8")
        self.send_header("Content-Length",str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,fmt,*args):print(fmt%args,flush=True)
if __name__=="__main__":
    port=int(os.environ.get("PORT","8000"))
    print(f"OpenLens running on port {port}",flush=True)
    ThreadingHTTPServer(("0.0.0.0",port),Handler).serve_forever()
