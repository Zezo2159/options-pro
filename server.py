import json,os,ssl,certifi,urllib.request,urllib.error
from http.server import HTTPServer,BaseHTTPRequestHandler
API_KEY=open(os.path.expanduser('~/options-pro/api_key.txt')).read().strip()
CTX=ssl.create_default_context(cafile=certifi.where())
class H(BaseHTTPRequestHandler):
    def log_message(self,*a):pass
    def cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type')
    def do_OPTIONS(self):self.send_response(200);self.cors();self.end_headers()
    def do_GET(self):
        f='/Applications/OptionsPro.app/Contents/Resources/options_pro_ultra.html'
        d=open(f,'rb').read()
        self.send_response(200);self.send_header('Content-Type','text/html');self.cors();self.end_headers();self.wfile.write(d)
    def do_POST(self):
        n=int(self.headers.get('Content-Length',0));b=self.rfile.read(n)
        r=urllib.request.Request('https://api.anthropic.com/v1/messages',data=b,
            headers={'Content-Type':'application/json','x-api-key':API_KEY,'anthropic-version':'2023-06-01'},method='POST')
        try:
            with urllib.request.urlopen(r,timeout=60,context=CTX) as res:
                rb=res.read();self.send_response(res.status);self.send_header('Content-Type','application/json');self.cors();self.end_headers();self.wfile.write(rb)
                print('OK')
        except urllib.error.HTTPError as e:
            eb=e.read();print('ERR',e.code,eb[:200]);self.send_response(e.code);self.send_header('Content-Type','application/json');self.cors();self.end_headers();self.wfile.write(eb)
print('Server running - keep this open while using the Mac app')
HTTPServer(('localhost',5010),H).serve_forever()
