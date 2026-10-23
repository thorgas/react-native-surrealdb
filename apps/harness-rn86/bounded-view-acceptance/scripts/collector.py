import http.server,json,os,pathlib,subprocess,sys,time
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=False)
serial=os.environ.get('BOUNDED_DEVICE_SERIAL')
if not serial:raise SystemExit('BOUNDED_DEVICE_SERIAL is required')
adb=str(pathlib.Path(os.environ.get('ANDROID_HOME',str(pathlib.Path.home()/'Library/Android/sdk')))/'platform-tools/adb')
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path=='/health' else 404);self.end_headers();self.wfile.write(b'ok')
    def do_POST(self):
        names={'/ready':'ready','/report':'report','/closed':'closed'}
        if self.path not in names:self.send_error(404);return
        try:
            size=int(self.headers.get('content-length','0'))
            if not 0<size<=65536:raise ValueError('bounded report')
            body=json.loads(self.rfile.read(size))
            pid=subprocess.run([adb,'-s',serial,'shell','pidof','com.surrealdbharness.boundedviews20261023'],capture_output=True,text=True,timeout=5).stdout.strip()
            envelope={'hostReceivedAt':time.time(),'kind':names[self.path],'devicePid':pid,'data':body}
            path=out/(names[self.path]+'.json')
            if path.exists():raise ValueError('duplicate phase')
            temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(envelope,indent=2)+'\n');temporary.replace(path)
            self.send_response(200);self.end_headers();self.wfile.write(b'ok')
        except (ValueError,OSError,subprocess.TimeoutExpired):self.send_error(400)
    def log_message(self,*args):pass
http.server.ThreadingHTTPServer(('127.0.0.1',19062),Handler).serve_forever()
