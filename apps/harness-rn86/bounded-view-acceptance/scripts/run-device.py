import json,os,pathlib,socket,subprocess,threading,time,urllib.request,uuid,signal
from device_ownership import snapshot, create_mappings, restore
from process_cleanup import stop_owned_group, settle_port, persist_cleanup
serial=os.environ.get('BOUNDED_DEVICE_SERIAL')
if not serial:raise SystemExit('BOUNDED_DEVICE_SERIAL must select the approved device')
root=pathlib.Path(__file__).resolve().parents[4];base=root/'output/bounded-view-android-20261023';run=base/('device-run-'+uuid.uuid4().hex[:10]);run.mkdir(parents=True);receiver=run/'collector'
adb=str(pathlib.Path(os.environ.get('ANDROID_HOME',str(pathlib.Path.home()/'Library/Android/sdk')))/'platform-tools/adb');package='com.surrealdbharness.boundedviews20261023';receipt={'serial':serial,'package':package,'runDirectory':str(run),'startedAt':time.time()}
def device(*args):return subprocess.run([adb,'-s',serial,*args],capture_output=True,text=True,timeout=15)
for port in [19061,19062]:
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1',port))==0:raise SystemExit('Owned local port occupied')
ids=subprocess.check_output([adb,'devices'],text=True).splitlines()[1:]
authorized=[row.split()[0] for row in ids if row.strip() and len(row.split())>1 and row.split()[1]=='device']
if authorized!=[serial]:raise SystemExit('Exact sole authorized Pixel required; refusing ambiguous harness model selection')
baseline=snapshot(device);receipt['deviceBaseline']=baseline;created=[]
(base/'current-device-run.json').write_text(json.dumps(receipt,indent=2)+'\n')
collector_log=open(run/'collector.log','wb');collector=subprocess.Popen(['/usr/bin/python3',str(pathlib.Path(__file__).with_name('collector.py')),str(receiver)],stdout=collector_log,stderr=subprocess.STDOUT)
stop=threading.Event()
def monitor():
    with open(run/'process-alive.jsonl','w') as log:
        while not stop.is_set():
            result=device('shell','pidof',package);log.write(json.dumps({'at':time.time(),'pid':result.stdout.strip(),'exit':result.returncode})+'\n');log.flush();stop.wait(.5)
thread=threading.Thread(target=monitor,daemon=True)
try:
    for _ in range(100):
        if collector.poll() is not None:raise RuntimeError('Receiver exited')
        try:
            with urllib.request.urlopen('http://127.0.0.1:19062/health',timeout=1) as r:
                if r.status==200:break
        except OSError:time.sleep(.1)
    else:raise RuntimeError('Receiver unavailable')
    create_mappings(device,created);thread.start()
    env=os.environ.copy();env['PATH']=os.environ['PATH'];env['ANDROID_HOME']=str(pathlib.Path(adb).parents[1]);env['ANDROID_SERIAL']=serial;env['BOUNDED_CAPTURE_DIRECTORY']=str(run/'metro-capture')
    with open(run/'harness.log','wb') as log:
        proc=subprocess.Popen(['../node_modules/.bin/react-native-harness','--harnessRunner','bounded-android','--json','--outputFile',str(run/'jest-results.json')],cwd=root/'apps/harness-rn86/bounded-view-acceptance',env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try: proc.wait(timeout=600)
        finally:
            try: stop_owned_group(proc)
            except Exception as error:
                receipt.setdefault('cleanupErrors', []).append(f'owned harness group: {error}')
                raise
    receipt['harnessExit']=proc.returncode
finally:
    def stop_monitor():
        stop.set()
        if thread.is_alive(): thread.join(timeout=20)
        if thread.is_alive(): raise RuntimeError('PID monitor did not stop')
        return True
    def stop_collector():
        collector.terminate()
        try: collector.wait(timeout=10)
        except subprocess.TimeoutExpired: collector.kill();collector.wait(timeout=5)
        collector_log.close()
        return collector.returncode
    def restore_device():
        errors = restore(device, baseline, created)
        receipt.setdefault('cleanupErrors', []).extend(errors)
        return not errors
    def read_reverse():
        result = device('reverse', '--list');result.check_returncode();return result.stdout
    def read_setting():
        result = device('shell','settings','get','global','hide_error_dialogs');result.check_returncode()
        return result.stdout.strip()==baseline['hideErrorDialogs']
    receipt['createdReversePorts']=created
    persist_cleanup(receipt, run/'receipt.json', [
        ('monitorStopped', stop_monitor),
        ('collectorExit', stop_collector),
        ('deviceRestored', restore_device),
        ('reverseAfterCleanup', read_reverse),
        ('hideErrorDialogsRestored', read_setting),
        ('collectorPortClosed', lambda: settle_port(19062)),
        ('metroPortClosed', lambda: settle_port(19061)),
    ])
if receipt.get('harnessExit')!=0 or not receipt.get('collectorPortClosed') or not receipt.get('metroPortClosed') or not receipt.get('hideErrorDialogsRestored') or receipt['cleanupErrors']:raise SystemExit(1)
