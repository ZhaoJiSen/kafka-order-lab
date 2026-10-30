#!/usr/bin/env python3
"""通过真实 PHP、Kafka、Java 验证双向消息，要求 Demo 已启动。"""
import http.client,json,os,subprocess,time,uuid,urllib.request,urllib.error
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
PHP='http://127.0.0.1:'+os.getenv('PHP_PORT','18080')
JAVA='http://127.0.0.1:'+os.getenv('JAVA_PORT','18081')
checks=[]
def check(value,label):
    if not value:raise AssertionError(label)
    checks.append(label);print('PASS',label,flush=True)
def api(path,body=None,key=None,base=PHP):
    headers={'Accept':'application/json'}
    if key:headers['Idempotency-Key']=key
    data=json.dumps(body).encode() if body is not None else None
    if data is not None:headers['Content-Type']='application/json'
    req=urllib.request.Request(base+path,data=data,headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=10) as r:return r.status,json.load(r)
    except urllib.error.HTTPError as e:
        raw=e.read().decode('utf-8',errors='replace')
        try:body=json.loads(raw)
        except json.JSONDecodeError:body={'message':raw}
        return e.code,body
def compose(*args):
    p=subprocess.run(['docker','compose',*args],cwd=ROOT,text=True,capture_output=True,timeout=90)
    if p.returncode:raise RuntimeError(p.stdout+p.stderr)
    return p.stdout

def wait(fn,label,timeout=70):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            result=fn()
            if result:return result
        except (urllib.error.URLError,TimeoutError,ConnectionError,http.client.HTTPException,json.JSONDecodeError):pass
        time.sleep(.5)
    raise AssertionError('Timeout: '+label)
def order(id):return api('/api/orders/'+id)[1]
def final(id,status):return wait(lambda:(x if (x:=order(id))['status']==status else None),'order '+status)
def create(price=19900,key=None):
    key=key or str(uuid.uuid4())
    status,body=api('/api/orders',{'product':'测试键盘','quantity':2,'unitPriceCents':price},key)
    check(status==202,'HTTP 只先确认任务受理')
    return body['orderId'],key
restored=[]
try:
    status,_=api('/api/orders',{'product':'invalid','quantity':0,'unitPriceCents':100},str(uuid.uuid4()))
    check(status==422,'非法数量被拒绝')
    compose('stop','php-outbox');restored.append('php-outbox')
    unsent,_=create(100)
    saved=order(unsent)
    check(saved['status']=='PENDING_REVIEW' and saved['outbound']['sentAt'] is None and saved['inbound'] is None,'PHP 发送进程暂停时，订单和待发送事件已保存')
    compose('start','php-outbox');restored.remove('php-outbox')
    final(unsent,'CONFIRMED');check(True,'PHP 发送进程恢复后继续完成整条链路')
    compose('stop','java-review');restored.append('java-review')
    first,request_key=create()
    sent=wait(lambda:(x if (x:=order(first))['outbound']['sentAt'] else None),'PHP writes Kafka')
    check(sent['status']=='PENDING_REVIEW' and sent['inbound'] is None,'Java 停止时消息已保存，PHP 订单仍等待')
    compose('start','java-review');restored.remove('java-review')
    approved=final(first,'CONFIRMED')
    check(approved['inbound']['event']['source']=='java-review','Java → PHP 返回真实审核结果')
    input_record=approved['inbound']['event']['data']['inputRecord']
    check(all(input_record[k]==approved['outbound'][k] for k in ['topic','partition','offset','key']),'Java 实际读取位置与 PHP 写入确认一致')
    check(approved['inbound']['key']==first and approved['outbound']['key']==first,'两个方向都以 orderId 为 Key')
    check(approved['inbound']['event']['causationId']==approved['outbound']['event']['eventId'],'结果消息关联原始事件')
    status,reused=api('/api/orders',{'product':'测试键盘','quantity':2,'unitPriceCents':19900},request_key)
    check(status==202 and reused['orderId']==first and reused['reused'],'相同请求重试复用订单')
    status,_=api('/api/orders',{'product':'测试键盘','quantity':3,'unitPriceCents':19900},request_key)
    check(status==409,'相同请求编号不能替换成不同订单内容')
    rejected,_=create(60000)
    check(final(rejected,'REJECTED')['inbound']['event']['data']['status']=='REJECTED','超过 1000 元时 Java 拒绝，PHP 保存拒绝结果')
    compose('stop','php-results');restored.append('php-results')
    pending,_=create(100)
    java_review=wait(lambda:(body if (status_body:=api('/api/reviews/'+pending,base=JAVA))[0]==200 and (body:=status_body[1])['publishedResults']==1 else None),'Java result published')
    check(order(pending)['status']=='PENDING_REVIEW' and order(pending)['inbound'] is None,'Java 已处理但 PHP 未消费时，页面状态仍等待')
    compose('start','php-results');restored.remove('php-results')
    final(pending,'CONFIRMED');check(True,'PHP 恢复消费后更新状态')
    # 重发相同 eventId，等待消费组越过新增记录，避免只看一个暂时未变化的数据库快照。
    replay=json.loads(compose('exec','-T','php-web','php','artisan','demo:replay-created',first).strip())
    def consumed_replay():
        out=compose('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--describe','--group','demo-java-order-reviewers')
        for line in out.splitlines():
            cols=line.split()
            if len(cols)>=5 and cols[1]==replay['topic'] and cols[2]==str(replay['partition']):
                return cols[3].isdigit() and int(cols[3])>replay['offset']
        return False
    wait(consumed_replay,'Java consumed replay',45)
    review=api('/api/reviews/'+first,base=JAVA)[1]
    check(review['processedEvents']==1 and review['resultEvents']==1,'重复创建事件没有再次审核或生成新结果')
    check(order(first)['status']=='CONFIRMED','旧事件重放没有回滚 PHP 数据库')
    # 回退 PHP 消费位置后重新读取旧结果，验证消息重读和数据库回滚的区别。
    compose('stop','php-results');restored.append('php-results')
    compose('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--group','demo-php-order-results','--topic','demo.orders.reviewed','--reset-offsets','--to-earliest','--execute')
    compose('start','php-results');restored.remove('php-results')
    def results_caught_up():
        out=compose('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--describe','--group','demo-php-order-results')
        rows=[line.split() for line in out.splitlines() if 'demo.orders.reviewed' in line]
        return len(rows)==3 and all(len(c)>5 and c[3].isdigit() and c[3]==c[4] for c in rows)
    wait(results_caught_up,'PHP replay caught up',45)
    check(order(first)['status']=='CONFIRMED' and order(rejected)['status']=='REJECTED','回退消费位置并重读后，已有订单终态保持不变')
    compose('restart','php-web','java-review')
    wait(lambda:api('/api/orders/'+first)[0]==200,'PHP restart')
    wait(lambda:api('/api/reviews/'+first,base=JAVA)[0]==200,'Java restart')
    check(order(first)['status']=='CONFIRMED','PHP 重启后保留订单和处理结果')
    check(api('/api/reviews/'+first,base=JAVA)[1]['processedEvents']==1,'Java 重启后保留去重记录')
    result={'passed':True,'checks':checks,'orders':{'approved':first,'rejected':rejected,'pendingExperiment':pending,'outboxExperiment':unsent}}
    out=ROOT/'.test-artifacts';out.mkdir(exist_ok=True);(out/'smoke-result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))
finally:
    for service in restored:compose('start',service)
