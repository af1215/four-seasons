const express=require('express'),crypto=require('crypto'),path=require('path'),http=require('http'),{Pool}=require('pg'),{WebSocketServer}=require('ws');
const app=express();app.use(express.json({limit:'100kb'}));
const pool=new Pool({connectionString:process.env.DATABASE_URL,ssl:process.env.DATABASE_URL&&!/localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL)?{rejectUnauthorized:false}:false});
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const wrap=f=>(q,r)=>f(q,r).catch(e=>{console.error(e);r.status(500).json({error:'server'})});
const hits=new Map();setInterval(()=>hits.clear(),60000);
app.use('/api',(q,r,n)=>{const k=q.ip,c=(hits.get(k)||0)+1;hits.set(k,c);c>120?r.status(429).json({error:'rate'}):n()});
const newUid=()=>String(Math.floor(100000000+Math.random()*900000000));
async function verify(uid,tok){
  if(!/^\d{9}$/.test(uid||'')||!tok)return false;
  const {rows}=await pool.query('select token_hash from players where uid=$1',[uid]);
  if(!rows[0])return false;
  const a=Buffer.from(rows[0].token_hash),b=Buffer.from(sha(String(tok)));
  return a.length===b.length&&crypto.timingSafeEqual(a,b);
}
async function auth(q,r,n){
  const uid=q.get('X-UID'),tok=(q.get('Authorization')||'').replace('Bearer ','');
  if(!await verify(uid,tok))return r.status(401).json({error:'auth'});
  q.uid=uid;n();
}
// ---------- 계정 ----------
app.post('/api/guest',wrap(async(q,r)=>{
  const token=crypto.randomBytes(16).toString('hex');
  for(let i=0;i<10;i++){const uid=newUid();
    const x=await pool.query('insert into players(uid,token_hash,data) values($1,$2,$3) on conflict do nothing returning uid',[uid,sha(token),q.body.data||{}]);
    if(x.rowCount)return r.json({uid,token});}
  r.status(500).json({error:'uid'});
}));
app.get('/api/save',auth,wrap(async(q,r)=>{const {rows}=await pool.query('select data from players where uid=$1',[q.uid]);r.json({uid:q.uid,data:rows[0].data})}));
app.put('/api/save',auth,wrap(async(q,r)=>{await pool.query('update players set data=$2,updated_at=now() where uid=$1',[q.uid,q.body.data||{}]);r.json({ok:true})}));
app.get('/api/profile/:uid',wrap(async(q,r)=>{
  if(!/^\d{9}$/.test(q.params.uid))return r.status(400).json({error:'uid'});
  const {rows}=await pool.query('select data from players where uid=$1',[q.params.uid]);
  if(!rows[0])return r.status(404).json({error:'none'});
  const d=rows[0].data||{};r.json({uid:q.params.uid,chars:Object.keys(d.own||{}).length,zones:(d.prog||[]).filter(x=>x>=8).length,party:(d.party||[]).slice(0,3)});
}));
// ---------- 실시간 (접속/방) ----------
const conns=new Map(),rooms=new Map();
const WW=2400,ZONE=(x,y)=>(x<WW/2?0:1)+(y<WW/2?0:2);
const send=(ws,o)=>{if(ws.readyState===1)ws.send(JSON.stringify(o))};
const ping=u=>{const c=conns.get(u);if(c)send(c.ws,{t:'fr'})};
async function friendIds(uid){
  const {rows}=await pool.query("select a,b from friendships where status='accepted' and (a=$1 or b=$1)",[uid]);
  return rows.map(x=>x.a===uid?x.b:x.a);
}
const notifyFriends=uid=>friendIds(uid).then(l=>l.forEach(ping)).catch(()=>{});
const mkOrb=r=>({id:r.n++,x:Math.round(50+Math.random()*(WW-100)),y:Math.round(50+Math.random()*(WW-100))});
function roomCode(){let c;do{c=Array.from({length:4},()=>'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.floor(Math.random()*24)]).join('')}while(rooms.has(c));return c}
function bcast(r,o,except){const m=JSON.stringify(o);r.members.forEach(u=>{const c=conns.get(u);if(c&&u!==except&&c.ws.readyState===1)c.ws.send(m)})}
function leave(c){const r=c.room&&rooms.get(c.room);c.room=null;if(!r)return;r.members.delete(c.uid);if(!r.members.size){rooms.delete(r.code);return}bcast(r,{t:'left',uid:c.uid})}
function roomInfo(r){return{t:'room',code:r.code,orbs:[...r.orbs.values()],players:[...r.members].map(u=>{const c=conns.get(u);return{u,x:c.x,y:c.y}})}}
function join(c,code){
  const r=rooms.get(code);
  if(!r)return send(c.ws,{t:'err',m:'없는 방이에요'});
  if(r.members.size>=4&&!r.members.has(c.uid))return send(c.ws,{t:'err',m:'방이 가득 찼어요 (최대 4명)'});
  leave(c);c.room=code;r.members.add(c.uid);
  send(c.ws,roomInfo(r));bcast(r,{t:'joined',uid:c.uid},c.uid);
}
setInterval(()=>rooms.forEach(r=>bcast(r,{t:'snap',p:[...r.members].map(u=>{const c=conns.get(u);return[u,Math.round(c.x),Math.round(c.y)]})})),100);
const server=http.createServer(app);
const wss=new WebSocketServer({server,path:'/ws',maxPayload:2048});
wss.on('connection',ws=>{
  let c=null;
  ws.on('error',()=>{});
  ws.on('message',async raw=>{
    let m;try{m=JSON.parse(raw)}catch{return}
    if(!c){
      if(m.t!=='auth')return;
      if(!await verify(String(m.uid),m.token)){send(ws,{t:'err',m:'auth'});return ws.close()}
      c={ws,uid:String(m.uid),x:WW/2,y:WW/2,room:null};
      const old=conns.get(c.uid);if(old&&old.ws!==ws){leave(old);old.ws.close()}
      conns.set(c.uid,c);send(ws,{t:'authok'});notifyFriends(c.uid);return;
    }
    if(m.t==='pos'){const x=+m.x,y=+m.y;if(isFinite(x)&&isFinite(y)){c.x=Math.max(0,Math.min(WW,x));c.y=Math.max(0,Math.min(WW,y))}}
    else if(m.t==='create'){
      const code=roomCode(),r={code,members:new Set(),orbs:new Map(),n:1};
      for(let i=0;i<60;i++){const o=mkOrb(r);r.orbs.set(o.id,o)}
      rooms.set(code,r);join(c,code);
    }
    else if(m.t==='join')join(c,String(m.code||'').toUpperCase());
    else if(m.t==='leave')leave(c);
    else if(m.t==='collect'){
      const r=c.room&&rooms.get(c.room),o=r&&r.orbs.get(+m.id);
      if(!o||Math.hypot(o.x-c.x,o.y-c.y)>600)return; // 시연용 느슨한 거리 검증
      r.orbs.delete(o.id);const n=mkOrb(r);r.orbs.set(n.id,n);
      send(ws,{t:'got',z:ZONE(o.x,o.y)});bcast(r,{t:'o',rm:o.id,add:n});
    }
    else if(m.t==='invite'){
      const to=String(m.uid||''),r=c.room&&rooms.get(c.room),t=conns.get(to);
      if(!r||!t)return;
      const ok=(await friendIds(c.uid)).includes(to);
      if(ok)send(t.ws,{t:'inv',from:c.uid,code:r.code});
    }
  });
  ws.on('close',()=>{if(c&&conns.get(c.uid)===c){leave(c);conns.delete(c.uid);notifyFriends(c.uid)}});
});
// ---------- 친구 (신청/수락) ----------
app.get('/api/friends',auth,wrap(async(q,r)=>{
  const {rows}=await pool.query('select a,b,status from friendships where a=$1 or b=$1',[q.uid]);
  const fr=[],inc=[],out=[];
  for(const x of rows){const o=x.a===q.uid?x.b:x.a;if(x.status==='accepted')fr.push(o);else if(x.b===q.uid)inc.push(o);else out.push(o)}
  const prof=async u=>{const {rows}=await pool.query('select data from players where uid=$1',[u]);const d=(rows[0]&&rows[0].data)||{};
    return{uid:u,chars:Object.keys(d.own||{}).length,zones:(d.prog||[]).filter(v=>v>=8).length,online:conns.has(u)}};
  r.json({friends:await Promise.all(fr.map(prof)),incoming:inc,outgoing:out});
}));
app.post('/api/friends/request',auth,wrap(async(q,r)=>{
  const to=String(q.body.uid||'');
  if(!/^\d{9}$/.test(to)||to===q.uid)return r.status(400).json({error:'uid'});
  if(!(await pool.query('select 1 from players where uid=$1',[to])).rowCount)return r.status(404).json({error:'none'});
  const ex=await pool.query('select a,b,status from friendships where (a=$1 and b=$2) or (a=$2 and b=$1)',[q.uid,to]);
  if(ex.rowCount){const x=ex.rows[0];
    if(x.status==='accepted')return r.json({state:'friends'});
    if(x.a===to){await pool.query("update friendships set status='accepted' where a=$1 and b=$2",[to,q.uid]);ping(to);return r.json({state:'accepted'})}
    return r.json({state:'pending'});}
  await pool.query("insert into friendships(a,b,status) values($1,$2,'pending')",[q.uid,to]);ping(to);r.json({state:'pending'});
}));
app.post('/api/friends/accept',auth,wrap(async(q,r)=>{
  const from=String(q.body.uid||'');
  await pool.query("update friendships set status='accepted' where a=$1 and b=$2 and status='pending'",[from,q.uid]);ping(from);r.json({ok:true});
}));
app.post('/api/friends/remove',auth,wrap(async(q,r)=>{
  const o=String(q.body.uid||'');
  await pool.query('delete from friendships where (a=$1 and b=$2) or (a=$2 and b=$1)',[q.uid,o]);ping(o);r.json({ok:true});
}));
app.get('/',(q,r)=>r.sendFile(path.join(__dirname,'index.html')));
(async()=>{
  await pool.query('create table if not exists players(uid text primary key,token_hash text not null,data jsonb not null default \'{}\',created_at timestamptz default now(),updated_at timestamptz default now())');
  await pool.query('create table if not exists friendships(a text not null,b text not null,status text not null,primary key(a,b))');
  server.listen(process.env.PORT||3000,()=>console.log('up'));
})().catch(e=>{console.error(e);process.exit(1)});
