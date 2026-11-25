// Hazardous Waste Management System - zero-dependency Node 22 server (node:sqlite)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');
const PORT = process.env.PORT || 3000;
const TURSO_URL = process.env.TURSO_URL && process.env.TURSO_URL.replace(/^libsql:/, 'https:'), TURSO_TOKEN = process.env.TURSO_TOKEN;
const lite = TURSO_URL ? null : new DatabaseSync(process.env.DB_FILE || path.join(__dirname, 'data.db'));
if (lite) lite.exec('PRAGMA journal_mode=WAL');
const targ = v => v === null || v === undefined ? { type: 'null' } : typeof v === 'number' ? (Number.isInteger(v) ? { type: 'integer', value: String(v) } : { type: 'float', value: v }) : { type: 'text', value: String(v) };
const tval = c => c.type === 'null' ? null : c.type === 'integer' ? Number(c.value) : c.type === 'float' ? Number(c.value) : c.value;
async function turso(stmts) { // stmts: [{sql,args}] -> array of {cols,rows,last}
  const r = await fetch(TURSO_URL + '/v2/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + TURSO_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ requests: [...stmts.map(x => ({ type: 'execute', stmt: { sql: x.sql, args: (x.args || []).map(targ) } })), { type: 'close' }] }) });
  const j = await r.json(); if (!r.ok) throw new Error('turso ' + r.status);
  return j.results.slice(0, stmts.length).map(x => { if (x.type === 'error') throw new Error(x.error.message); const res = x.response.result; const cols = res.cols.map(c => c.name);
    return { rows: res.rows.map(row => Object.fromEntries(row.map((c, i) => [cols[i], tval(c)]))), last: res.last_insert_rowid == null ? 0 : Number(res.last_insert_rowid) }; });
}
const D = {
  async all(sql, ...a) { if (lite) return lite.prepare(sql).all(...a); return (await turso([{ sql, args: a }]))[0].rows; },
  async get(sql, ...a) { return (await D.all(sql, ...a))[0]; },
  async run(sql, ...a) { if (lite) { const r = lite.prepare(sql).run(...a); return { lastInsertRowid: r.lastInsertRowid }; } return { lastInsertRowid: (await turso([{ sql, args: a }]))[0].last }; },
  async exec(sql) { if (lite) return lite.exec(sql); const parts = sql.split(';').map(x => x.trim()).filter(Boolean); if (parts.length) await turso(parts.map(x => ({ sql: x }))); },
};

const ROLES = { manager: 'مدير', employee: 'موظف', marketer: 'مسوّق' };
// resource schema: fields {k,l(label),t(type),opts,ref,req}; r=roles read, w=roles write, own=role restricted to own records
const R = {
  clients: { title: 'العملاء', r: ['manager','employee','marketer'], w: ['manager','marketer'], own: ['marketer'],
    f: [{k:'name',l:'اسم المنشأة',req:1},{k:'sector',l:'القطاع',t:'sel',opts:['مستشفى','مصنع','مختبر','محطة وقود','أخرى']},{k:'contact',l:'مسؤول التواصل'},{k:'phone',l:'رقم الجوال'},{k:'whatsapp',l:'رقم واتساب (إن اختلف)'},{k:'email',l:'البريد الإلكتروني'},{k:'contact_pref',l:'طريقة التواصل المفضلة',t:'sel',opts:['اتصال هاتفي','واتساب','رسالة نصية','بريد إلكتروني']},{k:'city',l:'المدينة'},{k:'address',l:'العنوان'},{k:'tax_no',l:'الرقم الضريبي / السجل التجاري'},{k:'status',l:'الحالة',t:'sel',opts:['محتمل','نشط','موقوف']},{k:'notes',l:'ملاحظات عن العميل',t:'area'}] },
  quotes: { title: 'عروض الأسعار', r: ['manager','marketer'], w: ['manager','marketer'], own: ['marketer'],
    f: [{k:'number',l:'رقم العرض'},{k:'client_id',l:'العميل',t:'ref',ref:'clients',req:1},{k:'waste_type',l:'نوع المخلفات',t:'sel',opts:['طبية معدية','كيميائية','زيوت مستعملة','بطاريات','مخلفات صناعية','حادة']},{k:'qty',l:'الكمية المتوقعة (كجم)',t:'num'},{k:'unit_price',l:'سعر الكجم (ر.س)',t:'num'},{k:'total',l:'الإجمالي شامل ضريبة 15%',t:'calc'},{k:'valid_until',l:'صالح حتى',t:'date'},{k:'status',l:'الحالة',t:'sel',opts:['مسودة','مُرسل','مقبول','مرفوض']},{k:'body',l:'نص العرض والشروط',t:'area'}] },
  contracts: { title: 'العقود', r: ['manager','employee','marketer'], w: ['manager','marketer'], own: ['marketer'],
    f: [{k:'number',l:'رقم العقد',req:1},{k:'client_id',l:'العميل',t:'ref',ref:'clients',req:1},{k:'scope',l:'نطاق الخدمة',t:'sel',opts:['جمع','جمع ونقل','جمع ونقل وتخلص']},{k:'start_date',l:'البداية',t:'date'},{k:'end_date',l:'النهاية',t:'date'},{k:'value',l:'قيمة العقد (ر.س)',t:'num'},{k:'status',l:'الحالة',t:'sel',opts:['ساري','منتهي','موقوف']},{k:'notes',l:'ملاحظات داخلية',t:'area'},{k:'contract_text',l:'نص العقد الكامل',t:'area'}] },
  jobs: { title: 'العمليات (جمع ونقل وتخلص)', r: ['manager','employee'], w: ['manager','employee'],
    f: [{k:'manifest',l:'رقم بيان النقل',req:1},{k:'client_id',l:'العميل',t:'ref',ref:'clients',req:1},{k:'contract_id',l:'العقد',t:'ref',ref:'contracts'},{k:'waste_type',l:'نوع المخلفات',t:'sel',opts:['طبية معدية','كيميائية','زيوت مستعملة','بطاريات','مخلفات صناعية','حادة']},{k:'hazard_class',l:'فئة الخطورة',t:'sel',opts:['1 - متفجرات','2 - غازات','3 - سوائل قابلة للاشتعال','4 - مواد صلبة قابلة للاشتعال','5 - مؤكسدات','6 - سامة ومعدية','8 - أكالة','9 - متنوعة','غير مصنفة']},{k:'containers',l:'عدد الحاويات / العبوات',t:'num'},{k:'qty',l:'الكمية (كجم)',t:'num'},{k:'pickup_address',l:'موقع الجمع'},{k:'job_date',l:'تاريخ الجمع',t:'date'},{k:'vehicle_id',l:'المركبة',t:'ref',ref:'vehicles'},{k:'vehicle',l:'المركبة (نص حر)'},{k:'driver_id',l:'السائق/الموظف',t:'ref',ref:'users'},{k:'disposal_site',l:'موقع التخلص'},{k:'status',l:'المرحلة',t:'sel',opts:['مجدولة','تم الجمع','في النقل','تم التخلص']},{k:'notes',l:'ملاحظات',t:'area'}] },
  transactions: { title: 'الإيرادات والمصروفات', r: ['manager','employee'], w: ['manager'], wcreate: ['manager','employee'],
    f: [{k:'kind',l:'النوع',t:'sel',opts:['إيراد','مصروف'],req:1},{k:'category',l:'البند',t:'sel',opts:['خدمات جمع ونقل','خدمات تخلص','وقود','رواتب','صيانة مركبات','رسوم تخلص','تراخيص','تسويق','أخرى']},{k:'amount',l:'المبلغ (ر.س)',t:'num',req:1},{k:'tx_date',l:'التاريخ',t:'date'},{k:'client_id',l:'العميل (للإيراد)',t:'ref',ref:'clients'},{k:'note',l:'ملاحظات'}] },
  vehicles: { title: 'المركبات والسائقون', r: ['manager','employee'], w: ['manager','employee'],
    f: [{k:'name',l:'رقم اللوحة / اسم المركبة',req:1},{k:'vtype',l:'النوع',t:'sel',opts:['شاحنة نقل مخلفات خطرة','صهريج','وانيت','أخرى']},{k:'capacity',l:'الحمولة (كجم)',t:'num'},{k:'driver',l:'السائق المعتمد'},{k:'driver_phone',l:'جوال السائق'},{k:'registration_expiry',l:'انتهاء الاستمارة',t:'date'},{k:'insurance_expiry',l:'انتهاء التأمين',t:'date'},{k:'permit_expiry',l:'انتهاء تصريح نقل المخلفات',t:'date'},{k:'status',l:'الحالة',t:'sel',opts:['متاحة','في مهمة','صيانة']}] },
  licenses: { title: 'التراخيص والتصاريح', r: ['manager','employee'], w: ['manager','employee'],
    f: [{k:'name',l:'اسم الترخيص',req:1},{k:'kind',l:'النوع',t:'sel',opts:['ترخيص بيئي','رخصة نقل مخلفات خطرة','سجل تجاري','رخصة بلدية','دفاع مدني','رخصة سائق','تأمين','أخرى']},{k:'number',l:'الرقم'},{k:'issuer',l:'الجهة المصدرة'},{k:'holder',l:'الخاص بـ (شركة / سائق / مركبة)'},{k:'issue_date',l:'تاريخ الإصدار',t:'date'},{k:'expiry_date',l:'تاريخ الانتهاء',t:'date',req:1},{k:'notes',l:'ملاحظات',t:'area'}] },
  certificates: { title: 'شهادات التخلص', r: ['manager','employee'], w: ['manager','employee'],
    f: [{k:'number',l:'رقم الشهادة',req:1},{k:'client_id',l:'العميل',t:'ref',ref:'clients',req:1},{k:'job_id',l:'العملية (بيان النقل)',t:'ref',ref:'jobs'},{k:'waste_type',l:'نوع المخلفات'},{k:'qty',l:'الكمية (كجم)',t:'num'},{k:'method',l:'طريقة التخلص',t:'sel',opts:['حرق آمن','معالجة كيميائية','معالجة حرارية','دفن آمن','إعادة تدوير','تعقيم']},{k:'disposal_site',l:'موقع التخلص'},{k:'issue_date',l:'تاريخ الإصدار',t:'date'},{k:'notes',l:'ملاحظات',t:'area'}] },
  invoices: { title: 'الفواتير والمدفوعات', r: ['manager','employee'], w: ['manager'],
    f: [{k:'number',l:'رقم الفاتورة',req:1},{k:'client_id',l:'العميل',t:'ref',ref:'clients',req:1},{k:'contract_id',l:'العقد',t:'ref',ref:'contracts'},{k:'amount',l:'المبلغ شامل الضريبة (ر.س)',t:'num',req:1},{k:'paid',l:'المدفوع (ر.س)',t:'num'},{k:'balance',l:'المتبقي',t:'calc'},{k:'issue_date',l:'تاريخ الإصدار',t:'date'},{k:'due_date',l:'تاريخ الاستحقاق',t:'date'},{k:'status',l:'الحالة',t:'sel',opts:['غير مدفوعة','مدفوعة جزئياً','مدفوعة']},{k:'notes',l:'ملاحظات',t:'area'}] },
  users: { title: 'المستخدمون', r: ['manager'], w: ['manager'], special: true,
    f: [{k:'name',l:'الاسم',req:1},{k:'username',l:'اسم الدخول',req:1},{k:'role',l:'الدور',t:'sel',opts:['manager','employee','marketer'],labels:ROLES},{k:'password',l:'كلمة المرور',t:'pass'},{k:'active',l:'نشط',t:'sel',opts:['1','0'],labels:{1:'نعم',0:'لا'}}] },
};
const colType = f => (f.t==='num'||f.t==='ref')?'REAL':'TEXT';
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
async function addUser(name, username, role, pw) {
  const salt = crypto.randomBytes(16).toString('hex');
  await D.run('INSERT INTO users(name,username,role,salt,hash) VALUES(?,?,?,?,?)', name, username, role, salt, hashPw(pw, salt));
}
async function initDb() {
  await D.exec(`CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT, username TEXT UNIQUE, role TEXT, salt TEXT, hash TEXT, active TEXT DEFAULT '1', created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, user_id INTEGER, expires INTEGER);
CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY, user_id INTEGER, action TEXT, res TEXT, rec INTEGER, at TEXT DEFAULT CURRENT_TIMESTAMP)`);
  for (const [n, d] of Object.entries(R)) if (n !== 'users')
    await D.exec(`CREATE TABLE IF NOT EXISTS ${n}(id INTEGER PRIMARY KEY, ${d.f.filter(f=>f.t!=='calc').map(f=>f.k+' '+colType(f)).join(',')}, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)`);
  for (const [n, d] of Object.entries(R)) if (n !== 'users') { // add columns introduced after first deploy
    const have = (await D.all(`PRAGMA table_info(${n})`)).map(c => c.name);
    for (const f of d.f) if (f.t !== 'calc' && !have.includes(f.k)) await D.exec(`ALTER TABLE ${n} ADD COLUMN ${f.k} ${colType(f)}`);
  }
  if (!await D.get('SELECT 1 x FROM users')) {
    if (process.env.NO_DEMO) { // production: only the manager account; password from ADMIN_PASSWORD (required)
      if (!process.env.ADMIN_PASSWORD || process.env.ADMIN_PASSWORD.length < 12) throw new Error('Set ADMIN_PASSWORD (12+ chars) when NO_DEMO=1');
      await addUser('المدير العام', 'admin', 'manager', process.env.ADMIN_PASSWORD);
    } else { // demo accounts with known passwords: never use with real data
      await addUser('المدير العام', 'admin', 'manager', process.env.ADMIN_PASSWORD || 'admin123');
      await addUser('خالد السبيعي', 'khaled', 'employee', 'emp123');
      await addUser('سعد الحربي', 'saad', 'employee', 'emp123');
      await addUser('نورة القحطاني', 'noura', 'marketer', 'mkt123');
      await addUser('فهد العتيبي', 'fahad', 'marketer', 'mkt123');
      await require('./seed.js')(D);
    }
  }
}

const send = (res, code, obj, headers={}) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', ...headers }); res.end(JSON.stringify(obj)); };
const body = req => new Promise(r => { let b=''; req.on('data',c=>{b+=c; if(b.length>1e6) req.destroy();}); req.on('end',()=>{ try{r(JSON.parse(b||'{}'))}catch{r({})} }); });
const cookie = req => Object.fromEntries((req.headers.cookie||'').split(';').map(s=>s.trim().split('=')).filter(a=>a[0]));
const sessionUser = async req => { const t = cookie(req).sid; if(!t) return null;
  return await D.get("SELECT u.id,u.name,u.username,u.role FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=? AND s.expires>? AND u.active='1'", t, Date.now()) || null; };
const attempts = {};
const clean = (d, o) => { const out = {}; for (const f of d.f) if (f.t!=='calc' && f.t!=='pass' && o[f.k]!==undefined) out[f.k] = (colType(f)==='REAL' ? (o[f.k]===''||o[f.k]===null?null:Number(o[f.k])) : String(o[f.k]).slice(0,2000)); return out; };

async function listRes(name, u) {
  const d = R[name];
  let rows;
  if (name === 'users') rows = await D.all('SELECT id,name,username,role,active FROM users');
  else if (d.own?.includes(u.role)) rows = await D.all(`SELECT * FROM ${name} WHERE created_by=? ORDER BY id DESC`, u.id);
  else rows = await D.all(`SELECT * FROM ${name} ORDER BY id DESC`);
  if (name==='invoices') rows.forEach(q=>q.balance=Math.round(((q.amount||0)-(q.paid||0))*100)/100);
  if (name==='quotes') rows.forEach(q=>q.total=Math.round((q.qty||0)*(q.unit_price||0)*1.15*100)/100);
  return rows;
}
async function summary() {
  const q = s => D.all(s);
  const tot = async k => (await D.get("SELECT COALESCE(SUM(amount),0) v FROM transactions WHERE kind=?", k)).v;
  const o = {
    revenue: await tot('إيراد'), expense: await tot('مصروف'),
    clients: (await D.get('SELECT COUNT(*) c FROM clients')).c,
    activeContracts: (await D.get("SELECT COUNT(*) c FROM contracts WHERE status='ساري'")).c,
    openQuotes: (await D.get("SELECT COUNT(*) c FROM quotes WHERE status IN ('مسودة','مُرسل')")).c,
    kgTotal: (await D.get('SELECT COALESCE(SUM(qty),0) v FROM jobs')).v,
    unpaid: (await D.get("SELECT COALESCE(SUM(amount-COALESCE(paid,0)),0) v, COUNT(*) c FROM invoices WHERE status!='مدفوعة'")),
    overdue: (await D.get("SELECT COUNT(*) c FROM invoices WHERE status!='مدفوعة' AND due_date IS NOT NULL AND due_date<>'' AND due_date<date('now')")).c,
    alerts: [].concat(
      (await q("SELECT 'ترخيص' t, name n, expiry_date d FROM licenses WHERE expiry_date IS NOT NULL AND expiry_date<>'' AND expiry_date<=date('now','+60 day')")),
      (await q("SELECT 'استمارة مركبة' t, name n, registration_expiry d FROM vehicles WHERE registration_expiry<>'' AND registration_expiry<=date('now','+60 day')")),
      (await q("SELECT 'تأمين مركبة' t, name n, insurance_expiry d FROM vehicles WHERE insurance_expiry<>'' AND insurance_expiry<=date('now','+60 day')")),
      (await q("SELECT 'تصريح نقل' t, name n, permit_expiry d FROM vehicles WHERE permit_expiry<>'' AND permit_expiry<=date('now','+60 day')")),
      (await q("SELECT 'عقد ينتهي' t, number n, end_date d FROM contracts WHERE status='ساري' AND end_date<>'' AND end_date<=date('now','+60 day')"))
    ).sort((a,b)=>String(a.d).localeCompare(String(b.d))),
    jobsByStatus: await q('SELECT status k, COUNT(*) v FROM jobs GROUP BY status'),
    wasteByType: await q('SELECT waste_type k, SUM(qty) v FROM jobs GROUP BY waste_type ORDER BY v DESC'),
    monthly: await q("SELECT substr(tx_date,1,7) m, SUM(CASE WHEN kind='إيراد' THEN amount ELSE 0 END) rev, SUM(CASE WHEN kind='مصروف' THEN amount ELSE 0 END) exp FROM transactions WHERE tx_date IS NOT NULL GROUP BY m ORDER BY m"),
    byCategory: await q("SELECT kind, category k, SUM(amount) v FROM transactions GROUP BY kind, category ORDER BY v DESC"),
    byClient: await q("SELECT c.id, c.name, COALESCE((SELECT SUM(amount) FROM transactions t WHERE t.client_id=c.id AND t.kind='إيراد'),0) revenue, (SELECT COUNT(*) FROM contracts k WHERE k.client_id=c.id AND k.status='ساري') contracts, (SELECT COALESCE(SUM(qty),0) FROM jobs j WHERE j.client_id=c.id) kg FROM clients c ORDER BY revenue DESC"),
    marketers: await q("SELECT u.name, (SELECT COUNT(*) FROM quotes x WHERE x.created_by=u.id) quotes, (SELECT COUNT(*) FROM quotes x WHERE x.created_by=u.id AND x.status='مقبول') won, (SELECT COALESCE(SUM(qty*unit_price*1.15),0) FROM quotes x WHERE x.created_by=u.id AND x.status='مقبول') wonValue FROM users u WHERE u.role='marketer'"),
  };
  return o;
}
const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript', '.css':'text/css' };

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x'); const p = url.pathname;
  try {
    if (!p.startsWith('/api/')) {
      if (require('./pwa.js')(p, res)) return;
      if (p === '/' || p === '/index.html') { res.writeHead(200, {'Content-Type': MIME['.html']}); return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res); }
      res.writeHead(404); return res.end('not found');
    }
    if (p === '/api/login' && req.method === 'POST') {
      const ip = req.socket.remoteAddress; const a = attempts[ip] = (attempts[ip]||[]).filter(t=>Date.now()-t<600000);
      if (a.length >= 10) return send(res, 429, { error: 'محاولات كثيرة، حاول بعد 10 دقائق' });
      const b = await body(req);
      const u = await D.get("SELECT * FROM users WHERE username=? AND active='1'", String(b.username||''));
      if (!u || !crypto.timingSafeEqual(Buffer.from(hashPw(String(b.password||''), u.salt)), Buffer.from(u.hash))) { a.push(Date.now()); return send(res, 401, { error: 'بيانات الدخول غير صحيحة' }); }
      const t = crypto.randomBytes(24).toString('hex');
      await D.run('INSERT INTO sessions VALUES(?,?,?)', t, u.id, Date.now()+12*3600e3);
      return send(res, 200, { ok: 1 }, { 'Set-Cookie': `sid=${t}; HttpOnly; Path=/; SameSite=Strict; Max-Age=43200${process.env.HTTPS?'; Secure':''}` });
    }
    const u = await sessionUser(req);
    if (p === '/api/logout') { await D.run('DELETE FROM sessions WHERE token=?', cookie(req).sid||''); return send(res, 200, {ok:1}, {'Set-Cookie':'sid=; Path=/; Max-Age=0'}); }
    if (!u) return send(res, 401, { error: 'unauth' });
    if (p === '/api/me') {
      const meta = {}; for (const [n,d] of Object.entries(R)) if (d.r.includes(u.role)) meta[n] = { title:d.title, f:d.f, canWrite: d.w.includes(u.role), canCreate: (d.wcreate||d.w).includes(u.role) };
      return send(res, 200, { user: u, roles: ROLES, meta });
    }
    if (p === '/api/summary') { if (u.role==='marketer') return send(res,403,{error:'forbidden'}); return send(res, 200, await summary()); }
    if (p === '/api/password' && req.method==='POST') { const b = await body(req); if(String(b.password||'').length<8) return send(res,400,{error:'كلمة المرور 8 أحرف على الأقل'});
      const salt = crypto.randomBytes(16).toString('hex'); await D.run('UPDATE users SET salt=?,hash=? WHERE id=?', salt, hashPw(b.password, salt), u.id); return send(res,200,{ok:1}); }
    const m = p.match(/^\/api\/(\w+)(?:\/(\d+))?$/);
    if (m && R[m[1]]) {
      const name = m[1], d = R[name], id = m[2] && Number(m[2]);
      if (!d.r.includes(u.role)) return send(res, 403, { error: 'غير مصرح' });
      if (req.method === 'GET' && !id) return send(res, 200, await listRes(name, u));
      if (name==='users' && req.method==='GET') return send(res,200,await D.all('SELECT id,name FROM users'));
      if (req.method === 'POST' && !id) {
        if (!(d.wcreate||d.w).includes(u.role)) return send(res, 403, { error: 'غير مصرح' });
        const b = await body(req);
        for (const f of d.f) if (f.req && !b[f.k]) return send(res, 400, { error: `الحقل مطلوب: ${f.l}` });
        if (name === 'users') {
          if (!b.password || b.password.length < 8) return send(res, 400, { error: 'كلمة المرور 8 أحرف على الأقل' });
          if (!ROLES[b.role]) return send(res, 400, { error: 'دور غير صحيح' });
          try { await addUser(b.name, b.username, b.role, b.password); } catch { return send(res, 400, { error: 'اسم الدخول مستخدم' }); }
          return send(res, 200, { ok: 1 });
        }
        const o = clean(d, b); const ks = Object.keys(o);
        const r = await D.run(`INSERT INTO ${name}(${ks.join(',')},created_by) VALUES(${ks.map(()=>'?').join(',')},?)`, ...ks.map(k=>o[k]), u.id);
        await D.run('INSERT INTO audit(user_id,action,res,rec) VALUES(?,?,?,?)', u.id,'create',name,Number(r.lastInsertRowid));
        return send(res, 200, { id: Number(r.lastInsertRowid) });
      }
      if (id && (req.method === 'PUT' || req.method === 'DELETE')) {
        if (!d.w.includes(u.role)) return send(res, 403, { error: 'غير مصرح' });
        if (d.own?.includes(u.role) && name!=='users' && !await D.get(`SELECT 1 FROM ${name} WHERE id=? AND created_by=?`, id, u.id)) return send(res, 403, { error: 'غير مصرح' });
        if (req.method === 'DELETE') {
          if (name==='users') { if (id===u.id) return send(res,400,{error:'لا يمكن حذف حسابك'}); await D.run("UPDATE users SET active='0' WHERE id=?", id); }
          else if (u.role!=='manager') return send(res,403,{error:'الحذف للمدير فقط'});
          else await D.run(`DELETE FROM ${name} WHERE id=?`, id);
          await D.run('INSERT INTO audit(user_id,action,res,rec) VALUES(?,?,?,?)', u.id,'delete',name,id); return send(res,200,{ok:1});
        }
        const b = await body(req);
        if (name === 'users') {
          if (b.name) await D.run('UPDATE users SET name=?,role=?,active=? WHERE id=?', b.name, ROLES[b.role]?b.role:'employee', b.active==='0'?'0':'1', id);
          if (b.password) { if (b.password.length<8) return send(res,400,{error:'كلمة المرور 8 أحرف على الأقل'}); const salt=crypto.randomBytes(16).toString('hex'); await D.run('UPDATE users SET salt=?,hash=? WHERE id=?', salt, hashPw(b.password,salt), id); }
          return send(res, 200, { ok: 1 });
        }
        const o = clean(d, b); const ks = Object.keys(o);
        if (ks.length) await D.run(`UPDATE ${name} SET ${ks.map(k=>k+'=?').join(',')} WHERE id=?`, ...ks.map(k=>o[k]), id);
        await D.run('INSERT INTO audit(user_id,action,res,rec) VALUES(?,?,?,?)', u.id,'update',name,id);
        return send(res, 200, { ok: 1 });
      }
    }
    send(res, 404, { error: 'not found' });
  } catch (e) { console.error(e); send(res, 500, { error: 'خطأ في الخادم' }); }
});
initDb().then(() => server.listen(PORT, () => console.log('listening on', PORT))).catch(e => { console.error(e); process.exit(1); });
