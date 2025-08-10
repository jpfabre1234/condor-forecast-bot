import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { chromium } from 'playwright';
import xlsx from 'xlsx';
import fetch from 'node-fetch';

const env = (k, d=null) => process.env[k] ?? d;

// REQUIRED
let PORTAL_URL = env('PORTAL_URL'); // https://de.acespower.com
if (PORTAL_URL && PORTAL_URL.startsWith('hhttps://')) PORTAL_URL = PORTAL_URL.replace('hhttps://','https://');
const USERNAME = env('USERNAME','');
const PASSWORD = env('PASSWORD','');
const MAKE_WEBHOOK_URL = env('MAKE_WEBHOOK_URL');

// OPTIONAL TUNING
const TIMEZONE = env('TIMEZONE','America/Chicago');
const PRICE_THRESHOLD = Number(env('PRICE_THRESHOLD','80'));
const HOURS_LOOKAHEAD = Number(env('HOURS_LOOKAHEAD','6'));
const FILE_REGEX = new RegExp(env('FILE_REGEX','AMIL\\.WVPA.*(forecast|fcst).*(csv|xlsx)$'), 'i');
const LINK_SELECTOR = env('LINK_SELECTOR', "a[href$='.csv'], a[href$='.xlsx']");
const DIRECT_CSV_URL = env('DIRECT_CSV_URL',''); // set this to skip clicking

if (!PORTAL_URL || !MAKE_WEBHOOK_URL) {
  console.error('Missing PORTAL_URL or MAKE_WEBHOOK_URL'); process.exit(2);
}

const sha256File = (fp) => { const h = crypto.createHash('sha256'); h.update(fs.readFileSync(fp)); return h.digest('hex'); };
const toISOZ = (d) => new Date(d).toISOString();
function toLocalISO(date, tz) {
  try {
    const fmt = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false });
    const parts = fmt.formatToParts(date);
    const m = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${m.year}-${m.month}-${m.day}T${m.hour}:${m.minute}:${m.second}`;
  } catch { return new Date(date).toString(); }
}

function parseCSV_amil_wvpa(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length > 0);
  const header = lines[0].split(',').map(s => s.trim().toLowerCase());
  const idx = { date: header.indexOf('date'), he: header.indexOf('he'), node: header.indexOf('node'), forecast: header.indexOf('forecast'), value: header.indexOf('value') };
  if (idx.date === -1 || idx.he === -1 || (idx.forecast === -1 && idx.value === -1)) {
    throw new Error(`CSV schema not recognized. Headers: ${header.join(', ')}`);
  }
  const out = [];
  for (let i=1;i<lines.length;i++){
    const cols = lines[i].split(',');
    if (cols.length < 3) continue;
    const dstr = (cols[idx.date] || '').trim();
    const heStr = (cols[idx.he] || '').trim();
    const priceStr = (cols[idx.forecast] || cols[idx.value] || '').trim();
    if (!dstr || !heStr || !priceStr) continue;
    const he = Number(heStr), price = Number(priceStr);
    if (Number.isNaN(he) || Number.isNaN(price)) continue;
    // HE 1 -> 01:00 local, HE 24 -> next-day 00:00 local
    const [Y,M,D] = dstr.split('-').map(Number);
    const hrEnd = he % 24;
    const addDays = he === 24 ? 1 : 0;
    const tsUTC = new Date(Date.UTC(Y, M-1, D + addDays, hrEnd, 0, 0)).toISOString();
    out.push({ ts_utc: tsUTC, price });
  }
  out.sort((a,b)=>a.ts_utc.localeCompare(b.ts_utc));
  return out;
}

function parseExcelFlexible(filePath) {
  const wb = xlsx.readFile(filePath, { cellDates: true });
  const s = wb.SheetNames[0];
  const sh = wb.Sheets[s];
  const rows = xlsx.utils.sheet_to_json(sh, { defval: null });
  const tsKeys = ['Timestamp','Time','IntervalStart','Start','Hour','DATETIME','INTERVAL START','ts','time_utc'];
  const priceKeys = ['Price','LMP','Value','PRICE','LMP ($/MWh)','forecast'];
  let tsKey = rows[0] ? Object.keys(rows[0]).find(k => tsKeys.map(x=>x.toLowerCase()).includes(k.toLowerCase())) : null;
  let pKey  = rows[0] ? Object.keys(rows[0]).find(k => priceKeys.map(x=>x.toLowerCase()).includes(k.toLowerCase())) : null;
  if (!tsKey || !pKey) throw new Error('Excel schema not recognized.');
  return rows.map(r => {
    const t = r[tsKey];
    const d = (t instanceof Date) ? t : new Date(t);
    return { ts_utc: new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), 0, 0)).toISOString(), price: Number(r[pKey]) };
  }).filter(x => !Number.isNaN(x.price));
}

async function downloadForecast() {
  const tmpDir = '/tmp/condor_dl';
  fs.mkdirSync(tmpDir, { recursive: true });

  if (DIRECT_CSV_URL) {
    const fn = path.join(tmpDir, DIRECT_CSV_URL.split('/').pop() || 'download.csv');
    const res = await fetch(DIRECT_CSV_URL);
    fs.writeFileSync(fn, Buffer.from(await res.arrayBuffer()));
    return fn;
  }

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  await page.goto(PORTAL_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
  if (USERNAME && PASSWORD) {
    try {
      await page.waitForSelector("input[type='text'], input[name='username'], #username", { timeout: 8000 });
      await page.fill("input[type='text'], input[name='username'], #username", USERNAME);
      await page.fill("input[type='password'], #password", PASSWORD);
      const btn = await page.$("button[type='submit'], input[type='submit'], button:has-text('Log in'), button:has-text('Sign in')");
      if (btn) await btn.click();
      await page.waitForLoadState('networkidle', { timeout: 120000 });
    } catch { /* login could be SSO; continue */ }
  }

  await page.waitForTimeout(1500);

  const links = await page.$$eval(LINK_SELECTOR, els => els.map(a => ({ href: a.getAttribute('href'), text: (a.textContent||'').trim() })));
  const candidates = (links||[]).filter(l => l.href && /\.(csv|xlsx)$/i.test(l.href));
  if (candidates.length === 0) throw new Error('No CSV/XLSX links found. Adjust LINK_SELECTOR or provide DIRECT_CSV_URL.');

  const ranked = candidates.sort((a,b) => {
    const aw = (FILE_REGEX.test(a.href) || FILE_REGEX.test(a.text)) ? 1 : 0;
    const bw = (FILE_REGEX.test(b.href) || FILE_REGEX.test(b.text)) ? 1 : 0;
    return bw - aw;
  });
  const target = ranked[0];

  let filePath;
  try {
    const [dl] = await Promise.all([
      page.waitForEvent('download', { timeout: 30000 }),
      page.click(`a[href="${target.href}"]`).catch(()=>page.evaluate((href)=>{window.location.href=href}, target.href))
    ]);
    const tmp = path.join(tmpDir, await dl.suggestedFilename());
    await dl.saveAs(tmp);
    filePath = tmp;
  } catch {
    const resp = await page.goto(target.href, { timeout: 60000 });
    const buf = await resp.body();
    const name = target.href.split('/').pop() || 'download.csv';
    filePath = path.join(tmpDir, name);
    fs.writeFileSync(filePath, buf);
  }

  await browser.close();
  return filePath;
}

(async () => {
  if (!MAKE_WEBHOOK_URL) { console.error('No MAKE_WEBHOOK_URL'); process.exit(2); }

  const filePath = await downloadForecast();
  const hash = sha256File(filePath);
  const ext = path.extname(filePath).toLowerCase();

  let series = [];
  try {
    series = (ext === '.csv') ? parseCSV_amil_wvpa(filePath) : parseExcelFlexible(filePath);
  } catch (err) {
    await fetch(MAKE_WEBHOOK_URL, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ source:'github_actions',
        error:{ message:String(err), stage:'parse|schema' },
        file_name: path.basename(filePath), file_sha256: hash, portal_url: PORTAL_URL })
    });
    throw err;
  }

  const now = new Date();
  const windowEnd = new Date(now.getTime() + HOURS_LOOKAHEAD*3600*1000);

  const flagged = [];
  const raw = [];
  for (const r of series) {
    const ts = new Date(r.ts_utc);
    if (ts >= now && ts <= windowEnd) {
      raw.push({ ts_utc: r.ts_utc, local: toLocalISO(ts, TIMEZONE), price: r.price });
      if (r.price >= PRICE_THRESHOLD) flagged.push({ ts_utc: r.ts_utc, local: toLocalISO(ts, TIMEZONE), price: r.price });
    }
  }

  const lastTs = series.length ? series[series.length-1].ts_utc : toISOZ(now);
  const payload = {
    source: 'github_actions',
    idempotency_key: `${hash}_amilwvpa_${Date.parse(lastTs)/1000}`,
    file_name: path.basename(filePath),
    file_sha256: hash,
    portal_url: PORTAL_URL,
    sheet: 'AMIL.WVPA',
    timezone: TIMEZONE,
    generated_at_utc: toISOZ(now),
    window_start_utc: toISOZ(now),
    window_end_utc: toISOZ(windowEnd),
    threshold: PRICE_THRESHOLD,
    interval_minutes: 60,
    rows_evaluated: series.length,
    flagged,
    raw_intervals: raw,
    notes: ['parsed CSV date,he,node,forecast; Excel fallback supported']
  };

  await fetch(MAKE_WEBHOOK_URL, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(payload) });
  console.log(`posted to Make: flagged=${flagged.length}, rows=${series.length}`);
})().catch(e => { console.error(e); process.exit(1); });
