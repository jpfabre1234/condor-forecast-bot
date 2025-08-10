import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { chromium } from 'playwright';
import xlsx from 'xlsx';
import fetch from 'node-fetch';

const env = (k, d = null) => process.env[k] ?? d;

// REQUIRED
let PORTAL_URL = env('PORTAL_URL'); // e.g. https://de.acespower.com
if (PORTAL_URL && PORTAL_URL.startsWith('hhttps://')) PORTAL_URL = PORTAL_URL.replace('hhttps://', 'https://');
const USERNAME = env('USERNAME', '');
const PASSWORD = env('PASSWORD', '');
const MAKE_WEBHOOK_URL = env('MAKE_WEBHOOK_URL');

// OPTIONAL
const TIMEZONE = env('TIMEZONE', 'America/Chicago');
const PRICE_THRESHOLD = Number(env('PRICE_THRESHOLD', '80'));
const HOURS_LOOKAHEAD = Number(env('HOURS_LOOKAHEAD', '6'));

if (!PORTAL_URL || !MAKE_WEBHOOK_URL) {
  console.error('Missing PORTAL_URL or MAKE_WEBHOOK_URL');
  process.exit(2);
}

const sha256File = (fp) => {
  const h = crypto.createHash('sha256');
  h.update(fs.readFileSync(fp));
  return h.digest('hex');
};
const toISOZ = (d) => new Date(d).toISOString();
function toLocalISO(date, tz) {
  try {
    const fmt = new Intl.DateTimeFormat('en-CA', {
      timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    });
    const parts = fmt.formatToParts(date);
    const m = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${m.year}-${m.month}-${m.day}T${m.hour}:${m.minute}:${m.second}`;
  } catch {
    return new Date(date).toString();
  }
}

// ---- Parsers ----
function parseCSV_amil_wvpa(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length > 0);
  const header = lines[0].split(',').map(s => s.trim().toLowerCase());
  const idx = {
    date: header.indexOf('date'),
    he: header.indexOf('he'),
    node: header.indexOf('node'),
    forecast: header.indexOf('forecast'),
    value: header.indexOf('value'),
  };
  if (idx.date === -1 || idx.he === -1 || (idx.forecast === -1 && idx.value === -1)) {
    throw new Error(`CSV schema not recognized. Headers: ${header.join(', ')}`);
  }
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < 3) continue;
    const dstr = (cols[idx.date] || '').trim();
    const heStr = (cols[idx.he] || '').trim();
    const priceStr = (cols[idx.forecast] || cols[idx.value] || '').trim();
    if (!dstr || !heStr || !priceStr) continue;
    const he = Number(heStr), price = Number(priceStr);
    if (Number.isNaN(he) || Number.isNaN(price)) continue;

    // HE 1 -> 01:00 local, HE 24 -> next-day 00:00 local
    const [Y, M, D] = dstr.split('-').map(Number);
    const hrEnd = he % 24;
    const addDays = he === 24 ? 1 : 0;
    const tsUTC = new Date(Date.UTC(Y, M - 1, D + addDays, hrEnd, 0, 0)).toISOString();
    out.push({ ts_utc: tsUTC, price });
  }
  out.sort((a, b) => a.ts_utc.localeCompare(b.ts_utc));
  return out;
}

function parseExcelFlexible(filePath) {
  const wb = xlsx.readFile(filePath, { cellDates: true });
  const s = wb.SheetNames[0];
  const sh = wb.Sheets[s];
  const rows = xlsx.utils.sheet_to_json(sh, { defval: null });
  const tsKeys = ['Timestamp', 'Time', 'IntervalStart', 'Start', 'Hour', 'DATETIME', 'INTERVAL START', 'ts', 'time_utc'];
  const priceKeys = ['Price', 'LMP', 'Value', 'PRICE', 'LMP ($/MWh)', 'forecast'];
  let tsKey = rows[0] ? Object.keys(rows[0]).find(k => tsKeys.map(x => x.toLowerCase()).includes(k.toLowerCase())) : null;
  let pKey = rows[0] ? Object.keys(rows[0]).find(k => priceKeys.map(x => x.toLowerCase()).includes(k.toLowerCase())) : null;
  if (!tsKey || !pKey) throw new Error('Excel schema not recognized.');
  return rows.map(r => {
    const t = r[tsKey];
    const d = (t instanceof Date) ? t : new Date(t);
    return {
      ts_utc: new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), 0, 0)).toISOString(),
      price: Number(r[pKey]),
    };
  }).filter(x => !Number.isNaN(x.price));
}

// ---- Downloader: click newest filename; catch native download or AJAX blob ----
async function downloadForecast() {
  const tmpDir = '/tmp/condor_dl';
  fs.mkdirSync(tmpDir, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  // 1) Login
  await page.goto(PORTAL_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
  if (USERNAME && PASSWORD) {
    try {
      await page.waitForSelector("input[type='text'], input[name='username'], #username", { timeout: 8000 });
      await page.fill("input[type='text'], input[name='username'], #username", USERNAME);
      await page.fill("input[type='password'], #password", PASSWORD);
      const btn = await page.$("button[type='submit'], input[type='submit'], button:has-text('Log in'), button:has-text('Sign in')");
      if (btn) await btn.click();
      await page.waitForLoadState('networkidle', { timeout: 120000 });
    } catch { /* continue even if login UI differs */ }
  }

  // 2) Sniff CSV/XLSX network responses (AJAX blob fallback)
  let sniffedPath = null;
  page.on('response', async (resp) => {
    try {
      const headers = resp.headers();
      const ct = (headers['content-type'] || '').toLowerCase();
      const cd = headers['content-disposition'] || '';
      const isFile = ct.includes('text/csv')
        || ct.includes('application/vnd.ms-excel')
        || ct.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      if (!isFile) return;
      const body = await resp.body();
      const m = /filename\*=UTF-8''([^;]+)|filename="?([^"]+)"?/i.exec(cd);
      const nameGuess = decodeURIComponent((m && (m[1] || m[2])) || 'download.csv');
      const fp = path.join(tmpDir, nameGuess);
      fs.writeFileSync(fp, body);
      sniffedPath = fp;
    } catch { /* ignore */ }
  });

  await page.waitForTimeout(1500);

  // 3) Find visible filenames (main page + iframes)
  const fileRe = /AMIL\.WVPA_rt_price_forecast_(\d{14})\.csv$/i;

  async function namesIn(frameLike) {
    try {
      return await frameLike.$$eval('a, [role="link"], .file-name, td, span, div', els =>
        Array.from(new Set(
          els.map(e => (e.textContent || '').trim())
             .filter(t => /AMIL\.WVPA_rt_price_forecast_\d{14}\.csv$/i.test(t))
        ))
      );
    } catch {
      return [];
    }
  }

  const frames = [page, ...page.frames().filter(f => f !== page)];
  let allNames = [];
  for (const f of frames) allNames = allNames.concat(await namesIn(f));
  allNames = Array.from(new Set(allNames));

  if (allNames.length === 0) {
    await page.screenshot({ path: 'page.png', fullPage: true });
    console.log('DEBUG_FILENAMES_BEGIN');
    console.log(JSON.stringify({ frames: frames.length, names: allNames }, null, 2));
    console.log('DEBUG_FILENAMES_END');
    await browser.close();
    throw new Error('No AMIL.WVPA_rt_price_forecast_YYYYMMDDHHMMSS.csv text found.');
  }

  // 4) Pick newest by timestamp
  allNames.sort((a, b) => {
    const na = Number((a.match(fileRe) || [])[1] || 0);
    const nb = Number((b.match(fileRe) || [])[1] || 0);
    return nb - na;
  });
  const targetName = allNames[0];
  console.log(`Chosen file: ${targetName}`);

  // 5) Click that exact text; capture native download or sniffed blob
  async function clickByExactText(frameLike) {
    const loc = frameLike.locator(`text="${targetName}"`);
    if (await loc.count() === 0) return null;

    // try native download event
    try {
      const [dl] = await Promise.all([
        page.waitForEvent('download', { timeout: 30000 }),
        loc.first().click({ delay: 50 }),
      ]);
      const fp = path.join(tmpDir, await dl.suggestedFilename());
      await dl.saveAs(fp);
      return fp;
    } catch {
      // maybe AJAX blob; click anyway and wait a beat
      await loc.first().click({ delay: 50 }).catch(() => {});
      await page.waitForTimeout(2000);
      if (sniffedPath) return sniffedPath;
      return null;
    }
  }

  let saved = await clickByExactText(page);
  if (!saved) {
    for (const f of frames) {
      if (f === page) continue;
      saved = await clickByExactText(f);
      if (saved) break;
    }
  }

  if (!saved) {
    await page.screenshot({ path: 'page.png', fullPage: true });
    console.log('DEBUG_FILENAMES_BEGIN');
    console.log(JSON.stringify({ tried: targetName, allNames }, null, 2));
    console.log('DEBUG_FILENAMES_END');
    await browser.close();
    throw new Error('Clicked filename but no file captured (no download event and no AJAX blob).');
  }

  await browser.close();
  return saved;
}

// ---- Main ----
(async () => {
  const filePath = await downloadForecast();
  const hash = sha256File(filePath);
  const ext = path.extname(filePath).toLowerCase();

  let series = [];
  try {
    series = (ext === '.csv') ? parseCSV_amil_wvpa(filePath) : parseExcelFlexible(filePath);
  } catch (err) {
    await fetch(MAKE_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        source: 'github_actions',
        error: { message: String(err), stage: 'parse|schema' },
        file_name: path.basename(filePath),
        file_sha256: hash,
        portal_url: PORTAL_URL,
      }),
    });
    throw err;
  }

  const now = new Date();
  const windowEnd = new Date(now.getTime() + HOURS_LOOKAHEAD * 3600 * 1000);

  const flagged = [];
  const raw = [];
  for (const r of series) {
    const ts = new Date(r.ts_utc);
    if (ts >= now && ts <= windowEnd) {
      raw.push({ ts_utc: r.ts_utc, local: toLocalISO(ts, TIMEZONE), price: r.price });
      if (r.price >= PRICE_THRESHOLD) {
        flagged.push({ ts_utc: r.ts_utc, local: toLocalISO(ts, TIMEZONE), price: r.price });
      }
    }
  }

  const lastTs = series.length ? series[series.length - 1].ts_utc : toISOZ(now);
  const payload = {
    source: 'github_actions',
    idempotency_key: `${hash}_amilwvpa_${Date.parse(lastTs) / 1000}`,
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
    notes: ['clicked newest filename; supports native & AJAX downloads'],
  };

  await fetch(MAKE_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });

  console.log(`posted to Make: flagged=${flagged.length}, rows=${series.length}`);
})().catch(e => { console.error(e); process.exit(1); });
