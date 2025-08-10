// scrape.js — Condor Bot (CSV → curtailment list)
// - Downloads latest CSV (DIRECT_CSV_URL or portal page)
// - Reads FIRST 48 rows only
// - Groups by date; prints one line per HE: "- HH:00: price", 🚨 when >= threshold
// - Builds plain-text message (tg_text) and posts JSON to MAKE_WEBHOOK_URL
//
// Env you can set in the workflow:
//   MAKE_WEBHOOK_URL    (required)
//   DIRECT_CSV_URL      (optional: skip browser; GET this URL)
//   PORTAL_URL          (optional: page where the files are listed)
//   LINK_SELECTOR       (default: 'a[href$=".csv"], a[href$=".CSV"]')
//   ROW_CHECKBOX_SELECTOR, DOWNLOAD_BTN_SELECTOR  (if your portal needs checkbox+Descargar)
//   PRICE_THRESHOLD     (default "80")
//   MAX_ROWS            (default "48")
//   BYPASS_DEDUPE       ("1" lets you resend same file during tests)
//   TIMEZONE            (label only, e.g., "America/Chicago")
//   SHEET_NAME          (label only, e.g., "AMIL.WVPA")
import { chromium } from 'playwright';
import crypto from 'crypto';

const MUST = (k) => {
  const v = process.env[k];
  if (!v) { console.error(`Missing env ${k}`); process.exit(2); }
  return v;
};

const MAKE_WEBHOOK_URL = MUST('MAKE_WEBHOOK_URL');

const DIRECT_CSV_URL = process.env.DIRECT_CSV_URL || '';
const PORTAL_URL     = process.env.PORTAL_URL     || '';
const LINK_SELECTOR  = process.env.LINK_SELECTOR  || 'a[href$=".csv"], a[href$=".CSV"]';

const ROW_CHECKBOX_SELECTOR = process.env.ROW_CHECKBOX_SELECTOR || ''; // e.g. 'tr:first-child input[type="checkbox"]'
const DOWNLOAD_BTN_SELECTOR = process.env.DOWNLOAD_BTN_SELECTOR || ''; // e.g. 'button:has-text("Descargar")'

const PRICE_THRESHOLD = Number(process.env.PRICE_THRESHOLD || '80');
const MAX_ROWS        = Number(process.env.MAX_ROWS || '48');
const TIMEZONE_LABEL  = process.env.TIMEZONE || 'America/Chicago';
const SHEET_NAME      = process.env.SHEET_NAME || 'AMIL.WVPA';
const HEADLESS        = (process.env.HEADLESS || 'true').toLowerCase() !== 'false';

// ---------- helpers ----------
const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const nowIso = () => new Date().toISOString();

function splitCsvLine(line) {
  const out = [];
  let cur = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (q && line[i + 1] === '"') { cur += '"'; i++; } else { q = !q; }
      continue;
    }
    if (ch === ',' && !q) { out.push(cur); cur = ''; } else { cur += ch; }
  }
  out.push(cur);
  return out;
}

function parseCsvFirst48(csvText, limit = 48) {
  const lines = csvText.replace(/^\uFEFF/, '').split(/\r?\n/).filter(l => l.trim().length);
  if (lines.length < 2) return { header: [], groups: new Map(), list: [], flaggedCount: 0 };

  const header = splitCsvLine(lines[0]).map(s => s.trim());
  const iDate = header.indexOf('date');
  const iHe   = header.indexOf('he');
  const iFc   = header.indexOf('forecast');
  if (iDate < 0 || iHe < 0 || iFc < 0) {
    throw new Error(`CSV missing columns (need date, he, forecast). Got: ${header.join(', ')}`);
  }

  const groups = new Map(); // date -> rows
  const list = [];
  let flaggedCount = 0;

  const take = Math.min(limit, lines.length - 1);
  for (let r = 1; r <= take; r++) {
    const cols = splitCsvLine(lines[r]);
    const date = (cols[iDate] ?? '').trim();
    const he   = Number((cols[iHe] ?? '').trim());
    const fc   = Number((cols[iFc] ?? '').trim());
    if (!date || Number.isNaN(he) || Number.isNaN(fc)) continue;

    const hh = String(he).padStart(2, '0') + ':00';
    const alert = fc >= PRICE_THRESHOLD;
    if (alert) flaggedCount++;

    const rec = { date, he, hh, forecast: fc, alert };
    list.push(rec);
    if (!groups.has(date)) groups.set(date, []);
    groups.get(date).push(rec);
  }

  return { header, groups, list, flaggedCount };
}

function buildCurtailmentText(fileName, analysis) {
  let out = `${fileName}\n\n`;
  out += `${analysis.flaggedCount} hours require curtailment on the forecasted prices.\n`;
  for (const [date, rows] of analysis.groups) {
    out += `\nDate ${date};\n`;
    for (const r of rows) {
      out += `- ${r.hh}: ${r.forecast.toFixed(2)}${r.alert ? ' 🚨' : ''}\n`;
    }
  }
  return out.trim();
}

async function postToMake(payload) {
  const res = await fetch(MAKE_WEBHOOK_URL, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`Make POST ${res.status}: ${await res.text().catch(()=> '')}`);
}

// ---------- downloader ----------
async function downloadCsvViaDirect(context, url) {
  const page = await context.newPage();
  const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
  if (!resp || !resp.ok()) throw new Error(`DIRECT_CSV_URL failed (${resp && resp.status()})`);
  const text = await resp.text();
  const fileName = inferNameFromUrl(url);
  await page.close();
  return { text, fileName };
}
function inferNameFromUrl(url) {
  try { const u = new URL(url); const last = u.pathname.split('/').filter(Boolean).pop() || 'file.csv'; return decodeURIComponent(last); }
  catch { return 'file.csv'; }
}

async function downloadCsvFromPortal(context) {
  if (!PORTAL_URL) throw new Error('No DIRECT_CSV_URL and no PORTAL_URL set.');
  const page = await context.newPage();
  await page.goto(PORTAL_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });

  // try direct anchor to .csv
  const a = await page.$(LINK_SELECTOR);
  if (a) {
    const href = await a.getAttribute('href');
    if (href && /^https?:/i.test(href)) {
      const r = await page.request.get(href);
      if (!r.ok()) throw new Error(`CSV GET failed (${r.status()})`);
      const text = await r.text();
      await page.close();
      return { text, fileName: inferNameFromUrl(href) };
    }
  }

  // try checkbox + Descargar pattern if selectors provided
  if (ROW_CHECKBOX_SELECTOR && DOWNLOAD_BTN_SELECTOR) {
    const cb = await page.$(ROW_CHECKBOX_SELECTOR);
    if (!cb) throw new Error('ROW_CHECKBOX_SELECTOR not found on page.');
    await cb.click({ force: true });
    const [dl] = await Promise.all([
      page.waitForEvent('download', { timeout: 15000 }).catch(() => null),
      page.click(DOWNLOAD_BTN_SELECTOR, { delay: 40 }).catch(()=> null)
    ]);
    if (!dl) {
      await page.close();
      throw new Error('Clicked download but no file captured. Adjust selectors.');
    }
    const fileName = dl.suggestedFilename() || 'file.csv';
    const stream = await dl.createReadStream();
    const text = await streamToString(stream);
    await page.close();
    return { text, fileName };
  }

  await page.close();
  throw new Error('No CSV link found. Set DIRECT_CSV_URL or provide ROW_CHECKBOX_SELECTOR & DOWNLOAD_BTN_SELECTOR.');
}

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    let data = '';
    stream.setEncoding('utf8');
    stream.on('data', (c) => data += c);
    stream.on('end', () => resolve(data));
    stream.on('error', reject);
  });
}

// ---------- main ----------
(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  const ctx = await browser.newContext({ acceptDownloads: true });

  try {
    // 1) get CSV text + fileName
    let csvText = '', fileName = '';
    if (DIRECT_CSV_URL) {
      ({ text: csvText, fileName } = await downloadCsvViaDirect(ctx, DIRECT_CSV_URL));
    } else {
      ({ text: csvText, fileName } = await downloadCsvFromPortal(ctx));
    }
    console.log(`Chosen file: ${fileName}`);

    // 2) analyze first 48 rows
    const analysis = parseCsvFirst48(csvText, MAX_ROWS);
    if (analysis.list.length === 0) {
      throw new Error('Parsed 0 usable rows (need columns: date, he, forecast).');
    }

    const tg_text = buildCurtailmentText(fileName, analysis);

    // 3) idempotency (bypass for tests if BYPASS_DEDUPE=1)
    const idemBase = (process.env.BYPASS_DEDUPE === '1')
      ? `${fileName}|${Date.now()}|${crypto.randomUUID()}`
      : sha256(csvText); // same content → same key
    const idempotency_key = sha256(String(idemBase));

    // 4) payload
    const payload = {
      source: 'github_actions',
      portal_url: PORTAL_URL || DIRECT_CSV_URL || '',
      file_name: fileName,
      idempotency_key,
      threshold: PRICE_THRESHOLD,
      timezone: TIMEZONE_LABEL,
      sheet: SHEET_NAME,
      generated_at_utc: nowIso(),
      first48_count: analysis.list.length,
      flagged: analysis.flaggedCount,   // number >= threshold in first 48
      rows: analysis.list,              // [{date, he, hh, forecast, alert}]
      tg_text                           // ← map this to Telegram Text (Parse mode: None)
    };

    // 5) post
    await postToMake(payload);
    console.log(`posted to Make: flagged=${analysis.flaggedCount}, rows=${analysis.list.length}`);
  } catch (err) {
    console.error('Error:', err?.message || err);
    process.exit(1);
  } finally {
    await ctx.close();
    await browser.close();
  }
})();

