// scrape.js - Condor Forecast Bot
// - Opens the portal (or uses DIRECT_CSV_URL)
// - Gets latest CSV contents
// - Parses rows { ts_utc, local, price }
// - Computes flagged (first 24 >= threshold)
// - Builds two lists for Telegram:
//   * first 24 >= threshold (plain text)
//   * full CSV view as lines with header (also MD code block)
// - POSTs payload to Make webhook

import { chromium } from 'playwright';
import crypto from 'crypto';

// ------------------------
// Config (ENV overrides)
// ------------------------
const MAKE_WEBHOOK_URL = mustEnv('MAKE_WEBHOOK_URL');

const DIRECT_CSV_URL   = process.env.DIRECT_CSV_URL || '';
const PORTAL_URL       = process.env.PORTAL_URL     || '';
const LINK_SELECTOR    = process.env.LINK_SELECTOR  || 'a[href$=".csv"], a[href$=".CSV"]';

const PRICE_THRESHOLD  = Number(process.env.PRICE_THRESHOLD || '80');     // example default
const SHEET_NAME       = process.env.SHEET_NAME || 'AMIL.WVPA';
const TIMEZONE_LABEL   = process.env.TIMEZONE || 'America/Chicago';

// For pages that require selecting a row then clicking a download button.
// (Your portal fits this pattern; leave blank if not needed.)
const ROW_CHECKBOX_SELECTOR   = process.env.ROW_CHECKBOX_SELECTOR || '';  // e.g. 'tr:first-child td input[type="checkbox"]'
const DOWNLOAD_BTN_SELECTOR   = process.env.DOWNLOAD_BTN_SELECTOR || '';  // e.g. 'button:has-text("Descargar")'

// Debug
const HEADLESS = (process.env.HEADLESS || 'true').toLowerCase() !== 'false';

// Limit for Telegram inline code block (4096 hard cap – keep it safe)
const MAX_TG_INLINE = 3000;

// ------------------------
// Utilities
// ------------------------
function mustEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing required env: ${name}`);
    process.exit(1);
  }
  return v;
}

function sha256(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

function nowIso() {
  return new Date().toISOString();
}

function clip(s, keep = MAX_TG_INLINE) {
  return s.length <= keep ? s : s.slice(0, keep) + '\n…(truncated)';
}

// CSV line splitter that handles basic quoted fields
function splitCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      // doubled quote inside quoted field
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

// Parse CSV -> { header:[], rows: [] of objects keyed by header }
function parseCsv(text) {
  // handle BOM + normalize
  let t = text.replace(/^\uFEFF/, '');
  const lines = t.split(/\r?\n/).filter(l => l.trim().length > 0);

  if (lines.length === 0) return { header: [], rows: [] };

  const header = splitCsvLine(lines[0]).map(h => h.trim());
  const idxMap = new Map(header.map((h, i) => [h.toLowerCase(), i]));

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = splitCsvLine(lines[i]);
    const obj = {};
    for (let c = 0; c < header.length; c++) obj[header[c]] = parts[c] ?? '';
    rows.push(obj);
  }

  return { header, headerLowerIdx: idxMap, rows };
}

function pickField(obj, headerIdx, name) {
  const i = headerIdx.get(name.toLowerCase());
  if (i == null) return undefined;

  // Find actual header name at that position
  const keys = Object.keys(obj);
  const key = keys[i] ?? name;
  return obj[key];
}

// Map CSV rows -> normalized { ts_utc, local, price }
function normalizeRows(csv) {
  const out = [];
  for (const r of csv.rows) {
    const ts_utc = pickField(r, csv.headerLowerIdx, 'ts_utc') ?? pickField(r, csv.headerLowerIdx, 'utc') ?? '';
    const local  = pickField(r, csv.headerLowerIdx, 'local')  ?? pickField(r, csv.headerLowerIdx, 'local_time') ?? '';
    const priceS = pickField(r, csv.headerLowerIdx, 'price')  ?? pickField(r, csv.headerLowerIdx, 'value') ?? '';
    const price  = Number(String(priceS).replace(/,/g, '').trim());

    // Skip if missing timestamp or price
    if (!ts_utc || !isFinite(price)) continue;

    out.push({ ts_utc, local, price });
  }
  return out;
}

function toLine(rec) {
  const p = Number(rec.price);
  return `${rec.ts_utc}, ${rec.local || ''}, ${isFinite(p) ? p.toFixed(2) : rec.price}`;
}

// ------------------------
// Portal fetching helpers
// ------------------------
async function fetchCsvViaDirectUrl(context, url) {
  const page = await context.newPage();
  const resp = await page.goto(url, { waitUntil: 'domcontentloaded' });
  if (!resp || !resp.ok()) throw new Error(`Failed to fetch CSV via DIRECT_CSV_URL (${resp && resp.status()})`);
  const text = await resp.text();
  await page.close();
  return { text, fileName: inferFileNameFromUrl(url) };
}

function inferFileNameFromUrl(url) {
  try {
    const u = new URL(url);
    const last = u.pathname.split('/').filter(Boolean).pop() || 'file.csv';
    return decodeURIComponent(last);
  } catch {
    return 'file.csv';
  }
}

async function fetchCsvFromPortal(context) {
  if (!PORTAL_URL) throw new Error('No PORTAL_URL configured and DIRECT_CSV_URL is empty.');

  const browserPage = await context.newPage();
  await browserPage.goto(PORTAL_URL, { waitUntil: 'domcontentloaded' });

  // Strategy A: a real <a href="*.csv"> link (LINK_SELECTOR)
  const linkHandle = await browserPage.$(LINK_SELECTOR);
  if (linkHandle) {
    const href = await linkHandle.getAttribute('href');
    if (href && /^https?:/i.test(href)) {
      const fileName = inferFileNameFromUrl(href);
      const res = await browserPage.request.get(href);
      if (!res.ok()) throw new Error(`Failed to GET CSV link (${res.status()})`);

      const text = await res.text();
      await browserPage.close();
      return { text, fileName };
    }
  }

  // Strategy B: select row checkbox then click "Descargar" (portal UI like de.acespower.com)
  if (ROW_CHECKBOX_SELECTOR && DOWNLOAD_BTN_SELECTOR) {
    await browserPage.waitForTimeout(500); // slight settle

    const checkbox = await browserPage.$(ROW_CHECKBOX_SELECTOR);
    if (!checkbox) throw new Error('Row checkbox not found. Adjust ROW_CHECKBOX_SELECTOR.');
    await checkbox.click({ force: true });

    const [download] = await Promise.all([
      browserPage.waitForEvent('download', { timeout: 10000 }).catch(() => null),
      browserPage.click(DOWNLOAD_BTN_SELECTOR, { delay: 50 })
    ]);

    if (!download) {
      throw new Error('Clicked download but no file captured (no download event / AJAX blob).');
    }

    const path = await download.path();
    const fileName = download.suggestedFilename() || 'file.csv';
    const text = await download.createReadStream().then(streamToString);
    await browserPage.close();
    return { text, fileName };
  }

  await browserPage.close();
  throw new Error('No CSV/XLSX links found. Adjust LINK_SELECTOR or provide DIRECT_CSV_URL (or set ROW_CHECKBOX_SELECTOR + DOWNLOAD_BTN_SELECTOR).');
}

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    let data = '';
    stream.setEncoding('utf8');
    stream.on('data', chunk => (data += chunk));
    stream.on('end', () => resolve(data));
    stream.on('error', reject);
  });
}

// ------------------------
// Make posting
// ------------------------
async function postToMake(payload) {
  const res = await fetch(MAKE_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error(`Make webhook POST failed: ${res.status} ${t.slice(0, 400)}`);
  }
}

// ------------------------
// Main
// ------------------------
(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext();

  try {
    // 1) Get CSV
    let csvText = '';
    let fileName = '';

    if (DIRECT_CSV_URL) {
      ({ text: csvText, fileName } = await fetchCsvViaDirectUrl(context, DIRECT_CSV_URL));
    } else {
      ({ text: csvText, fileName } = await fetchCsvFromPortal(context));
    }

    console.log(`Chosen file: ${fileName}`);

    // 2) Parse & normalize
    const csv = parseCsv(csvText);
    const normRows = normalizeRows(csv);

    if (normRows.length === 0) {
      throw new Error('Parsed 0 data rows (columns must include ts_utc/utc and price/value).');
    }

    // 3) Metrics / window
    const rowsEvaluated = normRows.length;
    const generatedAtUtc = nowIso();

    // interval guess (minutes) from first two rows
    let intervalMinutes = 0;
    if (normRows.length > 1) {
      const t0 = Date.parse(normRows[0].ts_utc);
      const t1 = Date.parse(normRows[1].ts_utc);
      if (isFinite(t0) && isFinite(t1) && t1 > t0) {
        intervalMinutes = Math.round((t1 - t0) / 60000);
      }
    }

    const windowStartUtc = normRows[0].ts_utc;
    const windowEndUtc   = normRows[normRows.length - 1].ts_utc;

    // 4) First 24 rows >= threshold
    const first24Over = normRows.slice(0, 24).filter(r => Number(r.price) >= PRICE_THRESHOLD);

    // 5) Build the lists
    const listHeader = 'ts_utc, local, price';

    const first24Text = first24Over.map(toLine).join('\n');

    const fullListRaw = [listHeader, ...normRows.map(toLine)].join('\n');
    const fullListMd  = '```\n' + clip(fullListRaw, MAX_TG_INLINE) + '\n```';

    // 6) Hash & idempotency
    const fileHash = sha256(csvText);
    const idempotencyKey =
      process.env.BYPASS_DEDUPE === '1'
        ? sha256(fileName + '|' + generatedAtUtc + '|' + Math.random())
        : fileHash;


    // 7) Build payload for Make
    const payload = {
      source: 'github_actions',
      idempotency_key: idempotencyKey,
      file_name: fileName,
      file_sha256: fileHash,
      portal_url: PORTAL_URL || DIRECT_CSV_URL || '',
      sheet: SHEET_NAME,
      timezone: TIMEZONE_LABEL,
      generated_at_utc: generatedAtUtc,
      window_start_utc: windowStartUtc,
      window_end_utc: windowEndUtc,
      threshold: PRICE_THRESHOLD,
      interval_minutes: intervalMinutes,
      rows_evaluated: rowsEvaluated,

      // your raw data if Make needs it
      raw_intervals: normRows,            // array of { ts_utc, local, price }

      // NEW fields for Telegram
      first24_count: first24Over.length,  // number of first-24 rows above threshold
      first24_text: first24Text,          // plain list of those lines
      full_list_text: fullListRaw,        // full CSV-as-lines (for file upload)
      full_list_text_md: fullListMd,      // Markdown code block (inline message)
    };

    // 8) flagged (to keep your existing filters in Make working)
    //    We'll include "flagged" as an array of the first-24 hits:
    payload.flagged = first24Over.map(r => ({ ts_utc: r.ts_utc, local: r.local, price: r.price }));

    // 9) POST to Make
    await postToMake(payload);

    console.log(`posted to Make: flagged=${payload.flagged.length}, rows=${rowsEvaluated}`);
  } catch (err) {
    console.error('Error:', err?.message || err);
    process.exit(1);
  } finally {
    await context.close();
    await browser.close();
  }
})();
