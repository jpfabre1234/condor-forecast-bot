// scrape.js — drop-in replacement
// Robust: loads all rows, picks newest by timestamp (DD-MM-YYYY HH:mm:ss), verifies it's last row, downloads, parses CSV, posts to Make.

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

// ------- ENV / CONFIG -------
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL; // required
const PORTAL_URL = process.env.PORTAL_URL;             // required

const ROW_SELECTOR = process.env.ROW_SELECTOR || "table tbody tr";
const ROW_CHECKBOX_SELECTOR = process.env.ROW_CHECKBOX_SELECTOR || 'input[type="checkbox"]';
const DOWNLOAD_BTN_SELECTOR = process.env.DOWNLOAD_BTN_SELECTOR || 'button:has-text("Descargar"), [aria-label="Descargar"]';

const PRICE_THRESHOLD = Number(process.env.PRICE_THRESHOLD || "80");
const MAX_ROWS = Number(process.env.MAX_ROWS || "48");
const TIMEZONE = process.env.TIMEZONE || "America/Chicago";
const SHEET_NAME = process.env.SHEET_NAME || "AMIL.WVPA";

const DEBUG_SELECTORS = process.env.DEBUG_SELECTORS === "1";

// Timestamp text on portal looks like: "10-08-2025 09:33:05"
function parsePortalTimestamp(tsText) {
  const m = tsText.match(/(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  const [ , dd, mm, yyyy, HH, MM, SS ] = m;
  // Build UTC date for comparison (treat as local if portal local; relative order still fine)
  return new Date(`${yyyy}-${mm}-${dd}T${HH}:${MM}:${SS}Z`).getTime();
}

async function scrollUntilStable(page, rowLocator) {
  let prevCount = await rowLocator.count();
  for (let i = 0; i < 30; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(400);
    const now = await rowLocator.count();
    if (DEBUG_SELECTORS) console.log(`[debug] rows: ${now}`);
    if (now === prevCount) break;
    prevCount = now;
  }
}

async function findNewestRowIndex(page) {
  const rows = page.locator(ROW_SELECTOR);
  await rows.first().waitFor({ state: "visible" });
  await scrollUntilStable(page, rows);

  const count = await rows.count();
  if (count === 0) throw new Error("No rows found on portal.");

  // Extract a timestamp string per row by reading each row's full text and locating a dd-mm-yyyy hh:mm:ss pattern
  const ts = [];
  for (let i = 0; i < count; i++) {
    const txt = await rows.nth(i).innerText();
    const m = txt.match(/(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})/);
    ts.push(m ? m[1] : null);
  }

  if (DEBUG_SELECTORS) console.log("[debug] raw timestamps:", ts);

  // Parse and select max
  let bestIdx = -1;
  let bestEpoch = -1;
  for (let i = 0; i < ts.length; i++) {
    const epoch = ts[i] ? parsePortalTimestamp(ts[i]) : -1;
    if (epoch > bestEpoch) {
      bestEpoch = epoch;
      bestIdx = i;
    }
  }
  if (bestIdx < 0) throw new Error("Could not parse any timestamps on the page.");

  // Double-check: newest should be last (bottom) if list is ascending
  const expectedLast = count - 1;
  if (bestIdx !== expectedLast) {
    console.warn(`[warn] Newest row index ${bestIdx} != last row ${expectedLast}. Portal order may have changed. Proceeding with newest by time.`);
  } else if (DEBUG_SELECTORS) {
    console.log("[debug] Newest row is also the last row as expected.");
  }

  return { index: bestIdx, count, newestText: ts[bestIdx] };
}

function parseCsvText(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length === 0) throw new Error("CSV empty");

  // header: date,he,node,forecast
  const header = lines[0].split(",").map(s => s.trim().replace(/^"|"$/g, ''));
  const dateIdx = header.indexOf("date");
  const heIdx = header.indexOf("he");
  const nodeIdx = header.indexOf("node");
  const forecastIdx = header.indexOf("forecast");
  if (dateIdx < 0 || heIdx < 0 || forecastIdx < 0) {
    throw new Error(`CSV header missing required columns. Got: ${header.join(", ")}`);
  }

  const rows = [];
  const limit = Math.min(MAX_ROWS, lines.length - 1);
  for (let i = 1; i <= limit; i++) {
    const cols = lines[i].split(",").map(s => s.trim().replace(/^"|"$/g, ''));
    if (cols.length < header.length) continue;
    rows.push({
      date: cols[dateIdx],
      he: Number(cols[heIdx]),
      // node: cols[nodeIdx], // ignored by request
      forecast: Number(cols[forecastIdx]),
    });
  }
  return { header, rows };
}

function buildMessage(fileName, parsed) {
  // Count > threshold in first 24 rows
  const first24 = parsed.rows.slice(0, 24);
  const hits24 = first24.filter(r => r.forecast > PRICE_THRESHOLD);
  const hits24List = hits24.map(r => `${r.he}:00 → ${r.forecast.toFixed(2)}`);

  // Group by date (keep input order)
  const byDate = new Map();
  for (const r of parsed.rows) {
    if (!byDate.has(r.date)) byDate.set(r.date, []);
    byDate.get(r.date).push(r);
  }

  const lines = [];
  lines.push(`file: ${fileName}`);
  lines.push(`${hits24.length} hours require curtailment on the forecasted prices (first 24).`);
  for (const [date, arr] of byDate.entries()) {
    lines.push(`date ${date};`);
    for (const r of arr) {
      const alert = r.forecast > PRICE_THRESHOLD ? " ⚠️" : "";
      lines.push(`- ${String(r.he).padStart(2, "0")}:00: ${r.forecast}${alert}`);
    }
    lines.push(""); // blank between dates
  }

  // For Make/Telegram “notes” section: the list of >threshold (first 24).
  const topList = hits24List.length
    ? `Above threshold in first 24: ${hits24List.join(", ")}`
    : `Above threshold in first 24: none`;

  return { text: lines.join("\n"), hits24Count: hits24.length, topList };
}

async function main() {
  if (!MAKE_WEBHOOK_URL) throw new Error("MAKE_WEBHOOK_URL is required.");
  if (!PORTAL_URL) throw new Error("PORTAL_URL is required.");

  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();

  try {
    console.log("Opening portal:", PORTAL_URL);
    await page.goto(PORTAL_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForLoadState("networkidle", { timeout: 30000 });

    // Find newest row by timestamp
    const { index: newestIdx, count, newestText } = await findNewestRowIndex(page);
    console.log(`Rows: ${count}. Newest row idx: ${newestIdx} (${newestText || "no-ts"})`);

    const newestRow = page.locator(ROW_SELECTOR).nth(newestIdx);

    // Ensure checkbox is in view & click
    const checkbox = newestRow.locator(ROW_CHECKBOX_SELECTOR);
    await checkbox.waitFor({ state: "attached", timeout: 10000 });
    await checkbox.scrollIntoViewIfNeeded();
    await checkbox.click({ timeout: 10000 });

    // Click Descargar and capture the download
    const [download] = await Promise.all([
      page.waitForEvent("download", { timeout: 45000 }),
      page.locator(DOWNLOAD_BTN_SELECTOR).click({ timeout: 10000 }),
    ]);

    const suggested = download.suggestedFilename();
    const tmp = path.join(process.cwd(), suggested);
    await download.saveAs(tmp);
    console.log("Downloaded:", tmp);

    // Parse CSV
    const csvText = fs.readFileSync(tmp, "utf8");
    const parsed = parseCsvText(csvText);

    const { text: formatted, hits24Count, topList } = buildMessage(suggested, parsed);

    // Post to Make
    const payload = {
      source: "github_actions",
      sheet: SHEET_NAME,
      timezone: TIMEZONE,
      file_name: suggested,
      rows_evaluated: parsed.rows.length,
      threshold: PRICE_THRESHOLD,
      flagged: hits24Count,
      notes: [topList],
      formatted_text: formatted,
    };

    console.log("Posting to Make:", { flagged: hits24Count, rows: parsed.rows.length });
    const res = await fetch(MAKE_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    console.log("Make response:", res.status, await res.text());
  } finally {
    await browser.close();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
