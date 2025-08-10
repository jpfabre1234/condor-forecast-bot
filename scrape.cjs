// scrape.cjs — selector-robust: avoids checkbox; tries multiple download paths.
// On failure, saves debug artifacts (PNG + HTML) for inspection.

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

// ------- ENV / CONFIG -------
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL; // required
const PORTAL_URL = process.env.PORTAL_URL;             // required

const ROW_SELECTOR = process.env.ROW_SELECTOR || "table tbody tr";
const DOWNLOAD_BTN_SELECTOR =
  process.env.DOWNLOAD_BTN_SELECTOR ||
  'button:has-text("Descargar"), [aria-label*="Descargar"], button:has-text("Download")';

const FORCE_LAST_ROW = process.env.FORCE_LAST_ROW === "1"; // skip timestamp parsing
const DEBUG_SELECTORS = process.env.DEBUG_SELECTORS === "1";

const PRICE_THRESHOLD = Number(process.env.PRICE_THRESHOLD || "80");
const MAX_ROWS = Number(process.env.MAX_ROWS || "48");
const TIMEZONE = process.env.TIMEZONE || "America/Chicago";
const SHEET_NAME = process.env.SHEET_NAME || "AMIL.WVPA";

// ---------- helpers ----------
async function scrollUntilStable(page, rowLocator) {
  let prev = await rowLocator.count();
  for (let i = 0; i < 30; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(350);
    const now = await rowLocator.count();
    if (DEBUG_SELECTORS) console.log(`[debug] rows: ${now}`);
    if (now === prev) break;
    prev = now;
  }
}

function parseAnyTimestamp(text) {
  const patterns = [
    { re: /(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/, order: "DMY" },
    { re: /(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/, order: "YMD" },
    { re: /(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/, order: "MDY" },
  ];
  for (const p of patterns) {
    const m = text.match(p.re);
    if (m) {
      let yyyy, mm, dd, HH, MM, SS;
      if (p.order === "DMY") [ , dd, mm, yyyy, HH, MM, SS ] = m;
      else if (p.order === "YMD") [ , yyyy, mm, dd, HH, MM, SS ] = m;
      else [ , mm, dd, yyyy, HH, MM, SS ] = m;
      return new Date(`${yyyy}-${mm}-${dd}T${HH}:${MM}:${SS}Z`).getTime();
    }
  }
  return null;
}

async function pickRowIndex(page) {
  const rows = page.locator(ROW_SELECTOR);
  await rows.first().waitFor({ state: "visible" });
  await scrollUntilStable(page, rows);
  const count = await rows.count();
  if (count === 0) throw new Error("No rows found on portal.");

  if (FORCE_LAST_ROW) {
    if (DEBUG_SELECTORS) console.log("[debug] FORCE_LAST_ROW=1 → using last row.");
    return { index: count - 1, count, pickedBy: "forced-last" };
  }

  // Try timestamps; fall back to last if none parse.
  let bestIdx = -1, bestEpoch = -1;
  for (let i = 0; i < count; i++) {
    const txt = await rows.nth(i).innerText().catch(() => "");
    const epoch = parseAnyTimestamp(txt || "");
    if (epoch != null && epoch > bestEpoch) { bestEpoch = epoch; bestIdx = i; }
  }
  if (bestIdx < 0) {
    console.warn("[warn] No parsable timestamps; using last row.");
    return { index: count - 1, count, pickedBy: "fallback-last" };
  }
  if (bestIdx !== count - 1) console.warn(`[warn] Newest by time = ${bestIdx}, last = ${count - 1}. Proceeding.`);
  return { index: bestIdx, count, pickedBy: "timestamp" };
}

async function dumpDebug(page, tag) {
  try {
    await page.screenshot({ path: `debug-${tag}.png`, fullPage: true });
    fs.writeFileSync(`debug-${tag}.html`, await page.content());
    const lastHTML = await page.locator(ROW_SELECTOR).last().innerHTML().catch(() => null);
    if (lastHTML) fs.writeFileSync("debug-last-row.html", lastHTML);
  } catch (e) {
    console.log("debug dump failed:", e.message);
  }
}

async function clickAndDownload(page, clickable) {
  const [download] = await Promise.all([
    page.waitForEvent("download", { timeout: 45000 }),
    clickable.click({ timeout: 15000 })
  ]);
  return download;
}

async function downloadForRow(page, row) {
  // 1) Any CSV link inside the row?
  let link = row.locator('a[href$=".csv"], a[href*=".csv"]');
  if (await link.count()) return await clickAndDownload(page, link.last());

  // 2) Row "Descargar/Download" button?
  let rowBtn = row.locator('button:has-text("Descargar"), [aria-label*="Descargar"], button:has-text("Download")');
  if (await rowBtn.count()) return await clickAndDownload(page, rowBtn.first());

  // 3) Any CSV link on the whole page?
  link = page.locator('a[href$=".csv"], a[href*=".csv"]').last();
  if (await link.count()) return await clickAndDownload(page, link);

  // 4) Global Download button?
  const globalBtn = page.locator(DOWNLOAD_BTN_SELECTOR).first();
  if (await globalBtn.count()) return await clickAndDownload(page, globalBtn);

  throw new Error("No CSV link/button found to download.");
}

function parseCsv(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (!lines.length) throw new Error("CSV empty");
  const header = lines[0].split(",").map(s => s.trim().replace(/^"|"$/g, ""));
  const dateIdx = header.indexOf("date");
  const heIdx = header.indexOf("he");
  const forecastIdx = header.indexOf("forecast");
  if (dateIdx < 0 || heIdx < 0 || forecastIdx < 0) {
    throw new Error(`CSV header missing required columns. Got: ${header.join(", ")}`);
  }
  const rows = [];
  const limit = Math.min(MAX_ROWS, lines.length - 1);
  for (let i = 1; i <= limit; i++) {
    const cols = lines[i].split(",").map(s => s.trim().replace(/^"|"$/g, ""));
    if (cols.length < header.length) continue;
    rows.push({ date: cols[dateIdx], he: Number(cols[heIdx]), forecast: Number(cols[forecastIdx]) });
  }
  return { header, rows };
}

function buildMessage(fileName, parsed, threshold) {
  const first24 = parsed.rows.slice(0, 24);
  const hits24 = first24.filter(r => r.forecast > threshold);
  const hitsList = hits24.map(r => `${String(r.he).padStart(2, "0")}:00 → ${r.forecast.toFixed(2)}`);

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
      const mark = r.forecast > threshold ? " ⚠️" : "";
      lines.push(`- ${String(r.he).padStart(2, "0")}:00: ${r.forecast}${mark}`);
    }
    lines.push("");
  }

  const topList = hitsList.length
    ? `Above threshold in first 24: ${hitsList.join(", ")}`
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

    const pick = await pickRowIndex(page);
    console.log(`Rows: ${pick.count}. Using row idx: ${pick.index} (${pick.pickedBy}).`);
    const row = page.locator(ROW_SELECTOR).nth(pick.index);

    let download;
    try {
      download = await downloadForRow(page, row);
    } catch (e) {
      await dumpDebug(page, "download-failure");
      throw e;
    }

    const suggested = download.suggestedFilename();
    const tmp = path.join(process.cwd(), suggested);
    await download.saveAs(tmp);
    console.log("Downloaded:", tmp);

    const csvText = fs.readFileSync(tmp, "utf8");
    const parsed = parseCsv(csvText);
    const { text: formatted, hits24Count, topList } = buildMessage(suggested, parsed, PRICE_THRESHOLD);

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

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
