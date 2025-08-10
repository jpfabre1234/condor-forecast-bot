// scrape.cjs — robust: picks newest row by timestamp when available,
// otherwise gracefully falls back to the last row. Then downloads CSV,
// formats it, and posts to Make.

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

// ------- ENV / CONFIG -------
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL; // required
const PORTAL_URL = process.env.PORTAL_URL;             // required

// Table & buttons
const ROW_SELECTOR = process.env.ROW_SELECTOR || "table tbody tr";
const ROW_CHECKBOX_SELECTOR = process.env.ROW_CHECKBOX_SELECTOR || 'input[type="checkbox"]';
const DOWNLOAD_BTN_SELECTOR =
  process.env.DOWNLOAD_BTN_SELECTOR ||
  'button:has-text("Descargar"), [aria-label="Descargar"]';

// Behavior
const FORCE_LAST_ROW = process.env.FORCE_LAST_ROW === "1"; // skip timestamp parsing
const DEBUG_SELECTORS = process.env.DEBUG_SELECTORS === "1";

// CSV interpretation
const PRICE_THRESHOLD = Number(process.env.PRICE_THRESHOLD || "80");
const MAX_ROWS = Number(process.env.MAX_ROWS || "48");
const TIMEZONE = process.env.TIMEZONE || "America/Chicago";
const SHEET_NAME = process.env.SHEET_NAME || "AMIL.WVPA";

// ----- timestamp parsing helpers -----

// Try multiple common patterns: "10-08-2025 09:33:05", "2025-08-10 09:33:05", "08/10/2025 09:33:05"
const TS_PATTERNS = [
  { re: /(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/, order: "DMY" },
  { re: /(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/, order: "YMD" },
  { re: /(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/, order: "MDY" },
];

function toEpoch(parts, order) {
  let yyyy, mm, dd, HH, MM, SS;
  if (order === "DMY") {
    [ , dd, mm, yyyy, HH, MM, SS ] = parts;
  } else if (order === "YMD") {
    [ , yyyy, mm, dd, HH, MM, SS ] = parts;
  } else {
    // MDY
    [ , mm, dd, yyyy, HH, MM, SS ] = parts;
  }
  // Use Z to avoid timezone ambiguity; we only need *ordering*
  return new Date(`${yyyy}-${mm}-${dd}T${HH}:${MM}:${SS}Z`).getTime();
}

function parseAnyTimestamp(text) {
  for (const p of TS_PATTERNS) {
    const m = text.match(p.re);
    if (m) return toEpoch(m, p.order);
  }
  return null;
}

async function scrollUntilStable(page, rowLocator) {
  let prev = await rowLocator.count();
  for (let i = 0; i < 30; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(350);
    const now = await rowLocator.count();
    if (DEBUG_SELECTORS) console.log(`[debug] rows now: ${now}`);
    if (now === prev) break;
    prev = now;
  }
}

async function findTargetRowIndex(page) {
  const rows = page.locator(ROW_SELECTOR);
  await rows.first().waitFor({ state: "visible" });
  await scrollUntilStable(page, rows);
  const count = await rows.count();
  if (count === 0) throw new Error("No rows found on portal.");

  // Optional: force last row (newest)
  if (FORCE_LAST_ROW) {
    if (DEBUG_SELECTORS) console.log("[debug] FORCE_LAST_ROW=1, picking last row only.");
    return { index: count - 1, count, pickedBy: "forced-last" };
  }

  // Try to pick by timestamp. Sample first few rows for debugging.
  if (DEBUG_SELECTORS) {
    const samples = Math.min(5, count);
    console.log(`[debug] sampling first ${samples} rows:`);
    for (let i = 0; i < samples; i++) {
      const t = (await rows.nth(i).innerText()).replace(/\s+/g, " ").trim();
      console.log(`  [${i}] ${t}`);
    }
    if (count > 5) {
      const j = count - 1;
      const t = (await rows.nth(j).innerText()).replace(/\s+/g, " ").trim();
      console.log(`  [last=${j}] ${t}`);
    }
  }

  // Extract a timestamp per row; if none, we will fall back to last row.
  let bestIdx = -1;
  let bestEpoch = -1;
  for (let i = 0; i < count; i++) {
    const txt = await rows.nth(i).innerText();
    const epoch = parseAnyTimestamp(txt || "");
    if (epoch != null && epoch > bestEpoch) {
      bestEpoch = epoch;
      bestIdx = i;
    }
  }

  if (bestIdx < 0) {
    console.warn("[warn] Could not parse any timestamps on the page; falling back to last row.");
    return { index: count - 1, count, pickedBy: "fallback-last" };
  }

  // If newest by time isn’t the last, warn but continue.
  if (bestIdx !== count - 1) {
    console.warn(`[warn] Newest by timestamp = row ${bestIdx}, but last row is ${count - 1}. Proceeding with timestamp pick.`);
  }

  return { index: bestIdx, count, pickedBy: "timestamp" };
}

// ----- CSV parsing + message building -----

function parseCsvText(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length === 0) throw new Error("CSV empty");

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
    rows.push({
      date: cols[dateIdx],
      he: Number(cols[heIdx]),
      forecast: Number(cols[forecastIdx]),
    });
  }
  return { header, rows };
}

function buildMessage(fileName, parsed) {
  // First 24 rows over threshold
  const first24 = parsed.rows.slice(0, 24);
  const hits24 = first24.filter(r => r.forecast > PRICE_THRESHOLD);
  const hits24List = hits24.map(r => `${String(r.he).padStart(2, "0")}:00 → ${r.forecast.toFixed(2)}`);

  // Group by date
  const byDate = new Map();
  for (const r of parsed.rows) {
    if (!byDate.has(r.date)) byDate.set(r.date, []);
    byDate.get(r.date).push(r);
  }

  const out = [];
  out.push(`file: ${fileName}`);
  out.push(`${hits24.length} hours require curtailment on the forecasted prices (first 24).`);
  for (const [date, arr] of byDate.entries()) {
    out.push(`date ${date};`);
    for (const r of arr) {
      const alert = r.forecast > PRICE_THRESHOLD ? " ⚠️" : "";
      out.push(`- ${String(r.he).padStart(2, "0")}:00: ${r.forecast}${alert}`);
    }
    out.push("");
  }

  const topList = hits24List.length
    ? `Above threshold in first 24: ${hits24List.join(", ")}`
    : `Above threshold in first 24: none`;

  return { text: out.join("\n"), hits24Count: hits24.length, topList };
}

// ----- main -----

async function main() {
  if (!MAKE_WEBHOOK_URL) throw new Error("MAKE_WEBHOOK_URL is required.");
  if (!PORTAL_URL) throw new Error("PORTAL_URL is required.");

  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();

  try {
    console.log("Opening portal:", PORTAL_URL);
    await page.goto(PORTAL_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForLoadState("networkidle", { timeout: 30000 });

    // Pick the row
    const pick = await findTargetRowIndex(page);
    console.log(`Rows: ${pick.count}. Using row idx: ${pick.index} (${pick.pickedBy}).`);

    const targetRow = page.locator(ROW_SELECTOR).nth(pick.index);

    // Select its checkbox
    const checkbox = targetRow.locator(ROW_CHECKBOX_SELECTOR);
    await checkbox.waitFor({ state: "attached", timeout: 15000 });
    await checkbox.scrollIntoViewIfNeeded();
    await checkbox.click({ timeout: 15000 });

    // Click Download and grab file
    const [download] = await Promise.all([
      page.waitForEvent("download", { timeout: 45000 }),
      page.locator(DOWNLOAD_BTN_SELECTOR).click({ timeout: 15000 }),
    ]);

    const suggested = download.suggestedFilename();
    const tmpPath = path.join(process.cwd(), suggested);
    await download.saveAs(tmpPath);
    console.log("Downloaded:", tmpPath);

    // Parse CSV and build message
    const csvText = fs.readFileSync(tmpPath, "utf8");
    const parsed = parseCsvText(csvText);
    const { text: formatted, hits24Count, topList } = buildMessage(suggested, parsed);

    // Send to Make
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

