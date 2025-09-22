// scripts/sync-last-3-days.js
import fetch from "node-fetch";

// Vaste Vercel URL
const BASE_URL = "https://databowl-webhook.vercel.app";

// Config defaults
const DEFAULT_SINCE = "2025-09-15"; // fallback
const BATCH = 500; // rows per batch
const PAGES = 10;  // max pages per API-call

// Argument parsing
function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--since" && args[i + 1]) {
      out.since = args[i + 1];
      i++;
    }
  }
  return out;
}

async function run() {
  const { since } = parseArgs();
  const START_DATE = since || DEFAULT_SINCE;

  let cursor = null;
  let totalSynced = 0;
  let round = 0;

  while (true) {
    round++;
    const url = new URL(`${BASE_URL}/api/etl-sync`);
    url.searchParams.set("batch", BATCH);
    url.searchParams.set("pages", PAGES);

    if (cursor) {
      url.searchParams.set("cursor", cursor);
    } else {
      url.searchParams.set("since", START_DATE);
    }

    console.log(`➡️  Run #${round}: ${url.toString()}`);

    const r = await fetch(url.toString());
    const body = await r.json();
    if (!r.ok) {
      throw new Error(`HTTP ${r.status}: ${JSON.stringify(body)}`);
    }

    const synced = body.synced || 0;
    totalSynced += synced;
    console.log(`   Synced this run: ${synced}, total so far: ${totalSynced}`);

    if (body.has_more) {
      cursor = body.next_cursor;
      console.log("   More data available, continue with next cursor…");
    } else {
      console.log("✅ Klaar! Alles gesynct.");
      break;
    }
  }

  console.log(`Totaal gesynct: ${totalSynced} rows sinds ${START_DATE}`);
}

run().catch((err) => {
  console.error("❌ Fout bij sync:", err);
  process.exit(1);
});
