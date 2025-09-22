// /api/sync.js
import fetch from "node-fetch";

const BASE_URL = "https://databowl-webhook.vercel.app";
const SUPABASE_URL = process.env.SUPABASE_URL?.replace(/\/+$/, "");
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

// Config
const BATCH = 500;
const PAGES = 10;

// Functie om ETL sync te draaien
async function runSync() {
  let cursor = null;
  let total = 0;

  while (true) {
    const url = new URL(`${BASE_URL}/api/etl-sync`);
    url.searchParams.set("batch", BATCH);
    url.searchParams.set("pages", PAGES);
    if (cursor) url.searchParams.set("cursor", cursor);

    const r = await fetch(url.toString());
    const body = await r.json();
    if (!r.ok) throw new Error(`ETL error: ${JSON.stringify(body)}`);

    total += body.synced || 0;
    if (body.has_more) {
      cursor = body.next_cursor;
    } else break;
  }

  return total;
}

// Functie om Supabase materialized views te refreshen
async function refreshSupabaseViews() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE");
  }

  // Voorbeeld: meerdere views refreshen
  const views = ["lead_uniques_day_tot", "lead_uniques_day_grp"];
  for (const view of views) {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/refresh_view`, {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ view_name: view }),
    });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(`Failed to refresh ${view}: ${txt}`);
    }
  }
}

export default async function handler(req, res) {
  try {
    const total = await runSync();
    await refreshSupabaseViews();

    res.status(200).json({
      ok: true,
      message: `Sync complete, ${total} rows toegevoegd en views ververst.`,
    });
  } catch (err) {
    console.error("[sync] error", err);
    res.status(500).json({ error: err.message || String(err) });
  }
}
