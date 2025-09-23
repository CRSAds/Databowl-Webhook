// api/sync.js
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const BASE_URL = "https://databowl-webhook.vercel.app"; // jouw ETL endpoint

// helpers
function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

function startOfTodayISO() {
  const t = new Date();
  t.setHours(0, 0, 0, 0);
  return t.toISOString();
}

async function getLastSynced(client) {
  const { data, error } = await client
    .from("sync_runs")
    .select("last_synced_at")
    .order("id", { ascending: false })
    .limit(1)
    .single();
  if (error) {
    console.error("[sync] getLastSynced error", error);
    return null;
  }
  return data?.last_synced_at || null;
}

async function updateLastSynced(client, ts) {
  const { error } = await client.from("sync_runs").insert({ last_synced_at: ts });
  if (error) console.error("[sync] updateLastSynced error", error);
}

async function runEtlSync(since) {
  let cursor = null;
  let total = 0;
  let round = 0;

  while (true) {
    round++;
    const url = new URL(`${BASE_URL}/api/etl-sync`);
    url.searchParams.set("batch", "500");
    url.searchParams.set("pages", "10");
    if (cursor) {
      url.searchParams.set("cursor", cursor);
    } else {
      url.searchParams.set("since", since);
    }

    console.log(`[sync] Run #${round}: ${url.toString()}`);
    const r = await fetch(url.toString());
    const body = await r.json();
    if (!r.ok) throw new Error(`ETL failed ${r.status}: ${JSON.stringify(body)}`);

    total += body.synced || 0;
    if (body.has_more) {
      cursor = body.next_cursor;
    } else {
      break;
    }
  }
  return { synced: total };
}

async function refreshSupabaseViews(client) {
  try {
    const { error } = await client.rpc("refresh_materialized_views");
    if (error) throw error;
    console.log("[sync] ✅ Views refreshed");
  } catch (err) {
    if (String(err.message).includes("concurrently")) {
      console.warn("[sync] ⏳ Refresh skipped (already running)");
    } else {
      throw err;
    }
  }
}

// handler
export default async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET")
    return res.status(405).json({ error: "Method not allowed" });

  try {
    console.log("[sync] Start run…");
    const client = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
      auth: { persistSession: false },
    });

    const lastSynced = await getLastSynced(client);
    const since = lastSynced
      ? new Date(new Date(lastSynced).getTime() - 10 * 60 * 1000).toISOString() // 10 min overlap
      : startOfTodayISO();

    console.log("[sync] since:", since);

    const result = await runEtlSync(since);
    await refreshSupabaseViews(client);

    const now = new Date().toISOString();
    await updateLastSynced(client, now);

    return res.status(200).json({
      ok: true,
      since,
      finished_at: now,
      synced: result.synced || 0,
    });
  } catch (err) {
    console.error("[sync] error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
