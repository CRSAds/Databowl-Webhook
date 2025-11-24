// /api/sync-sponsors.js
import { createClient } from "@supabase/supabase-js";

const DIRECTUS_URL = process.env.DIRECTUS_URL;
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

async function fetchDirectus(path) {
  const r = await fetch(`${DIRECTUS_URL}/items/${path}?limit=-1`, {
    headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
  });
  const json = await r.json();
  return json.data || [];
}

export default async function handler(req, res) {
  try {
    // ====== 1. Ophalen uit Directus ======
    const coSponsors = await fetchDirectus("co_sponsors?fields=cid,title");
    const coregCampaigns = await fetchDirectus("coreg_campaigns?fields=cid,sponsor");
    const coregAnswers = await fetchDirectus("coreg_answers?fields=cid,label");

    // ====== 2. Upsert per collectie ======

    // A) co_sponsors
    const rowsA = coSponsors.map(s => ({
      cid: String(s.cid),
      sponsor_name: s.title || ""
    }));
    if (rowsA.length) {
      await sb.from("sponsor_lookup").upsert(rowsA, { onConflict: "cid" });
    }

    // B) coreg_campaigns
    const rowsB = coregCampaigns.map(s => ({
      cid: String(s.cid),
      sponsor_name: s.sponsor || ""
    }));
    if (rowsB.length) {
      await sb.from("sponsor_lookup").upsert(rowsB, { onConflict: "cid" });
    }

    // C) coreg_answers
    const rowsC = coregAnswers.map(s => ({
      cid: String(s.cid),
      sponsor_name: s.label || ""
    }));
    if (rowsC.length) {
      await sb.from("sponsor_lookup").upsert(rowsC, { onConflict: "cid" });
    }

    return res.status(200).json({
      ok: true,
      inserted: rowsA.length + rowsB.length + rowsC.length
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
