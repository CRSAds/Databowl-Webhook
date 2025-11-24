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
    if (!DIRECTUS_URL || !DIRECTUS_TOKEN)
      return res.status(500).json({ error: "Missing Directus vars" });

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE)
      return res.status(500).json({ error: "Missing Supabase vars" });

    // 1) Haal ALLE sponsorvarianten op
    const [co, cc, ca] = await Promise.all([
      fetchDirectus("co_sponsors?fields=cid,title"),
      fetchDirectus("coreg_campaigns?fields=cid,sponsor"),
      fetchDirectus("coreg_answers?fields=cid,label"),
    ]);

    // 2) Maak mapping: zelfde CID -> juiste naam
    const rows = [];

    co.forEach((s) => {
      if (s.cid)
        rows.push({ cid: String(s.cid), sponsor_name: s.title || "" });
    });

    cc.forEach((s) => {
      if (s.cid)
        rows.push({ cid: String(s.cid), sponsor_name: s.sponsor || "" });
    });

    ca.forEach((s) => {
      if (s.cid)
        rows.push({ cid: String(s.cid), sponsor_name: s.label || "" });
    });

    // 3) Upsert ALLE data
    const { error } = await sb
      .from("sponsor_lookup")
      .upsert(rows, { onConflict: "cid" });

    if (error) throw error;

    return res.status(200).json({
      ok: true,
      inserted: rows.length,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
