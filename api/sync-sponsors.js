// /api/sync-all-sponsors.js
import { createClient } from "@supabase/supabase-js";

const DIRECTUS_URL = process.env.DIRECTUS_URL;
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

async function fetchDirectus(path) {
  const r = await fetch(`${DIRECTUS_URL}${path}`, {
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

    // 1) Haal ALLE sponsors op uit ALLE tabellen
    const [co, cc, ca] = await Promise.all([
      fetchDirectus(`/items/co_sponsors?limit=-1&fields=cid,title`),
      fetchDirectus(`/items/coreg_campaigns?limit=-1&fields=cid,sponsor`),
      fetchDirectus(`/items/coreg_answers?limit=-1&fields=cid,label`),
    ]);

    // 2) Map alle bronnen naar één uniforme vorm
    const rows = [
      ...co.map((s) => ({
        cid: String(s.cid),
        sponsor_name: s.title || "",
      })),
      ...cc.map((s) => ({
        cid: String(s.cid),
        sponsor_name: s.sponsor || "",
      })),
      ...ca.map((s) => ({
        cid: String(s.cid),
        sponsor_name: s.label || "",
      })),
    ]
      // filter lege cid's eruit
      .filter((r) => r.cid && r.sponsor_name);

    // 3) Upsert in Supabase
    const { error } = await sb
      .from("sponsor_lookup")
      .upsert(rows, { onConflict: "cid" });

    if (error) throw error;

    return res.status(200).json({
      ok: true,
      fetched: {
        co_sponsors: co.length,
        coreg_campaigns: cc.length,
        coreg_answers: ca.length,
      },
      inserted: rows.length,
    });

  } catch (err) {
    console.error("[sync-all-sponsors] error:", err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
