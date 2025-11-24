// /api/sync-sponsors.js
import { createClient } from "@supabase/supabase-js";

const DIRECTUS_URL = process.env.DIRECTUS_URL;
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

export default async function handler(req, res) {
  try {
    if (!DIRECTUS_URL || !DIRECTUS_TOKEN)
      return res.status(500).json({ error: "Missing Directus vars" });

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE)
      return res.status(500).json({ error: "Missing Supabase vars" });

    // 1) Ophalen vanuit Directus
    const r = await fetch(
      `${DIRECTUS_URL}/items/co_sponsors?limit=-1&fields=cid,title`,
      { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } }
    );

    const json = await r.json();
    const sponsors = json.data || [];

    // 2) Mappen naar Supabase formaat
    const rows = sponsors.map((s) => ({
      cid: String(s.cid),
      sponsor_name: s.title || "",
    }));

    // 3) Upsert in Supabase
    const { data, error } = await sb
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
