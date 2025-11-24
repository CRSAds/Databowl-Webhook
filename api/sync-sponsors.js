// /api/sync-all-sponsors.js
import { createClient } from "@supabase/supabase-js";

const DIRECTUS_URL = process.env.DIRECTUS_URL;
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

// Helper: fetch collectie uit Directus
async function getDirectus(path) {
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

    // 1️⃣ Haal alle drie bronnen op
    const [co, cc, ca] = await Promise.all([
      getDirectus("co_sponsors"),        // fields: cid, title
      getDirectus("coreg_campaigns"),    // fields: cid, sponsor
      getDirectus("coreg_answers"),      // fields: cid, label
    ]);

    // 2️⃣ Combineer alle bronnen in één array
    let combined = [
      ...co.map(s => ({
        cid: String(s.cid),
        sponsor_name: s.title || ""
      })),
      ...cc.map(s => ({
        cid: String(s.cid),
        sponsor_name: s.sponsor || ""
      })),
      ...ca.map(s => ({
        cid: String(s.cid),
        sponsor_name: s.label || ""
      })),
    ];

    // 3️⃣ Filter lege namen eruit
    combined = combined.filter(s => s.sponsor_name.trim() !== "");

    // 4️⃣ Unieke cid’s bewaren (eerste naam wint)
    const map = new Map();
    for (const row of combined) {
      if (!map.has(row.cid)) map.set(row.cid, row);
    }

    const finalList = [...map.values()];

    // 5️⃣ Upsert in Supabase
    const { error } = await sb
      .from("sponsor_lookup")
      .upsert(finalList, { onConflict: "cid" });

    if (error) throw error;

    return res.status(200).json({
      ok: true,
      sources: {
        co_sponsors: co.length,
        coreg_campaigns: cc.length,
        coreg_answers: ca.length,
      },
      unique_sponsors: finalList.length,
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
