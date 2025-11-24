export default async function handler(req, res) {
  try {
    const DIRECTUS_URL = process.env.DIRECTUS_URL;
    const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;
    const SUPABASE_URL = process.env.SUPABASE_URL;
    const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

    const { createClient } = await import("@supabase/supabase-js");
    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

    // Fetch 3 sources
    const endpoints = [
      ["co_sponsors", "title"],
      ["coreg_campaigns", "sponsor"],
      ["coreg_answers", "label"],
    ];

    const merged = {};

    for (const [collection, field] of endpoints) {
      const r = await fetch(`${DIRECTUS_URL}/items/${collection}?limit=-1`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` }
      });

      const json = await r.json();
      for (const row of json.data || []) {
        if (!row.cid) continue;
        merged[String(row.cid)] = row[field] || null;
      }
    }

    // Upsert in Supabase
    const payload = Object.entries(merged).map(([cid, name]) => ({
      cid,
      sponsor_name: name
    }));

    const { error } = await sb.from("sponsor_lookup").upsert(payload);
    if (error) throw error;

    return res.status(200).json({ ok: true, inserted: payload.length });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
}
