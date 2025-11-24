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

    // ---- 1) Directus Data Ophalen ----
    const [coRes, ccRes, caRes] = await Promise.all([
      fetch(`${DIRECTUS_URL}/items/co_sponsors?limit=-1&fields=cid,title`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
      fetch(`${DIRECTUS_URL}/items/coreg_campaigns?limit=-1&fields=cid,Sponsor,sponsor`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
      fetch(`${DIRECTUS_URL}/items/coreg_answers?limit=-1&fields=cid,label`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
    ]);

    const co = (await coRes.json()).data || [];
    const cc = (await ccRes.json()).data || [];
    const ca = (await caRes.json()).data || [];

    // ---- 2) Normaliseren → zelfde structuur ----
    const rows = [
      // co_sponsors
      ...co.map(s => ({
        cid: String(s.cid),
        sponsor_name: s.title || "",
      })),

      // coreg_campaigns
      ...cc.map(s => ({
        cid: String(s.cid),
        sponsor_name: 
          s.Sponsor ||        // hoofdletter veld (jouw Directus setup)
          s.sponsor ||        // fallback lowercase
          "",
      })),

      // coreg_answers
      ...ca.map(s => ({
        cid: String(s.cid),
        sponsor_name: s.label || "",
      })),
    ];

    // ---- 3) Lege namen eruit ----
    const cleaned = rows.filter(r => r.sponsor_name && r.sponsor_name.trim() !== "");

    // ---- 4) Duplicaten verwijderen (laatste overwint) ----
    const unique = Object.values(
      cleaned.reduce((acc, item) => {
        acc[item.cid] = item;
        return acc;
      }, {})
    );

    // ---- 5) Upsert in Supabase ----
    const { error } = await sb
      .from("sponsor_lookup")
      .upsert(unique, { onConflict: "cid" });

    if (error) throw error;

    return res.status(200).json({
      ok: true,
      sources: {
        co_sponsors: co.length,
        coreg_campaigns: cc.length,
        coreg_answers: ca.length,
      },
      unique_sponsors: unique.length,
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
