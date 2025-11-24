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
    console.log("ENV CHECK:", {
      DIRECTUS_URL,
      DIRECTUS_TOKEN: DIRECTUS_TOKEN ? "OK" : "MISSING",
      SUPABASE_URL,
      SUPABASE_SERVICE_ROLE: SUPABASE_SERVICE_ROLE ? "OK" : "MISSING",
    });

    // -----------------------
    // 1) DIRECTUS FETCH
    // -----------------------
    const url = `${DIRECTUS_URL}/items/co_sponsors?limit=-1&fields=cid,title`;

    console.log("Fetching from Directus:", url);

    const r = await fetch(url, {
      headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
    });

    const txt = await r.text();
    console.log("Directus RAW response:", txt);

    let json;
    try {
      json = JSON.parse(txt);
    } catch (e) {
      return res.status(500).json({ error: "Invalid JSON from Directus", txt });
    }

    const sponsors = json?.data || [];

    console.log("Directus sponsors count:", sponsors.length);
    console.log("Directus sample:", sponsors[0]);

    if (sponsors.length === 0) {
      return res.status(200).json({
        ok: true,
        inserted: 0,
        reason: "Directus returned 0 sponsors",
      });
    }

    // -----------------------
    // 2) MAPPING
    // -----------------------
    const rows = sponsors.map((s) => ({
      cid: String(s.cid),
      sponsor_name: s.title || "",
    }));

    console.log("Mapped rows count:", rows.length);
    console.log("Mapped rows sample:", rows[0]);

    // -----------------------
    // 3) SUPABASE UPSERT
    // -----------------------
    const { data, error } = await sb
      .from("sponsor_lookup")
      .upsert(rows, { onConflict: "cid" })
      .select();

    console.log("Supabase upsert error:", error);
    console.log("Supabase upsert returned:", data);

    if (error) {
      return res.status(500).json({ error });
    }

    return res.status(200).json({
      ok: true,
      inserted: rows.length,
      supabase_returned: data?.length,
    });
  } catch (err) {
    console.error("SYNC ERROR:", err);
    return res.status(500).json({ error: String(err) });
  }
}
