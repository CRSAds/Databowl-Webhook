// api/refresh-views.js
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  try {
    console.log("[refresh] Start…");
    const client = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

    const { error } = await client.rpc("refresh_materialized_views");
    if (error) throw error;

    console.log("[refresh] ✅ Views refreshed");
    return res.status(200).json({ ok: true, refreshed_at: new Date().toISOString() });
  } catch (err) {
    console.error("[refresh] error", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
