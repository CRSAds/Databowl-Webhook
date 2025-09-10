// /api/dashboard-filters.js
// Haalt unieke waardes op uit `lead_uniques_day_grp` (campagne 925 uitgesloten)

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const EXCLUDED_CAMPAIGN = "925";

function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

async function supaGet(path, qs) {
  const q = qs ? `?${qs}` : "";
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}${q}`, {
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "count=exact",
    },
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`Supabase ${path} ${r.status}: ${txt || r.statusText}`);
  }
  return r.json();
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: "Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY" });
    }

    // DISTINCT via PostgREST: gebruik ?select=kolom&distinct
    const base = new URLSearchParams();
    base.append("campaign_id", `neq.${EXCLUDED_CAMPAIGN}`);

    const qOffer = new URLSearchParams(base);
    qOffer.append("select", "offer_id");
    qOffer.append("distinct", "");
    qOffer.append("order", "offer_id.asc");
    qOffer.append("limit", "100000");

    const qCampaign = new URLSearchParams();
    qCampaign.append("select", "campaign_id");
    qCampaign.append("distinct", "");
    qCampaign.append("order", "campaign_id.asc");
    qCampaign.append("limit", "100000");
    // 925 filter hier niet nodig; kan wel, maar distinct zonder filter geeft je ook 925. We sluiten ‘m uit met client:
    // we doen het toch server-side, consistent:
    qCampaign.append("campaign_id", `neq.${EXCLUDED_CAMPAIGN}`);

    const qAffiliate = new URLSearchParams(base);
    qAffiliate.append("select", "affiliate_id");
    qAffiliate.append("distinct", "");
    qAffiliate.append("order", "affiliate_id.asc");
    qAffiliate.append("limit", "100000");

    const [offers, campaigns, affiliates] = await Promise.all([
      supaGet("lead_uniques_day_grp", qOffer.toString()),
      supaGet("lead_uniques_day_grp", qCampaign.toString()),
      supaGet("lead_uniques_day_grp", qAffiliate.toString()),
    ]);

    const uniq = (arr) =>
      Array.from(new Set((arr || []).map((r) => Object.values(r)[0]).filter(Boolean))).sort((a, b) =>
        ("" + a).localeCompare("" + b, "nl", { numeric: true })
      );

    const result = {
      offer_ids: uniq(offers),
      campaign_ids: uniq(campaigns).filter((c) => c !== EXCLUDED_CAMPAIGN),
      affiliate_ids: uniq(affiliates),
      sub_ids: [], // niet aanwezig in geaggregeerde tabel; laat leeg
    };

    return res.status(200).json({ data: result });
  } catch (e) {
    console.error("[dashboard-filters] error:", e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
