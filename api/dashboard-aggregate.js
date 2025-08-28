// /api/dashboard-aggregate.js
// Doel: per DAG aggregeren met:
// - leads = # distinct (t_id if present else lead_id)
// - cost  = som(cost)
// Filtering op offer/campaign/affiliate/sub en date-range
// Alles server-side om CORS & "t_id ontbreekt" issues te voorkomen.

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// Maak YYYY-MM-DD in Europe/Amsterdam
function dateKeyNL(value) {
  const d = new Date(value);
  // 'sv-SE' geeft YYYY-MM-DD terug, met vaste volgorde (handig om later te sorteren)
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d); // bv "2025-08-28"
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to } = req.query;

    // We halen RUWE records op (geen aggregate), alleen de velden die we nodig hebben
    const p = new URLSearchParams();
    p.append('limit', '-1');
    p.append('fields', ['created_at', 't_id', 'lead_id', 'cost'].join(','));

    if (date_from) p.append('filter[created_at][_gte]', date_from);
    if (date_to)   p.append('filter[created_at][_lte]', date_to);
    if (offer_id)     p.append('filter[offer_id][_eq]', offer_id);
    if (campaign_id)  p.append('filter[campaign_id][_eq]', campaign_id);
    if (affiliate_id) p.append('filter[affiliate_id][_eq]', affiliate_id);
    if (sub_id)       p.append('filter[sub_id][_eq]', sub_id);

    const url = `${DIRECTUS_URL}/items/Databowl_lead_events?${p.toString()}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));

    const rows = Array.isArray(j.data) ? j.data : [];

    // Bucket per dag en tel distinct(t_id || lead_id), som(cost)
    const buckets = new Map(); // key "YYYY-MM-DD" -> { date, leadsSet:Set, cost:number }
    for (const row of rows) {
      const key = dateKeyNL(row.created_at);
      const b = buckets.get(key) || { date: key, leadsSet: new Set(), cost: 0 };

      // Unieke sleutel voor lead: t_id als die er is, anders lead_id als fallback
      const leadKey = (row?.t_id && String(row.t_id).trim()) || (row?.lead_id && String(row.lead_id).trim());
      if (leadKey) b.leadsSet.add(leadKey);

      const costNum = Number(row?.cost ?? 0);
      if (!Number.isNaN(costNum)) b.cost += costNum;

      buckets.set(key, b);
    }

    // Maak output en sorteer op datum aflopend
    const aggregated = Array
      .from(buckets.values())
      .map(b => ({ date: b.date, leads: b.leadsSet.size, cost: b.cost }))
      .sort((a, b) => (a.date < b.date ? 1 : -1));

    return res.status(200).json({ data: aggregated });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
