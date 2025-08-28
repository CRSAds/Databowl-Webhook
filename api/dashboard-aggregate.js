// /api/dashboard-aggregate.js
const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// Maak een YYYY-MM-DD key in Europe/Amsterdam
function dateKeyNL(value) {
  const d = new Date(value);
  // 'sv-SE' geeft altijd YYYY-MM-DD terug; zet expliciet NL-tijdzone
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d); // bv "2025-08-28"
  return s;
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to } = req.query;

    const p = new URLSearchParams();
    // Haal ruwe rijen op per timestamp en tel alvast distinct t_id en som cost per timestamp
    p.append('aggregate[countDistinct]', 't_id');
    p.append('aggregate[sum]', 'cost');
    p.append('groupBy[]', 'created_at');

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

    // Bucket op dag (Europe/Amsterdam) en sommeer
    const buckets = new Map();
    for (const row of rows) {
      const key = dateKeyNL(row.created_at); // "YYYY-MM-DD"
      const leads = Number(row?.countDistinct?.t_id || 0);
      const cost  = Number(row?.sum?.cost || 0);
      const prev  = buckets.get(key) || { date: key, leads: 0, cost: 0 };
      prev.leads += leads;
      prev.cost  += cost;
      buckets.set(key, prev);
    }

    // Sorteer aflopend op datum
    const aggregated = Array.from(buckets.values()).sort((a, b) => (a.date < b.date ? 1 : -1));

    return res.status(200).json({ data: aggregated });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
