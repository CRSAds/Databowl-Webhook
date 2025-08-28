// /api/dashboard-aggregate.js
const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || ''; // of liever een read-only token

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    // filters uit query
    const { offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to } = req.query;

    // bouw Directus query (aggregate + filters)
    const p = new URLSearchParams();
    p.append('aggregate[countDistinct]', 't_id');
    p.append('aggregate[sum]', 'cost');
    p.append('groupBy[]', 'created_at'); // we groeperen op created_at; client toont per dag

    // datum filters (optioneel)
    if (date_from) p.append('filter[created_at][_gte]', date_from);
    if (date_to)   p.append('filter[created_at][_lte]', date_to);

    // id-filters (optioneel)
    if (offer_id)     p.append('filter[offer_id][_eq]', offer_id);
    if (campaign_id)  p.append('filter[campaign_id][_eq]', campaign_id);
    if (affiliate_id) p.append('filter[affiliate_id][_eq]', affiliate_id);
    if (sub_id)       p.append('filter[sub_id][_eq]', sub_id);

    // haal op bij Directus
    const url = `${DIRECTUS_URL}/items/Databowl_lead_events?${p.toString()}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));

    // retourneer 1:1 naar browser
    return res.status(200).json(j);
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
