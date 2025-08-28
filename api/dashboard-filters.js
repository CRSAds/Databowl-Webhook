// /api/dashboard-filters.js
const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

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
    // haal alleen de kolommen op die we nodig hebben
    const params = new URLSearchParams();
    params.append('limit', '-1');
    params.append('fields', 'offer_id,campaign_id,affiliate_id,sub_id');

    const r = await fetch(`${DIRECTUS_URL}/items/Databowl_lead_events?${params}`, {
      headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` }
    });
    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));

    const uniq = (arr) => Array.from(new Set(arr.filter(Boolean))).sort();

    const rows = j.data || [];
    const result = {
      offer_ids:     uniq(rows.map(r => r.offer_id)),
      campaign_ids:  uniq(rows.map(r => r.campaign_id)),
      affiliate_ids: uniq(rows.map(r => r.affiliate_id)),
      sub_ids:       uniq(rows.map(r => r.sub_id)),
    };

    return res.status(200).json({ data: result });
  } catch (e) {
    console.error('[dashboard-filters] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
