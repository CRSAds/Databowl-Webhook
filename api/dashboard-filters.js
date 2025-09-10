// /api/dashboard-filters.js  (Supabase versie)
// Unieke waarden voor dropdowns, campagne 925 uitgesloten.

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const EXCLUDED_CAMPAIGN = '925';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function mustEnv(name, val) {
  if (!val) throw new Error(`Missing env ${name}`);
}

async function sbGet(path, params = {}) {
  mustEnv('SUPABASE_URL', SUPABASE_URL);
  mustEnv('SUPABASE_SERVICE_ROLE', SUPABASE_SERVICE_ROLE);

  const usp = new URLSearchParams(params);
  const url = `${SUPABASE_URL}/rest/v1/${path}${usp.toString() ? `?${usp}` : ''}`;

  const r = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      Prefer: 'count=exact',
    },
  });
  const txt = await r.text();
  let data = [];
  try { data = txt ? JSON.parse(txt) : []; } catch {}
  if (!r.ok) throw new Error(`Supabase ${path} ${r.status}: ${txt}`);
  return Array.isArray(data) ? data : [];
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    // We pakken unieke kolommen uit de aggregatietabel (lichtgewicht).
    // PostgREST: distinct doe je met `select=<kolom>&distinct`.
    const baseFilter = { 'campaign_id=neq': EXCLUDED_CAMPAIGN };

    const [offers, campaigns, affiliates, subs] = await Promise.all([
      sbGet('lead_uniques_day_grp', { ...baseFilter, select: 'offer_id', distinct: 'on' }),
      sbGet('lead_uniques_day_grp', { ...baseFilter, select: 'campaign_id', distinct: 'on' }),
      sbGet('lead_uniques_day_grp', { ...baseFilter, select: 'affiliate_id', distinct: 'on' }),
      sbGet('lead_uniques_day_grp', { ...baseFilter, select: 'sub_id', distinct: 'on' }), // alleen als kolom bestaat
    ]);

    const clean = (arr, k) => Array.from(new Set(arr.map(x => x?.[k]).filter(Boolean))).sort();

    const result = {
      offer_ids: clean(offers, 'offer_id'),
      campaign_ids: clean(campaigns, 'campaign_id'),
      affiliate_ids: clean(affiliates, 'affiliate_id'),
      sub_ids: clean(subs, 'sub_id'),
    };

    return res.status(200).json({ data: result });
  } catch (e) {
    console.error('[dashboard-filters] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
