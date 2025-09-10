// Unieke filterlijsten uit dezelfde view (campagne 925 uitgesloten)

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const EXCLUDED_CAMPAIGN = '925';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

async function supaGet(path, params) {
  const qs = params ? `?${params}` : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}${qs}`, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'count=exact',
    },
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`Supabase ${path} ${r.status}: ${txt || r.statusText}`);
  }
  return r.json();
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    const p = new URLSearchParams();
    // we halen simpelweg kolommen op en dedupliceren client-side
    p.append('select', 'offer_id,campaign_id,affiliate_id');
    p.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    p.append('limit', '100000'); // ruime marge; matview is toch al gecomprimeerd

    const rows = await supaGet('lead_uniques_day_grp', p.toString());
    const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean))).sort((a, b) =>
      ('' + a).localeCompare('' + b, 'nl', { numeric: true })
    );

    const data = {
      offer_ids:     uniq(rows.map(r => r.offer_id)),
      campaign_ids:  uniq(rows.map(r => r.campaign_id)),
      affiliate_ids: uniq(rows.map(r => r.affiliate_id)),
      // sub_id bestaat hier niet; voor compatibiliteit leveren we een lege lijst
      sub_ids: []
    };

    return res.status(200).json({ data });
  } catch (err) {
    console.error('[dashboard-filters] error:', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
