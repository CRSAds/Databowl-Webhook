// --- COMPLETE FILE ---
// Haalt unieke waarden voor filters op uit Supabase tabel `lead_uniques_day_grp`.
// Campagne 925 is uitgesloten uit de lijst.

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const EXCLUDED_CAMPAIGN = '925';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

async function supaGet(path, qs) {
  const q = qs ? `?${qs}` : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}${q}`, {
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'count=exact'
    }
  });
  if (!r.ok) {
    const txt = await r.text().catch(()=> '');
    throw new Error(`Supabase ${path} ${r.status}: ${txt || r.statusText}`);
  }
  return r.json();
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    // We vragen alle kolommen op en dedupen in Node (met distinct=true scheelt payload).
    const params = new URLSearchParams();
    params.append('select', 'offer_id,campaign_id,affiliate_id,sub_id');
    params.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    params.append('distinct', 'true'); // PostgREST distinct
    params.append('order', 'offer_id.asc,campaign_id.asc,affiliate_id.asc');

    const rows = await supaGet('lead_uniques_day_grp', params.toString());
    const uniq = (arr) => Array.from(new Set(arr.filter(v => v !== null && v !== undefined && `${v}`.length))).sort((a,b)=>(`${a}`).localeCompare(`${b}`,'nl',{numeric:true}));

    const result = {
      offer_ids:     uniq(rows.map(r => r.offer_id)),
      campaign_ids:  uniq(rows.map(r => r.campaign_id)),
      affiliate_ids: uniq(rows.map(r => r.affiliate_id)),
      sub_ids:       uniq(rows.map(r => r.sub_id)), // kolom is optioneel; resulteert dan gewoon in []
    };

    return res.status(200).json({ data: result });
  } catch (e) {
    console.error('[dashboard-filters] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
