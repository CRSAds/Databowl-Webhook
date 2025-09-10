// --- REPLACE THE WHOLE FILE WITH THIS FINAL VERSION ---

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
      Prefer: 'count=exact',
    },
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

    // We halen unieke waarden uit de geaggregeerde tabel/view (sneller dan raw),
    // en sluiten campaign 925 uit.
    const p = new URLSearchParams();
    p.append('select', 'offer_id,campaign_id,affiliate_id,sub_id');
    p.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    p.append('order', 'offer_id.asc,campaign_id.asc,affiliate_id.asc,sub_id.asc');
    p.append('limit', '100000');

    const rows = await supaGet('lead_uniques_day_grp', p.toString());

    const uniq = (arr) => Array.from(new Set(arr.filter(v => v !== null && v !== undefined && v !== ''))).sort((a,b)=>(`${a}`).localeCompare(`${b}`,'nl',{numeric:true}));

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
