// --- FINAL: filters uit Supabase (distinct) ---
// Leest unieke waarden uit 'lead_uniques_day_grp' en sluit campagne 925 uit.

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
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    // DISTINCT offer_id
    const pOffer = new URLSearchParams();
    pOffer.append('select', 'offer_id');
    pOffer.append('distinct', 'true');
    pOffer.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    pOffer.append('order', 'offer_id.asc');
    pOffer.append('limit', '100000');
    const offers = await supaGet('lead_uniques_day_grp', pOffer.toString());

    // DISTINCT campaign_id (zonder 925)
    const pCamp = new URLSearchParams();
    pCamp.append('select', 'campaign_id');
    pCamp.append('distinct', 'true');
    pCamp.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    pCamp.append('order', 'campaign_id.asc');
    pCamp.append('limit', '100000');
    const camps = await supaGet('lead_uniques_day_grp', pCamp.toString());

    // DISTINCT affiliate_id
    const pAff = new URLSearchParams();
    pAff.append('select', 'affiliate_id');
    pAff.append('distinct', 'true');
    pAff.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    pAff.append('order', 'affiliate_id.asc');
    pAff.append('limit', '100000');
    const affs = await supaGet('lead_uniques_day_grp', pAff.toString());

    // DISTINCT sub_id (alleen als aanwezig in je view; anders lege lijst)
    let subIds = [];
    try {
      const pSub = new URLSearchParams();
      pSub.append('select', 'sub_id');
      pSub.append('distinct', 'true');
      pSub.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
      pSub.append('order', 'sub_id.asc');
      pSub.append('limit', '100000');
      subIds = await supaGet('lead_uniques_day_grp', pSub.toString());
    } catch {
      subIds = [];
    }

    const uniq = (arr, key) =>
      Array.from(new Set((arr || []).map((r) => r?.[key]).filter((v) => v !== null && v !== undefined && v !== ''))).sort(
        (a, b) => ('' + a).localeCompare('' + b, 'nl', { numeric: true })
      );

    return res.status(200).json({
      data: {
        offer_ids: uniq(offers, 'offer_id'),
        campaign_ids: uniq(camps, 'campaign_id'),
        affiliate_ids: uniq(affs, 'affiliate_id'),
        sub_ids: uniq(subIds, 'sub_id'),
      },
    });
  } catch (e) {
    console.error('[dashboard-filters] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
