// Supabase-driven aggregate for the dashboard (fast, <=3d, excludes campaign 925)
const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';

const MAX_DAYS = 3;
const EXCLUDED_CAMPAIGN = '925';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function clampRange(date_from, date_to) {
  const today = new Date();
  const to = date_to ? new Date(date_to) : today;
  const from = date_from ? new Date(date_from) : new Date(to);

  const minFrom = new Date(to);
  minFrom.setDate(to.getDate() - (MAX_DAYS - 1));

  const clampedFrom = from < minFrom ? minFrom : from;
  const clampedTo = to;

  const days = Math.ceil((clampedTo - clampedFrom) / 86400000) + 1;
  if (days > MAX_DAYS) return { error: `Date range too large (${days}d). Max ${MAX_DAYS} days.` };

  return {
    date_from: clampedFrom.toISOString().slice(0, 10),
    date_to: clampedTo.toISOString().slice(0, 10),
  };
}

function dateKeyNL(value) {
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(d);
}

function labelForDay(key) {
  const [y,m,d] = (key || '').split('-').map(Number);
  if (!y || !m || !d) return key || '—';
  return new Date(Date.UTC(y, m-1, d)).toLocaleDateString('nl-NL');
}

function toNumber(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }

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

  // korte CDN-cache
  res.setHeader('Cache-Control', 's-maxage=120, stale-while-revalidate=300');

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    const { offer_id, campaign_id, affiliate_id, sub_id, order = 'day,affiliate,offer' } = req.query;
    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // We lezen vanuit materialized view / aggregate view:
    // lead_uniques_day_grp(day, campaign_id, affiliate_id, offer_id, leads, total_cost)
    // NB: campagne 925 altijd uitsluiten
    const params = new URLSearchParams();
    params.append('select', 'day,campaign_id,affiliate_id,offer_id,leads,total_cost');
    params.append('day', `gte.${date_from}`);
    params.append('day', `lte.${date_to}`);
    params.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (offer_id)     params.append('offer_id', `eq.${offer_id}`);
    if (campaign_id)  params.append('campaign_id', `eq.${campaign_id}`); // wordt samengevoegd door PostgREST (AND)
    if (affiliate_id) params.append('affiliate_id', `eq.${affiliate_id}`);
    if (sub_id)       params.append('sub_id', `eq.${sub_id}`); // alleen als je 'm in de view hebt
    params.append('order', 'day.desc,affiliate_id.asc,offer_id.asc');

    const rows = await sifa(params);

    function sifa(p){ return s ipaGet('lead_uniques_day_grp', p.toString()); } // keep small helper above

  } catch (err) {
    console.error('[dashboard-aggregate] error', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
