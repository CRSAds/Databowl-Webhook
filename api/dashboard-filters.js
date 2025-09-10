// --- FINAL /api/dashboard-filters.js ---
// Haalt DISTINCT filterwaarden uit Supabase materialized view `lead_uniques_day_grp`
// (met dagrange + global exclude van campagne 925).

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
  const minFrom = new Date(to); minFrom.setDate(to.getDate() - (MAX_DAYS - 1));
  const clampedFrom = from < minFrom ? minFrom : from;
  const clampedTo = to;
  const days = Math.ceil((clampedTo - clampedFrom) / 86400000) + 1;
  if (days > MAX_DAYS) return { error: `Date range too large (${days}d). Max ${MAX_DAYS} days.` };
  return {
    date_from: clampedFrom.toISOString().slice(0, 10),
    date_to: clampedTo.toISOString().slice(0, 10),
  };
}

async function supaGet(path, qs) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}?${qs}`, {
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

    // Optioneel: zelfde daterange gebruiken als het dashboard; default = laatste 3 dagen
    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // Belangrijk: geen lege and=() of where=true sturen.
    // We vragen gewoon de benodigde kolommen op binnen de daterange,
    // en dedupliceren hier in de functie.
    const params = new URLSearchParams();
    params.append('select', 'offer_id,campaign_id,affiliate_id'); // voeg hier 'sub_id' toe als die in je view zit
    params.append('day', `gte.${date_from}`);
    params.append('day', `lte.${date_to}`);
    params.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    // Eventueel nog: params.append('limit','100000'); // meestal niet nodig

    const rows = await supaGet('lead_uniques_day_grp', params.toString());

    const uniq = (arr) => Array.from(new Set(arr.filter((v) => v !== null && v !== ''))).sort((a, b) =>
      ('' + a).localeCompare('' + b, 'nl', { numeric: true })
    );

    const data = {
      offer_ids:     uniq(rows.map(r => r.offer_id)),
      campaign_ids:  uniq(rows.map(r => r.campaign_id)),
      affiliate_ids: uniq(rows.map(r => r.affiliate_id)),
      sub_ids:       [], // alleen vullen als de view deze kolom bevat
    };

    return res.status(200).json({ data });
  } catch (err) {
    console.error('[dashboard-filters] error:', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
