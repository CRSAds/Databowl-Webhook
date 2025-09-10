// Aggregatie voor het dashboard rechtstreeks uit lead_uniques_day_grp
// Hiërarchie: Dag -> Affiliate -> Offer

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const EXCLUDED_CAMPAIGN = '925';
const MAX_DAYS = 3;

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

function nlDate(yyyy_mm_dd) {
  const [y, m, d] = (yyyy_mm_dd || '').split('-').map(Number);
  if (!y || !m || !d) return yyyy_mm_dd || '—';
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
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

  // korte CDN-cache
  res.setHeader('Cache-Control', 's-maxage=180, stale-while-revalidate=600');

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    const {
      affiliate_id,
      offer_id,
      campaign_id,
      order = 'day,affiliate,offer',
    } = req.query;

    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // Query samenstellen (alleen geldige filters toevoegen, geen lege/exotische "filter=true" zaken)
    const p = new URLSearchParams();
    p.append('select', 'day,affiliate_id,offer_id,campaign_id,leads,total_cost');
    p.append('day', `gte.${date_from}`);
    p.append('day', `lte.${date_to}`);
    p.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (affiliate_id) p.append('affiliate_id', `eq.${affiliate_id}`);
    if (offer_id)     p.append('offer_id',     `eq.${offer_id}`);
    if (campaign_id)  p.append('campaign_id',  `eq.${campaign_id}`);
    p.append('order', 'day.desc,affiliate_id.asc,offer_id.asc');

    const rows = await supaGet('lead_uniques_day_grp', p.toString());

    // Boom bouwen: day -> affiliate -> offer
    const dayMap = new Map();
    for (const r of rows || []) {
      const dayKey = String(r.day);
      const affKey = String(r.affiliate_id || '');
      const offKey = String(r.offer_id || '');

      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: nlDate(dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      let affNode = dayNode.children.get(affKey);
      if (!affNode) {
        affNode = { key: affKey, label: affKey || '—', leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, affNode);
      }
      let offNode = affNode.children.get(offKey);
      if (!offNode) {
        offNode = { key: offKey, label: offKey || '—', leads: 0, cost: 0, campaign_id: r.campaign_id || null };
        affNode.children.set(offKey, offNode);
      }

      const leads = Number(r.leads || 0);
      const cost  = Number(r.total_cost || 0);

      offNode.leads += leads;  offNode.cost += cost;
      affNode.leads += leads;  affNode.cost += cost;
      dayNode.leads += leads;  dayNode.cost += cost;
    }

    const sortTop = (level) => (a, b) => (level === 'day'
      ? (a.key < b.key ? 1 : -1) // nieuwste dag eerst
      : ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true })
    );

    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n => {
        const out = { key: n.key, label: n.label, leads: n.leads || 0, cost: n.cost || 0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) {
          out.children = toArray(n.children, level === 'day' ? 'affiliate' : 'offer');
        }
        return out;
      });
      return arr.sort(sortTop(level));
    };

    const tree = toArray(dayMap, 'day');

    return res.status(200).json({
      data: {
        order: ['day', 'affiliate', 'offer'],
        applied_filters: {
          from: date_from, to: date_to,
          affiliate_id: affiliate_id || null,
          offer_id: offer_id || null,
          campaign_id: campaign_id || null
        },
        tree
      }
    });
  } catch (err) {
    console.error('[dashboard-aggregate] error:', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
