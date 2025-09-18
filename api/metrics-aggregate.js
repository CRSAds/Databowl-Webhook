// /api/metrics-aggregate.js
// Snelle metrics feed: sommeer per dag -> affiliate -> offer

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE|| '';
const EXCLUDED_CAMPAIGN = '925';
const MAX_DAYS = 365;

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
  const cf = from < minFrom ? minFrom : from;
  const ct = to;
  const days = Math.ceil((ct - cf) / 86400000) + 1;
  if (days > MAX_DAYS) {
    return { error: `Date range too large (${days}d). Max ${MAX_DAYS} days.` };
  }
  return {
    date_from: cf.toISOString().slice(0, 10),
    date_to: ct.toISOString().slice(0, 10),
  };
}

// consistent: altijd YYYY-MM-DD als key
function dateKeyISO(value) {
  const d = new Date(value);
  return d.toISOString().slice(0, 10);
}

// nette label in NL formaat
function labelForDay(keyISO) {
  const [y, m, d] = keyISO.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
}

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
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
  if (req.method !== 'GET')
    return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return res
        .status(500)
        .json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    const { affiliate_id, offer_id, campaign_id } = req.query;

    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    const p = new URLSearchParams();
    p.append('select', 'day,affiliate_id,offer_id,campaign_id,leads,total_cost');
    p.append('day', `gte.${date_from}`);
    p.append('day', `lte.${date_to}`);
    p.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (affiliate_id) p.append('affiliate_id', `eq.${affiliate_id}`);
    if (offer_id) p.append('offer_id', `eq.${offer_id}`);
    if (campaign_id) p.append('campaign_id', `eq.${campaign_id}`);
    p.append('order', 'day.desc,affiliate_id.asc,offer_id.asc');

    const rows = await supaGet('lead_uniques_day_grp', p.toString());

    const dayMap = new Map();
    for (const r of rows || []) {
      const dayKey = dateKeyISO(r.day);
      const affKey = String(r.affiliate_id || '');
      const offKey = String(r.offer_id || '');
      const leads = toNumber(r.leads);
      const cost = toNumber(r.total_cost);

      let d = dayMap.get(dayKey);
      if (!d) {
        d = {
          key: dayKey,
          label: labelForDay(dayKey),
          leads: 0,
          cost: 0,
          children: new Map(),
        };
        dayMap.set(dayKey, d);
      }

      let a = d.children.get(affKey);
      if (!a) {
        a = {
          key: affKey,
          label: affKey || '—',
          leads: 0,
          cost: 0,
          children: new Map(),
        };
        d.children.set(affKey, a);
      }

      let o = a.children.get(offKey);
      if (!o) {
        o = {
          key: offKey,
          label: offKey || '—',
          leads: 0,
          cost: 0,
          campaign_id: r.campaign_id || null,
        };
        a.children.set(offKey, o);
      }

      o.leads += leads;
      o.cost += cost;
      a.leads += leads;
      a.cost += cost;
      d.leads += leads;
      d.cost += cost;
    }

    const sortTop = (level) => (a, b) =>
      level === 'day'
        ? a.key < b.key
          ? 1
          : -1
        : ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });

    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map((n) => {
        const out = {
          key: n.key,
          label: n.label,
          leads: n.leads || 0,
          cost: n.cost || 0,
        };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size)
          out.children = toArray(
            n.children,
            level === 'day' ? 'affiliate' : 'offer'
          );
        return out;
      });
      return arr.sort(sortTop(level));
    };

    const tree = toArray(dayMap, 'day');

    res.status(200).json({
      data: {
        order: ['day', 'affiliate', 'offer'],
        applied_filters: {
          from: date_from,
          to: date_to,
          affiliate_id: affiliate_id || null,
          offer_id: offer_id || null,
          campaign_id: campaign_id || null,
        },
        tree,
      },
    });
  } catch (err) {
    console.error('[metrics-aggregate] error:', err);
    res.status(500).json({ error: String(err?.message || err) });
  }
}
