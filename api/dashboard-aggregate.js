// /api/dashboard-aggregate.js
// Geeft boomstructuur: Dag → Affiliate → Offer
// Altijd voor alle dagen in de range, inclusief children[]

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';
const EXCLUDED_CAMPAIGN = '925';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function labelForDay(key) {
  const [y,m,d] = (key||'').split('-').map(Number);
  if (!y||!m||!d) return key||'—';
  return new Date(Date.UTC(y,m-1,d)).toLocaleDateString('nl-NL');
}

async function supaGet(path, qs) {
  const url = `${SUPABASE_URL}/rest/v1/${path}${qs ? `?${qs}` : ''}`;
  const r = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE}`,
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
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE' });
    }

    const { offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to } = req.query;

    // 1) totals per dag
    const pTot = new URLSearchParams();
    pTot.append('select', 'day,leads,total_cost');
    if (date_from) pTot.append('day', `gte.${date_from}`);
    if (date_to)   pTot.append('day', `lte.${date_to}`);
    pTot.append('order', 'day.asc');
    const totals = await supaGet('lead_uniques_day_tot', pTot.toString());

    // 2) drilldown per dag/affiliate/offer
    const pGrp = new URLSearchParams();
    pGrp.append('select', 'day,campaign_id,affiliate_id,offer_id,leads,total_cost');
    if (date_from) pGrp.append('day', `gte.${date_from}`);
    if (date_to)   pGrp.append('day', `lte.${date_to}`);
    pGrp.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (offer_id)     pGrp.append('offer_id', `eq.${offer_id}`);
    if (campaign_id)  pGrp.append('campaign_id', `eq.${campaign_id}`);
    if (affiliate_id) pGrp.append('affiliate_id', `eq.${affiliate_id}`);
    if (sub_id)       pGrp.append('sub_id', `eq.${sub_id}`);
    pGrp.append('order', 'day.asc,affiliate_id.asc,offer_id.asc');
    const rows = await supaGet('lead_uniques_day_grp', pGrp.toString());

    // 3) bouw dagMap
    const dayMap = new Map();

    // eerst alle dag-totalen neerzetten
    for (const t of totals) {
      const key = String(t.day);
      if (!dayMap.has(key)) {
        dayMap.set(key, {
          key,
          label: labelForDay(key),
          leads: 0,
          cost: 0,
          children: new Map(),
        });
      }
      const node = dayMap.get(key);
      node.leads = toNumber(t.leads);
      node.cost  = toNumber(t.total_cost);
    }

    // nu alle affiliates/offers invullen
    for (const r of rows) {
      const dayKey = String(r.day);
      if (!dayMap.has(dayKey)) {
        dayMap.set(dayKey, {
          key: dayKey,
          label: labelForDay(dayKey),
          leads: 0,
          cost: 0,
          children: new Map(),
        });
      }
      const d = dayMap.get(dayKey);

      const affKey = String(r.affiliate_id || '');
      if (!d.children.has(affKey)) {
        d.children.set(affKey, {
          key: affKey,
          label: affKey || '—',
          leads: 0,
          cost: 0,
          children: new Map(),
        });
      }
      const a = d.children.get(affKey);

      const offKey = String(r.offer_id || '');
      if (!a.children.has(offKey)) {
        a.children.set(offKey, {
          key: offKey,
          label: offKey || '—',
          leads: 0,
          cost: 0,
          campaign_id: r.campaign_id ?? null,
        });
      }
      const o = a.children.get(offKey);

      const leads = toNumber(r.leads);
      const cost  = toNumber(r.total_cost);

      o.leads += leads; o.cost += cost;
      a.leads += leads; a.cost += cost;
    }

    // 4) naar arrays
    const toArray = (map, level) => {
      return Array.from(map.values()).map(n => {
        const out = {
          key: n.key,
          label: n.label,
          leads: n.leads,
          cost: n.cost,
        };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) {
          out.children = toArray(n.children, level === 'day' ? 'affiliate' : 'offer');
        }
        return out;
      });
    };

    const tree = toArray(dayMap, 'day');

    res.status(200).json({
      data: { order: ['day','affiliate','offer'], tree }
    });
  } catch (err) {
    console.error('[dashboard-aggregate] error', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
