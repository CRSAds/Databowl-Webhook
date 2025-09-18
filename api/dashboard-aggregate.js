// /api/dashboard-aggregate.js
// L1 (dag) komt uit lead_uniques_day_tot  -> correct DISTINCT t_id per dag
// Drilldown (dag -> affiliate -> offer) uit lead_uniques_day_grp

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const MAX_DAYS = 31;
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
    date_from: clampedFrom.toISOString().slice(0,10),
    date_to: clampedTo.toISOString().slice(0,10),
  };
}

function dateKeyNL(value) {
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', { timeZone:'Europe/Amsterdam', year:'numeric', month:'2-digit', day:'2-digit' }).format(d);
}
function labelForDay(key) {
  const [y,m,d] = (key||'').split('-').map(Number);
  if (!y||!m||!d) return key||'—';
  return new Date(Date.UTC(y,m-1,d)).toLocaleDateString('nl-NL');
}
function toNumber(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }

async function supaGet(path, qs) {
  const q = qs ? `?${qs}` : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}${q}`, {
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

  res.setHeader('Cache-Control', 's-maxage=120, stale-while-revalidate=300');

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE' });
    }

    const { offer_id, campaign_id, affiliate_id, sub_id, order='day,affiliate,offer' } = req.query;
    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // 1) Totals per dag (distinct t_id) uit lead_uniques_day_tot
    {
      // NB: PostgREST: meerdere filters -> herhaal dezelfde key
    }
    const pTot = new URLSearchParams();
    pTot.append('select', 'day,leads,total_cost');
    pTot.append('day', `gte.${date_from}`);
    pTot.append('day', `lte.${date_to}`);
    pTot.append('order', 'day.desc');
    const totals = await supaGet('lead_uniques_day_tot', pTot.toString()); // [{day, leads, total_cost}]

    // 2) Drilldown-rijen uit lead_uniques_day_grp
    const pGrp = new URLSearchParams();
    pGrp.append('select', 'day,campaign_id,affiliate_id,offer_id,leads,total_cost');
    pGrp.append('day', `gte.${date_from}`);
    pGrp.append('day', `lte.${date_to}`);
    pGrp.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (offer_id)     pGrp.append('offer_id', `eq.${offer_id}`);
    if (campaign_id)  pGrp.append('campaign_id', `eq.${campaign_id}`);
    if (affiliate_id) pGrp.append('affiliate_id', `eq.${affiliate_id}`);
    if (sub_id)       pGrp.append('sub_id', `eq.${sub_id}`); // alleen als kolom bestaat
    pGrp.append('order', 'day.desc,affiliate_id.asc,offer_id.asc');

    const rows = await supaGet('lead_uniques_day_grp', pGrp.toString());

    // 3) Boom bouwen + dagsommen Overschrijven vanuit totals
    const dayMap = new Map();

    // eerst de children vullen
    for (const r of rows) {
      const dayKey = dateKeyNL(r.day);
      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelForDay(dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      const affKey = (r.affiliate_id ?? '') + '';
      const offKey = (r.offer_id ?? '') + '';

      let l2 = dayNode.children.get(affKey);
      if (!l2) {
        l2 = { key: affKey, label: affKey || '—', leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, l2);
      }
      let l3 = l2.children.get(offKey);
      if (!l3) {
        l3 = { key: offKey, label: offKey || '—', leads: 0, cost: 0, campaign_id: r.campaign_id ?? null };
        l2.children.set(offKey, l3);
      }
      const leads = toNumber(r.leads);
      const cost  = toNumber(r.total_cost);
      l3.leads += leads; l3.cost += cost;
      l2.leads += leads; l2.cost += cost;
      // Let op: dayNode.leads/cost NIET ophogen hier (anders dubbel tellen)
    }

    // vervolgens de dag-totals correct invullen
    for (const t of totals) {
      const dayKey = dateKeyNL(t.day);
      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelForDay(dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      dayNode.leads = toNumber(t.leads);
      dayNode.cost  = toNumber(t.total_cost);
    }

    const sortTop = (level) => (a,b)=>{
      if (level==='day') return a.key < b.key ? 1 : -1;
      return (''+a.key).localeCompare(''+b.key,'nl',{numeric:true});
    };
    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n=>{
        const out = { key:n.key, label:n.label, leads:n.leads||0, cost:n.cost||0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) out.children = toArray(n.children, level==='day'?'affiliate':'offer');
        return out;
      });
      return arr.sort(sortTop(level));
    };

    const tree = toArray(dayMap, 'day');
    res.status(200).json({ data: { order: ['day','affiliate','offer'], tree } });
  } catch (err) {
    console.error('[dashboard-aggregate] error', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
