// /api/dashboard-aggregate.js
// Hybride aggregatie (range ≤ 3 dagen, campagne 925 uitgesloten).
// - L1 (per dag): DISTINCT t_id + SUM(cost) via REST.
// - Drilldown (dag → affiliate → offer [+ campaign_id-info]): via GraphQL aggregate.

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

const MAX_DAYS = 3;                 // hard cap
const EXCLUDED_CAMPAIGN = '925';    // global exclude

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// YYYY-MM-DD bucket in Europe/Amsterdam
function dateKeyNL(value) {
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

function nextDayStr(dStr) {
  if (!dStr) return null;
  const d = new Date(dStr);
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function toNumber(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
  let s = String(v).trim().replace(/[€\s]/g, '');
  if (s.indexOf('.') === -1 && s.indexOf(',') > -1) s = s.replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function sortTopByLevel(level) {
  return (a, b) => {
    if (level === 'day') return a.key < b.key ? 1 : -1; // nieuwste dag eerst
    return ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });
  };
}

function labelFor(level, key) {
  if (level === 'day') {
    const [y, m, d] = (key || '').split('-').map(Number);
    if (!y || !m || !d) return key || '—';
    return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
  }
  return key || '—';
}

// Clamp & valideer datumbereik tot MAX_DAYS (inclusief end)
function clampRange(date_from, date_to) {
  const today = new Date();
  const to = date_to ? new Date(date_to) : today;
  const from = date_from ? new Date(date_from) : new Date(to);

  const minFrom = new Date(to);
  minFrom.setDate(to.getDate() - (MAX_DAYS - 1));

  const clampedFrom = from < minFrom ? minFrom : from;
  const clampedTo = to;

  const days = Math.ceil((clampedTo - clampedFrom) / 86400000) + 1;
  if (days > MAX_DAYS) {
    return { error: `Date range too large (${days}d). Max ${MAX_DAYS} days.` };
  }
  return {
    date_from: clampedFrom.toISOString().slice(0, 10),
    date_to: clampedTo.toISOString().slice(0, 10),
  };
}

/* ---------------- REST: DISTINCT t_id per dag (en sum(cost)) ---------------- */
async function fetchDistinctPerDay({ offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to }) {
  const dayMap = new Map(); // yyyy-mm-dd -> { set:Set<t_id>, cost:number }
  const pageSize = 2000; // iets lager, stabieler
  let offset = 0;

  while (true) {
    const p = new URLSearchParams();
    p.append('limit', String(pageSize));
    p.append('offset', String(offset));
    p.append('fields', 'created_at,t_id,cost');

    if (date_from) p.append('filter[created_at][_gte]', date_from);
    if (date_to)   p.append('filter[created_at][_lt]', nextDayStr(date_to));
    if (offer_id)     p.append('filter[offer_id][_eq]', offer_id);
    if (campaign_id)  p.append('filter[campaign_id][_eq]', campaign_id);
    if (affiliate_id) p.append('filter[affiliate_id][_eq]', affiliate_id);
    if (sub_id)       p.append('filter[sub_id][_eq]', sub_id);

    // globale uitsluiting
    p.append('filter[campaign_id][_neq]', EXCLUDED_CAMPAIGN);

    const url = `${DIRECTUS_URL}/items/Databowl_lead_events?${p.toString()}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
    const j = await r.json();
    if (!r.ok) throw new Error(`REST distinct fetch failed: ${JSON.stringify(j)}`);

    const rows = Array.isArray(j.data) ? j.data : [];
    if (!rows.length) break;

    for (const row of rows) {
      const day = dateKeyNL(row.created_at);
      const tId = (row?.t_id && String(row.t_id).trim()) || null;
      const cost = toNumber(row?.cost);

      let bucket = dayMap.get(day);
      if (!bucket) {
        bucket = { set: new Set(), cost: 0 };
        dayMap.set(day, bucket);
      }
      if (tId) bucket.set.add(tId);
      bucket.cost += cost;
    }

    if (rows.length < pageSize) break;
    offset += pageSize;
  }

  // plain objecten
  const out = new Map(); // day -> { leads:number, cost:number }
  for (const [day, { set, cost }] of dayMap.entries()) {
    out.set(day, { leads: set.size, cost });
  }
  return out;
}

/* ---------------- GraphQL: drilldown per dag -> affiliate -> offer ---------------- */
async function fetchGraphGroups({ offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to }) {
  const GQL = `
    query($filter: Databowl_lead_events_filter, $limit: Int, $offset: Int){
      Databowl_lead_events_aggregated(
        groupBy: ["created_at","affiliate_id","offer_id","campaign_id"],
        filter: $filter,
        limit: $limit,
        offset: $offset
      ){
        group
        countDistinct { t_id }
        sum { cost }
      }
    }
  `;

  const filter = {};
  if (date_from || date_to) {
    filter.created_at = {};
    if (date_from) filter.created_at._gte = date_from;
    if (date_to)   filter.created_at._lt  = nextDayStr(date_to);
  }
  if (offer_id)     filter.offer_id     = { _eq: offer_id };
  if (campaign_id)  filter.campaign_id  = { _eq: campaign_id };
  if (affiliate_id) filter.affiliate_id = { _eq: affiliate_id };
  if (sub_id)       filter.sub_id       = { _eq: sub_id };

  // globale uitsluiting
  filter.campaign_id = filter.campaign_id
    ? { ...filter.campaign_id, _neq: EXCLUDED_CAMPAIGN }
    : { _neq: EXCLUDED_CAMPAIGN };

  const pageSize = 2000;
  let offset = 0;
  const groups = [];

  while (true) {
    const body = {
      query: GQL,
      variables: { filter, limit: pageSize, offset }
    };
    const r = await fetch(`${DIRECTUS_URL}/graphql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DIRECTUS_TOKEN}`,
      },
      body: JSON.stringify(body),
    });
    const j = await r.json();
    if (!r.ok || j.errors) throw new Error(`GraphQL aggregate failed: ${JSON.stringify(j.errors || j)}`);

    const batch = j?.data?.Databowl_lead_events_aggregated || [];
    if (!batch.length) break;
    groups.push(...batch);

    if (batch.length < pageSize) break;
    offset += pageSize;
  }

  return groups; // [{group:{created_at,affiliate_id,offer_id,campaign_id}, countDistinct:{t_id}, sum:{cost}}]
}

/* ---------------- handler ---------------- */
export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  // korte CDN-cache
  res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');

  try {
    const {
      offer_id, campaign_id, affiliate_id, sub_id,
      order = 'day,affiliate,offer',
    } = req.query;

    // range uit query clampen naar MAX_DAYS
    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // 1) Unieke leads + kosten per dag (correct, zonder dubbeltelling)
    const perDayDistinct = await fetchDistinctPerDay({ offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to });

    // 2) Drilldowngroepen uit GraphQL
    const groups = await fetchGraphGroups({ offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to });

    // 3) Bouw tree: dag -> affiliate -> offer (bewaar campaign_id als veld)
    const dayMap = new Map(); // dayKey -> { key,label, leads:number, cost:number, children: Map }

    for (const g of groups) {
      const grp = g.group || {};
      const dayKey = dateKeyNL(grp.created_at);
      const affKey = (grp.affiliate_id ?? '') + '';
      const offKey = (grp.offer_id ?? '') + '';
      const campId = grp.campaign_id ?? null;

      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelFor('day', dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }

      // L2 (affiliate)
      let l2 = dayNode.children.get(affKey);
      if (!l2) {
        l2 = { key: affKey, label: labelFor('affiliate', affKey), leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, l2);
      }

      // L3 (offer)
      let l3 = l2.children.get(offKey);
      if (!l3) {
        l3 = { key: offKey, label: labelFor('offer', offKey), leads: 0, cost: 0, campaign_id: campId };
        l2.children.set(offKey, l3);
      }

      const cnt = Number(g?.countDistinct?.t_id || 0);
      const sum = toNumber(g?.sum?.cost || 0);

      // kinderen krijgen hun eigen counts
      l3.leads += cnt;
      l3.cost  += sum;
      l2.leads += cnt;
      l2.cost  += sum;
    }

    // 4) Overschrijf L1 met echte DISTINCT per dag uit REST
    for (const [dayKey, totals] of perDayDistinct.entries()) {
      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelFor('day', dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      dayNode.leads = totals.leads;
      dayNode.cost  = totals.cost;
    }

    // 5) Map → arrays + sort
    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n => {
        const out = {
          key: n.key,
          label: n.label,
          leads: n.leads || 0,
          cost: n.cost || 0,
        };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) out.children = toArray(n.children, level === 'day' ? 'affiliate' : 'offer');
        return out;
      });
      return arr.sort(sortTopByLevel(level));
    };

    const tree = toArray(dayMap, 'day');

    return res.status(200).json({ data: { order: ['day','affiliate','offer'], tree } });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
