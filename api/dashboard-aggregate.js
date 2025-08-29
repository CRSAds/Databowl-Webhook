// /api/dashboard-aggregate.js
// Drill-down aggregatie met DISTINCT t_id (geen lead_id).
// Default: day > affiliate > offer. Wisselbaar via ?order=day,affiliate,offer
// Filters: offer_id, campaign_id, affiliate_id, sub_id, date_from, date_to  (date_to is inclusief)

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

/** Tunables */
const PAGE_SIZE = Number(process.env.DASHBOARD_PAGE_SIZE || 1000); // rijen per page uit Directus
const MAX_DAYS   = Number(process.env.DASHBOARD_MAX_DAYS  || 31);  // hard guard op datum-range

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// YYYY-MM-DD in Europe/Amsterdam (bucket key)
function dateKeyNL(value) {
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

// Inclusieve einddatum: geef volgende dag terug (YYYY-MM-DD)
function nextDayStr(dStr) {
  const d = new Date(dStr);
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

// Robuuste parser voor kosten (accepteert "€0,75", "0,75", "0.75", 0.75)
function toNumber(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
  let s = String(v).trim();
  s = s.replace(/[€\s]/g, '');
  if (s.indexOf('.') === -1 && s.indexOf(',') > -1) s = s.replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function getGroupKey(row, level) {
  if (level === 'day') return dateKeyNL(row.created_at);
  if (level === 'affiliate') return (row.affiliate_id ?? '') + '';
  if (level === 'offer') return (row.offer_id ?? '') + '';
  return '';
}

function getGroupLabel(key, level) {
  if (level === 'day') {
    const [y, m, d] = key.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
  }
  if (level === 'affiliate') return key || '—';
  if (level === 'offer') return key || '—';
  return key;
}

/** Helper: alle pagina's ophalen en samenvoegen */
async function fetchAllRows(params) {
  const headers = { Authorization: `Bearer ${DIRECTUS_TOKEN}` };
  let offset = 0;
  let all = [];

  while (true) {
    const p = new URLSearchParams(params);
    p.set('limit', String(PAGE_SIZE));
    p.set('offset', String(offset));

    const url = `${DIRECTUS_URL}/items/Databowl_lead_events?${p.toString()}`;
    const r = await fetch(url, { headers });
    const j = await r.json();
    if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(j)}`);

    const rows = Array.isArray(j.data) ? j.data : [];
    all = all.concat(rows);

    if (rows.length < PAGE_SIZE) break; // laatste pagina
    offset += PAGE_SIZE;
  }
  return all;
}

/** Guard: range limiter (voorkomt gigantische pulls per ongeluk) */
function guardDateRange(q) {
  // default laatste 7 dagen als niets opgegeven
  let from = q.date_from ? new Date(q.date_from) : new Date(Date.now() - 6 * 864e5);
  let to   = q.date_to   ? new Date(q.date_to)   : new Date();

  // normaliseer naar YYYY-MM-DD
  const toISO = (d) => d.toISOString().slice(0, 10);
  // max window
  const ms = to - from;
  const days = Math.ceil(ms / 86400000) + 1;
  if (days > MAX_DAYS) {
    // cap naar laatste MAX_DAYS
    from = new Date(to.getTime() - (MAX_DAYS - 1) * 86400000);
  }
  return { date_from: toISO(from), date_to: toISO(to) };
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const {
      offer_id, campaign_id, affiliate_id, sub_id,
      order = 'day,affiliate,offer',
    } = req.query;

    // Range guard + inclusief eind
    const { date_from, date_to } = guardDateRange(req.query);

    // Ruwe records: alleen noodzakelijke velden
    const params = new URLSearchParams();
    params.append('fields', ['created_at', 't_id', 'affiliate_id', 'offer_id', 'cost'].join(','));
    params.append('filter[created_at][_gte]', date_from);
    params.append('filter[created_at][_lt]', nextDayStr(date_to));
    if (offer_id)     params.append('filter[offer_id][_eq]', offer_id);
    if (campaign_id)  params.append('filter[campaign_id][_eq]', campaign_id);
    if (affiliate_id) params.append('filter[affiliate_id][_eq]', affiliate_id);
    if (sub_id)       params.append('filter[sub_id][_eq]', sub_id);

    const rows = await fetchAllRows(params);

    // Volgorde van niveaus bepalen
    const levels = order.split(',').map(s => s.trim()).map(s => {
      if (s === 'affiliate_id') return 'affiliate';
      if (s === 'offer_id') return 'offer';
      return s; // 'day' | 'affiliate' | 'offer'
    });
    const [L1, L2, L3] = levels;

    // Tree bouwen: L1 -> L2 -> L3
    const L1map = new Map(); // key -> {label, leadsSet:Set, cost:number, children:Map}

    for (const row of rows) {
      const tId = (row?.t_id && String(row.t_id).trim()) || null;
      const cost = toNumber(row?.cost);

      const k1 = getGroupKey(row, L1);
      const n1 = L1map.get(k1) || { key: k1, label: getGroupLabel(k1, L1), leadsSet: new Set(), cost: 0, children: new Map() };
      if (tId) n1.leadsSet.add(tId);
      n1.cost += cost;

      if (L2) {
        const k2 = getGroupKey(row, L2);
        const L2map = n1.children;
        const n2 = L2map.get(k2) || { key: k2, label: getGroupLabel(k2, L2), leadsSet: new Set(), cost: 0, children: new Map() };
        if (tId) n2.leadsSet.add(tId);
        n2.cost += cost;

        if (L3) {
          const k3 = getGroupKey(row, L3);
          const L3map = n2.children;
          const n3 = L3map.get(k3) || { key: k3, label: getGroupLabel(k3, L3), leadsSet: new Set(), cost: 0 };
          if (tId) n3.leadsSet.add(tId);
          n3.cost += cost;
          L3map.set(k3, n3);
        }
        L2map.set(k2, n2);
      }
      L1map.set(k1, n1);
    }

    // Netjes naar arrays + basis sortering
    const sortFn = (a, b) => {
      if (L1 === 'day') return a.key < b.key ? 1 : -1; // dagen desc
      return ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });
    };

    const toArray = (map) =>
      Array.from(map.values()).map(n => {
        const out = {
          key: n.key,
          label: n.label,
          leads: n.leadsSet ? n.leadsSet.size : 0,
          cost: n.cost,
        };
        if (n.children && n.children.size) out.children = toArray(n.children);
        return out;
      }).sort(sortFn);

    const tree = toArray(L1map);

    // handige response headers voor debug/monitoring
    res.setHeader('X-Queried-From', date_from);
    res.setHeader('X-Queried-To', date_to);
    res.setHeader('X-Rows-Scanned', String(rows.length));
    res.setHeader('X-Page-Size', String(PAGE_SIZE));

    return res.status(200).json({ data: { order: levels, tree } });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
