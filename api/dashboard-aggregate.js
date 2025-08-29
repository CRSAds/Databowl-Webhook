// /api/dashboard-aggregate.js
// Server-side aggregatie via Directus GraphQL:
// - groupBy: created_at, affiliate_id, offer_id
// - metrics: COUNT DISTINCT t_id (leads) + SUM(cost)
// - batching met limit/offset (veilig bij veel data)
// Response shape: { data: { order:[L1,L2,L3], tree:[...] } } — compatibel met je dashboard.html

const DIRECTUS_URL   = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// Inclusieve einddatum → < next day (YYYY-MM-DD)
function nextDayStr(dStr) {
  if (!dStr) return null;
  const d = new Date(dStr);
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
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
function niceLabelFromKey(key) {
  const [y,m,dd] = (key || '').split('-').map(Number);
  if (!y || !m || !dd) return key || '—';
  return new Date(Date.UTC(y, m-1, dd)).toLocaleDateString('nl-NL');
}

function levelKey(row, level) {
  if (level === 'day')       return dateKeyNL(row.created_at);
  if (level === 'affiliate') return String(row.affiliate_id ?? '');
  if (level === 'offer')     return String(row.offer_id ?? '');
  return '';
}
function levelLabel(key, level) {
  if (level === 'day')       return niceLabelFromKey(key);
  if (level === 'affiliate') return key || '—';
  if (level === 'offer')     return key || '—';
  return key;
}

const GQL = `
query agg($filter: Databowl_lead_events_filter, $limit: Int, $offset: Int){
  Databowl_lead_events_aggregated(
    groupBy: ["created_at","affiliate_id","offer_id"],
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

async function gqlFetch(variables) {
  const r = await fetch(`${DIRECTUS_URL}/graphql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${DIRECTUS_TOKEN}`,
    },
    body: JSON.stringify({ query: GQL, variables }),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.errors) {
    throw new Error(`GraphQL error: ${JSON.stringify(j.errors || j)}`);
  }
  return j.data?.Databowl_lead_events_aggregated || [];
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    let {
      offer_id, campaign_id, affiliate_id, sub_id,
      date_from, date_to,
      order = 'day,affiliate,offer',
    } = req.query;

    // Veilige defaults (laatste 14 dagen) als user geen datums kiest
    if (!date_from || !date_to) {
      const now = new Date();
      const past = new Date(); past.setDate(now.getDate()-14);
      date_from = date_from || past.toISOString().slice(0,10);
      date_to   = date_to   || now.toISOString().slice(0,10);
    }

    // Directus filter (GraphQL)
    const filter = { _and: [] };
    if (date_from) filter._and.push({ created_at: { _gte: date_from } });
    if (date_to)   filter._and.push({ created_at: { _lt:  nextDayStr(date_to) } });
    if (offer_id)     filter._and.push({ offer_id:     { _eq: offer_id } });
    if (campaign_id)  filter._and.push({ campaign_id:  { _eq: campaign_id } });
    if (affiliate_id) filter._and.push({ affiliate_id: { _eq: affiliate_id } });
    if (sub_id)       filter._and.push({ sub_id:       { _eq: sub_id } });
    if (!filter._and.length) delete filter._and;

    // Lees gewenste hiërarchie
    const levels = order.split(',').map(s => s.trim()).map(s => {
      if (s === 'affiliate_id') return 'affiliate';
      if (s === 'offer_id')     return 'offer';
      return s; // 'day' | 'affiliate' | 'offer'
    });
    const [L1, L2, L3] = levels;

    // Aggregatie in memory (uit GQL resultatensets)
    const L1map = new Map(); // key -> {key,label, leads:number, cost:number, children:Map}

    const pageSize = 3000; // veilig; verlaag als je timeouts ziet
    for (let offset=0;; offset += pageSize) {
      const chunk = await gqlFetch({ filter, limit: pageSize, offset });
      if (!chunk.length) break;

      for (const row of chunk) {
        const g = row.group || {};
        // GraphQL geeft group velden terug als JSON; pak eruit wat we nodig hebben
        const created_at = g.created_at || null;
        const aff = g.affiliate_id ?? null;
        const off = g.offer_id ?? null;

        const leads = Number(row?.countDistinct?.t_id || 0);
        const cost  = Number(row?.sum?.cost || 0);

        // Herleid keys per niveau uit group JSON
        const lvl1Key = levelKey({ created_at, affiliate_id: aff, offer_id: off }, L1);
        const lvl1Lbl = levelLabel(lvl1Key, L1);
        const n1 = L1map.get(lvl1Key) || { key: lvl1Key, label: lvl1Lbl, leads: 0, cost: 0, children: new Map() };
        n1.leads += leads;
        n1.cost  += cost;

        if (L2) {
          const lvl2Key = levelKey({ created_at, affiliate_id: aff, offer_id: off }, L2);
          const lvl2Lbl = levelLabel(lvl2Key, L2);
          const L2map = n1.children;
          const n2 = L2map.get(lvl2Key) || { key: lvl2Key, label: lvl2Lbl, leads: 0, cost: 0, children: new Map() };
          n2.leads += leads;
          n2.cost  += cost;

          if (L3) {
            const lvl3Key = levelKey({ created_at, affiliate_id: aff, offer_id: off }, L3);
            const lvl3Lbl = levelLabel(lvl3Key, L3);
            const L3map = n2.children;
            const n3 = L3map.get(lvl3Key) || { key: lvl3Key, label: lvl3Lbl, leads: 0, cost: 0 };
            n3.leads += leads;
            n3.cost  += cost;
            L3map.set(lvl3Key, n3);
          }
          L2map.set(lvl2Key, n2);
        }
        L1map.set(lvl1Key, n1);
      }

      if (chunk.length < pageSize) break; // laatste pagina
    }

    // Sort: level1 = dag: nieuwst eerst; anders alfanumeriek
    const sortTop = (lev) => (a,b) => {
      if (lev === 'day') return a.key < b.key ? 1 : -1;
      return (''+a.key).localeCompare(''+b.key, 'nl', { numeric:true });
    };
    const toArray = (map, level, nextLevel) => {
      const arr = Array.from(map.values()).map(n => {
        const out = { key: n.key, label: n.label, leads: n.leads, cost: n.cost };
        if (n.children && n.children.size) {
          out.children = toArray(n.children, nextLevel, null);
        }
        return out;
      });
      return arr.sort(sortTop(level));
    };

    const tree = toArray(L1map, L1, L2);
    return res.status(200).json({ data: { order: levels, tree } });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
