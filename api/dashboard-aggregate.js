// --- COMPLETE FILE ---
// Aggregatie vanuit Supabase tabel `lead_uniques_day_grp`.
// - Range guard: max 3 dagen
// - Campagne 925 wordt altijd uitgefilterd
// - Werkt met 2 varianten van de tabel:
//   (A) al geaggregeerd:  day, campaign_id, affiliate_id, offer_id, leads, total_cost
//   (B) niet-geaggregeerd: day, campaign_id, affiliate_id, offer_id, t_id, cost  (we aggregeren in Node)

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
    date_from: clampedFrom.toISOString().slice(0,10),
    date_to: clampedTo.toISOString().slice(0,10),
  };
}

function dateKeyNL(value) {
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(d);
}
function labelForDay(key) {
  const [y,m,d] = (key||'').split('-').map(Number);
  if (!y||!m||!d) return key||'—';
  return new Date(Date.UTC(y,m-1,d)).toLocaleDateString('nl-NL');
}
const toNumber = (v) => Number.isFinite(Number(v)) ? Number(v) : 0;

async function supaGet(path, qs) {
  const q = qs ? `?${qs}` : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}${q}`, {
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'count=exact'
    }
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
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    const { offer_id, campaign_id, affiliate_id, sub_id, order='day,affiliate,offer' } = req.query;

    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });
    const { date_from, date_to } = cr;

    // We vragen alle kolommen op die we kunnen nodig hebben; PostgREST geeft enkel bestaande kolommen terug.
    const p = new URLSearchParams();
    p.append('select', 'day,campaign_id,affiliate_id,offer_id,leads,total_cost,t_id,cost');
    p.append('day', `gte.${date_from}`);
    p.append('day', `lte.${date_to}`);
    p.append('campaign_id', `neq.${EXCLUDED_CAMPAIGN}`);
    if (offer_id)     p.append('offer_id', `eq.${offer_id}`);
    if (campaign_id)  p.append('campaign_id', `eq.${campaign_id}`);
    if (affiliate_id) p.append('affiliate_id', `eq.${affiliate_id}`);
    if (sub_id)       p.append('sub_id', `eq.${sub_id}`); // alleen als kolom bestaat
    p.append('order', 'day.desc,affiliate_id.asc,offer_id.asc');

    const rows = await supaGet('lead_uniques_day_grp', p.toString());
    // rows kunnen 2 smaken zijn (zie header).

    // === Boom opbouwen: day -> affiliate -> offer
    const dayMap = new Map(); // dayKey -> { children: Map(affKey -> { children: Map(offKey -> node) }) }

    // Helper om nodes te pakken/aan te maken
    function ensureNodes(dayKey, campaignId, affKey, offKey) {
      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelForDay(dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      let l2 = dayNode.children.get(affKey);
      if (!l2) {
        l2 = { key: affKey, label: affKey || '—', leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, l2);
      }
      let l3 = l2.children.get(offKey);
      if (!l3) {
        l3 = { key: offKey, label: offKey || '—', leads: 0, cost: 0, campaign_id: campaignId ?? null, __set: new Set() };
        l2.children.set(offKey, l3);
      }
      return { dayNode, l2, l3 };
    }

    // Detecteren of we geaggregeerde kolommen hebben
    const hasAggCols = rows.length && (rows[0].leads !== undefined || rows[0].total_cost !== undefined);

    if (hasAggCols) {
      // Gebruik aggregaties direct
      for (const r of rows) {
        const dayKey = dateKeyNL(r.day);
        const affKey = (r.affiliate_id ?? '') + '';
        const offKey = (r.offer_id ?? '') + '';
        const { dayNode, l2, l3 } = ensureNodes(dayKey, r.campaign_id, affKey, offKey);

        const leads = toNumber(r.leads);
        const cost  = toNumber(r.total_cost);

        l3.leads += leads; l3.cost += cost;
        l2.leads += leads; l2.cost += cost;
        dayNode.leads += leads; dayNode.cost += cost;
      }
    } else {
      // Niet geaggregeerd → zelf distinct t_id tellen + cost sommeren
      for (const r of rows) {
        const dayKey = dateKeyNL(r.day);
        const affKey = (r.affiliate_id ?? '') + '';
        const offKey = (r.offer_id ?? '') + '';
        const { dayNode, l2, l3 } = ensureNodes(dayKey, r.campaign_id, affKey, offKey);

        const tId = (r.t_id ?? '') + '';
        const cost = toNumber(r.cost);

        // distinct t_id
        if (tId && !l3.__set.has(tId)) {
          l3.__set.add(tId);
          l3.leads += 1;
          l2.leads += 1;
          dayNode.leads += 1;
        }
        l3.cost += cost; l2.cost += cost; dayNode.cost += cost;
      }

      // opruimen helper set
      for (const d of dayMap.values()) {
        for (const a of d.children.values()) {
          for (const o of a.children.values()) delete o.__set;
        }
      }
    }

    // Naar arrays + sorteringen
    const sortTop = (level) => (a,b)=>{
      if (level==='day') return a.key < b.key ? 1 : -1;
      return (''+a.key).localeCompare(''+b.key, 'nl', { numeric:true });
    };
    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n=>{
        const out = { key:n.key, label:n.label, leads:n.leads||0, cost:n.cost||0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) {
          out.children = toArray(n.children, level==='day' ? 'affiliate' : 'offer');
        }
        return out;
      });
      return arr.sort(sortTop(level));
    };

    const tree = toArray(dayMap, 'day');
    return res.status(200).json({ data: { order: ['day','affiliate','offer'], tree } });
  } catch (err) {
    console.error('[dashboard-aggregate] error', err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
