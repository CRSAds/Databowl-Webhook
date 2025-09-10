// /api/dashboard-aggregate.js  (Supabase versie)
// Range ≤ 3 dagen (inclusief end), campagne 925 uitgesloten (read-only).
// Data komt uit de ETL-aggregatietabel `lead_uniques_day_grp` op Supabase.
// Optioneel: als de view/tabel `lead_uniques_day` bestaat gebruiken we die voor
// correcte L1 (per dag) DISTINCT totals. Anders vallen we terug op som van kinderen.

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const MAX_DAYS = 3;
const EXCLUDED_CAMPAIGN = '925';

// --- helpers ---
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function mustEnv(name, val) {
  if (!val) {
    throw new Error(`Missing env ${name}`);
  }
}

function svDateKey(value) {
  // yyyy-mm-dd in Europe/Amsterdam
  const d = new Date(value);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

function labelFor(level, key) {
  if (level === 'day') {
    const [y, m, d] = (key || '').split('-').map(Number);
    if (!y || !m || !d) return key || '—';
    return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
  }
  return key || '—';
}

function sortTop(level) {
  return (a, b) => {
    if (level === 'day') return a.key < b.key ? 1 : -1; // nieuwste dag eerst
    return ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });
  };
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
  if (days > MAX_DAYS) {
    return { error: `Date range too large (${days}d). Max ${MAX_DAYS} days.` };
  }
  return {
    from: clampedFrom.toISOString().slice(0, 10),
    to: clampedTo.toISOString().slice(0, 10),
  };
}

function toMoneyNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// --- Supabase REST helper ---
async function sbGet(path, params = {}) {
  mustEnv('SUPABASE_URL', SUPABASE_URL);
  mustEnv('SUPABASE_SERVICE_ROLE', SUPABASE_SERVICE_ROLE);

  const usp = new URLSearchParams(params);
  const url = `${SUPABASE_URL}/rest/v1/${path}${usp.toString() ? `?${usp}` : ''}`;

  const r = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      Prefer: 'count=exact',
    },
  });

  // 404 op een view/tabel die niet bestaat → behandel als lege set
  if (r.status === 404) return { data: [], ok: false, status: 404, text: await r.text() };

  const txt = await r.text();
  let data = [];
  try { data = txt ? JSON.parse(txt) : []; } catch { /* ignore */ }

  if (!r.ok) {
    throw new Error(`Supabase ${path} ${r.status}: ${txt}`);
  }
  return { data, ok: true, status: r.status };
}

/**
 * Haal drilldownregels op uit lead_uniques_day_grp
 * Output-rijen bevatten: day, affiliate_id, offer_id, campaign_id, leads, total_cost
 */
async function fetchGroupRows({ from, to, offer_id, campaign_id, affiliate_id, sub_id }) {
  // Baseselect
  const select =
    'select=day,affiliate_id,offer_id,campaign_id,leads,total_cost';

  // Filters
  const params = {
    [select]: '',
    'day=gte': from,
    'day=lte': to,
    // globale exclude
    'campaign_id=neq': EXCLUDED_CAMPAIGN,
    order: 'day.desc',
  };

  if (offer_id) params['offer_id=eq'] = offer_id;
  if (campaign_id) params['campaign_id=eq'] = campaign_id;
  if (affiliate_id) params['affiliate_id=eq'] = affiliate_id;
  if (sub_id) params['sub_id=eq'] = sub_id; // alleen als je sub_id in de tabel hebt

  const { data } = await sbGet('lead_uniques_day_grp', params);
  return Array.isArray(data) ? data : [];
}

/**
 * Totals per dag (distinct) – gebruikt `lead_uniques_day` als die bestaat,
 * anders berekenen we de som van kinderen.
 */
async function fetchDayTotalsDistinct({ from, to, offer_id, campaign_id, affiliate_id, sub_id }) {
  const select = 'select=day,leads,total_cost';

  const params = {
    [select]: '',
    'day=gte': from,
    'day=lte': to,
    'campaign_id=neq': EXCLUDED_CAMPAIGN,
    order: 'day.desc',
  };
  if (offer_id) params['offer_id=eq'] = offer_id;
  if (campaign_id) params['campaign_id=eq'] = campaign_id;
  if (affiliate_id) params['affiliate_id=eq'] = affiliate_id;
  if (sub_id) params['sub_id=eq'] = sub_id;

  const res = await sbGet('lead_uniques_day', params);
  if (!res.ok) return []; // view bestaat niet → caller zal fallback doen

  const rows = Array.isArray(res.data) ? res.data : [];
  // normaliseer keys naar dezelfde types
  return rows.map(r => ({
    day: svDateKey(r.day || r.day_key || r.date || r.d),
    leads: Number(r.leads || 0),
    total_cost: toMoneyNumber(r.total_cost || 0),
  }));
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  // iets cachebaar op edge/CDN
  res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');

  try {
    const {
      offer_id,
      campaign_id,
      affiliate_id,
      sub_id,
      order = 'day,affiliate,offer',
    } = req.query;

    const cr = clampRange(req.query.date_from, req.query.date_to);
    if (cr.error) return res.status(400).json({ error: cr.error });

    // 1) haal alle leaf-rijen (dag → affiliate → offer)
    const rows = await fetchGroupRows({ ...cr, offer_id, campaign_id, affiliate_id, sub_id });

    // 2) bouw boom (dag → affiliate → offer)
    const dayMap = new Map(); // dayKey → node

    for (const r of rows) {
      const dayKey = svDateKey(r.day);
      const affKey = (r.affiliate_id ?? '') + '';
      const offKey = (r.offer_id ?? '') + '';
      const campId = r.campaign_id ?? null;

      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: labelFor('day', dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }

      let l2 = dayNode.children.get(affKey);
      if (!l2) {
        l2 = { key: affKey, label: labelFor('affiliate', affKey), leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, l2);
      }

      let l3 = l2.children.get(offKey);
      if (!l3) {
        l3 = { key: offKey, label: labelFor('offer', offKey), leads: 0, cost: 0, campaign_id: campId };
        l2.children.set(offKey, l3);
      }

      const leads = Number(r.leads || 0);
      const cost = toMoneyNumber(r.total_cost || 0);

      l3.leads += leads;
      l3.cost  += cost;
      l2.leads += leads;
      l2.cost  += cost;
      // L1 totals zetten we hier nog NIET (kan overcount geven) → stap 3
    }

    // 3) L1 totals: probeer DISTINCT totals uit `lead_uniques_day`. Fallback: som van kinderen.
    const distinctTotals = await fetchDayTotalsDistinct({ ...cr, offer_id, campaign_id, affiliate_id, sub_id })
      .catch(() => []);

    const dtMap = new Map();
    for (const r of distinctTotals) dtMap.set(svDateKey(r.day), { leads: r.leads, cost: r.total_cost });

    for (const [dayKey, node] of dayMap.entries()) {
      const distinct = dtMap.get(dayKey);
      if (distinct) {
        node.leads = distinct.leads;
        node.cost  = distinct.cost;
      } else {
        // fallback – som van alle children
        let leads = 0, cost = 0;
        for (const l2 of node.children.values()) {
          leads += Number(l2.leads || 0);
          cost  += toMoneyNumber(l2.cost || 0);
        }
        node.leads = leads;
        node.cost  = cost;
      }
    }

    // 4) Map → arrays + sort
    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n => {
        const out = { key: n.key, label: n.label, leads: n.leads || 0, cost: n.cost || 0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) {
          const nextLevel = (level === 'day') ? 'affiliate' : 'offer';
          out.children = toArray(n.children, nextLevel);
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
          from: cr.from, to: cr.to,
          affiliate_id: affiliate_id || null,
          offer_id: offer_id || null,
          campaign_id: campaign_id || null
        },
        tree
      }
    });
  } catch (e) {
    console.error('[dashboard-aggregate] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
