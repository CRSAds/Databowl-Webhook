// /api/metrics-aggregate.js
// Snelle aggregatie-feed voor het dashboard uit v_lead_metrics_day
// Hiërarchie: Dag → Affiliate → Offer (order in response meegegeven)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL =
  (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_ROLE || // fallback – oude naam
  '';

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function todayYMDInTZ(tz) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  }).formatToParts(new Date());
  const get = t => parts.find(p => p.type === t)?.value;
  return `${get('year')}-${get('month')}-${get('day')}`;
}

function addDaysYMD(ymd, delta) {
  const [y, m, d] = (ymd || '').split('-').map(Number);
  const dt = new Date(Date.UTC(y, (m || 1) - 1, d || 1));
  dt.setUTCDate(dt.getUTCDate() + delta);
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(dt.getUTCDate()).padStart(2, '0');
  return `${yy}-${mm}-${dd}`;
}

const isValidYMD = s => /^\d{4}-\d{2}-\d{2}$/.test(s);

const sortTopByLevel = level => (a, b) => {
  if (level === 'day') return a.key < b.key ? 1 : -1; // nieuwste dag eerst
  return ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });
};

const nlDateLabel = ymd => {
  const [y, m, d] = (ymd || '').split('-').map(Number);
  if (!y || !m || !d) return ymd || '—';
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
};

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET')    return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY' });
    }

    // Query params
    let {
      date_from,
      date_to,
      affiliate_id,
      offer_id,
      campaign_id,
      order = 'day,affiliate,offer',
    } = req.query;

    // Defaults: laatste 3 dagen in Europe/Amsterdam
    const tz = 'Europe/Amsterdam';
    const toDefault = todayYMDInTZ(tz);
    const fromDefault = addDaysYMD(toDefault, -2);

    const from = date_from && isValidYMD(date_from) ? date_from : fromDefault;
    const to   = date_to   && isValidYMD(date_to)   ? date_to   : toDefault;

    if (!isValidYMD(from) || !isValidYMD(to)) {
      return res.status(400).json({ error: 'Invalid date format; use YYYY-MM-DD for date_from/date_to' });
    }

    // Basisselectie – pas de kolomnamen aan als jouw view anders heet
    let q = sb
      .from('v_lead_metrics_day')
      .select('day,affiliate_id,offer_id,campaign_id,leads,cost')
      .gte('day', from)
      .lte('day', to)
      .order('day', { ascending: false })
      .order('affiliate_id', { ascending: true })
      .order('offer_id', { ascending: true });

    if (affiliate_id) q = q.eq('affiliate_id', affiliate_id);
    if (offer_id)     q = q.eq('offer_id', offer_id);
    if (campaign_id)  q = q.eq('campaign_id', campaign_id);

    const { data, error } = await q;
    if (error) {
      // Geef duidelijke hint als kolom/View niet bestaat
      throw new Error(`Supabase v_lead_metrics_day: ${error.message || String(error)}`);
    }

    // Boom bouwen: day -> affiliate -> offer
    const dayMap = new Map();
    for (const r of data || []) {
      // Defensief: map eventueel afwijkende kolomnamen (bv. total_leads → leads)
      const leadsVal = r.leads ?? r.total_leads ?? r.lead_count ?? 0;
      const costVal  = r.cost  ?? r.total_cost  ?? 0;

      const dayKey = String(r.day);
      const affKey = r.affiliate_id || '';
      const offKey = r.offer_id || '';

      let dayNode = dayMap.get(dayKey);
      if (!dayNode) {
        dayNode = { key: dayKey, label: nlDateLabel(dayKey), leads: 0, cost: 0, children: new Map() };
        dayMap.set(dayKey, dayNode);
      }
      let affNode = dayNode.children.get(affKey);
      if (!affNode) {
        affNode = { key: affKey, label: affKey || '—', leads: 0, cost: 0, children: new Map() };
        dayNode.children.set(affKey, affNode);
      }
      let offNode = affNode.children.get(offKey);
      if (!offNode) {
        offNode = { key: offKey, label: offKey || '—', leads: 0, cost: 0, campaign_id: r.campaign_id || '' };
        affNode.children.set(offKey, offNode);
      }

      const leads = Number(leadsVal || 0);
      const cost  = Number(costVal  || 0);

      offNode.leads += leads; offNode.cost += cost;
      affNode.leads += leads; affNode.cost += cost;
      dayNode.leads += leads; dayNode.cost += cost;
    }

    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n => {
        const out = { key: n.key, label: n.label, leads: n.leads || 0, cost: n.cost || 0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) out.children = toArray(n.children, level === 'day' ? 'affiliate' : 'offer');
        return out;
      });
      return arr.sort(sortTopByLevel(level));
    };

    const tree = toArray(dayMap, 'day');

    res.status(200).json({
      data: {
        order: ['day','affiliate','offer'],
        applied_filters: {
          from, to,
          affiliate_id: affiliate_id || null,
          offer_id: offer_id || null,
          campaign_id: campaign_id || null
        },
        tree
      }
    });
  } catch (e) {
    console.error('[metrics-aggregate] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
