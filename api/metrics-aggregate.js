// /api/metrics-aggregate.js
// Snelle aggregatie-feed voor je dashboard, uit v_lead_metrics_day
// Default: laatste 3 dagen, Dag → Affiliate → Offer

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';
const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function toISODate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function sortTopByLevel(level) {
  return (a, b) => {
    if (level === 'day') return a.key < b.key ? 1 : -1; // nieuwste dag eerst
    return ('' + a.key).localeCompare('' + b.key, 'nl', { numeric: true });
  };
}

function nlDateLabel(yyyy_mm_dd) {
  const [y, m, d] = yyyy_mm_dd.split('-').map(Number);
  if (!y || !m || !d) return yyyy_mm_dd;
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('nl-NL');
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    // Query params
    const {
      date_from,
      date_to,
      affiliate_id,
      offer_id,
      campaign_id,
      order = 'day,affiliate,offer',
    } = req.query;

    // Default laatste 3 dagen (Europe/Amsterdam)
    const now = new Date();
    const tzNow = new Date(now.toLocaleString('en-CA', { timeZone: 'Europe/Amsterdam' }));
    const to = date_to || toISODate(tzNow);
    const fromDate = new Date(tzNow); fromDate.setDate(fromDate.getDate() - 2);
    const from = date_from || toISODate(fromDate);

    // Basisselectie
    let q = sb.from('v_lead_metrics_day')
      .select('day,affiliate_id,offer_id,campaign_id,leads,cost')
      .gte('day', from)
      .lte('day', to);

    if (affiliate_id) q = q.eq('affiliate_id', affiliate_id);
    if (offer_id)     q = q.eq('offer_id', offer_id);
    if (campaign_id)  q = q.eq('campaign_id', campaign_id);

    // NB: Campagne 925 is al niet ingestroomd via ETL; extra defensief kun je dit nog afdwingen:
    // q = q.neq('campaign_id', '925');

    // Sort stabiel voor boomopbouw
    q = q.order('day', { ascending: false }).order('affiliate_id', { ascending: true }).order('offer_id', { ascending: true });

    const { data, error } = await q;
    if (error) throw error;

    // Boom: day -> affiliate -> offer
    const dayMap = new Map();

    for (const r of data || []) {
      const dayKey = String(r.day); // 'YYYY-MM-DD'
      const affKey = r.affiliate_id || '';
      const offKey = r.offer_id || '';
      const leads = Number(r.leads || 0);
      const cost  = Number(r.cost  || 0);

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

      // Optellen per niveau (hier klopt optellen wél, omdat v_lead_metrics_day al distinct is)
      offNode.leads += leads; offNode.cost += cost;
      affNode.leads += leads; affNode.cost += cost;
      dayNode.leads += leads; dayNode.cost += cost;
    }

    const toArray = (map, level) => {
      const arr = Array.from(map.values()).map(n => {
        const out = { key: n.key, label: n.label, leads: n.leads || 0, cost: n.cost || 0 };
        if (n.campaign_id != null) out.campaign_id = n.campaign_id;
        if (n.children && n.children.size) {
          out.children = toArray(n.children, level === 'day' ? 'affiliate' : 'offer');
        }
        return out;
      });
      return arr.sort(sortTopByLevel(level));
    };

    const tree = toArray(dayMap, 'day');
    const levels = order.split(',').map(s => s.trim()).map(s => {
      if (s === 'affiliate_id') return 'affiliate';
      if (s === 'offer_id') return 'offer';
      return s;
    });

    res.status(200).json({
      data: {
        order: ['day','affiliate','offer'], // huidige hiërarchie
        applied_filters: { from, to, affiliate_id: affiliate_id || null, offer_id: offer_id || null, campaign_id: campaign_id || null },
        tree
      }
    });
  } catch (e) {
    console.error('[metrics-aggregate] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
