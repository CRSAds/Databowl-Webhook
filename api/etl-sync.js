// /api/etl-sync.js
// Incrementiële sync: Directus -> Supabase (alleen events_staging)
// - Seek pagination op (created_at, event_key)
// - Exclude campaign 925
// - Geld parsing robuust (0.12 / 0,12 etc.)

import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL   = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

const SUPABASE_URL   = process.env.SUPABASE_URL || '';
const SUPABASE_KEY   = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const sb = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

const EXCLUDED_CAMPAIGN = '925';
const BATCH = 1000;

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}
const toISO = (s) => new Date(s).toISOString();

function parseMoney(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? Math.round(v * 100) / 100 : null;
  let s = String(v).trim().replace(/[€\s]/g, '');
  const lastComma = s.lastIndexOf(',');
  const lastDot   = s.lastIndexOf('.');
  if (lastComma > -1 && lastDot > -1) {
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(',', '.');
    else                     s = s.replace(/,/g, '');
  } else if (lastComma > -1) {
    s = s.replace(/\./g, '').replace(',', '.');
  } else {
    s = s.replace(/,/g, '');
  }
  const n = Number(s);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

// ---- cursor opslag in supabase ----
async function getSyncCursor() {
  const { data, error } = await sb
    .from('sync_state')
    .select('last_created_at,last_event_key')
    .eq('id', 'directus-events')
    .maybeSingle();
  if (error) throw error;
  return data || {};
}
async function setSyncCursor({ last_created_at, last_event_key }) {
  const { error } = await sb
    .from('sync_state')
    .upsert({ id: 'directus-events', last_created_at, last_event_key }, { onConflict: 'id' });
  if (error) throw error;
}

// ---- Directus fetch met seek pagination ----
async function fetchDirectusPage({ lastCreatedAt, lastEventKey }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set('fields', [
    'event_key','lead_id','status','revenue','cost','currency',
    'offer_id','campaign_id','affiliate_id','sub_id','t_id','created_at','raw',
  ].join(','));
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('sort', 'created_at,event_key');

  const filter = { _and: [ { campaign_id: { _neq: EXCLUDED_CAMPAIGN } } ] };
  if (lastCreatedAt && lastEventKey) {
    filter._and.push({
      _or: [
        { created_at: { _gt: lastCreatedAt } },
        { _and: [
          { created_at: { _eq: lastCreatedAt } },
          { event_key:  { _gt: lastEventKey } }
        ] }
      ]
    });
  } else if (lastCreatedAt) {
    filter._and.push({ created_at: { _gt: lastCreatedAt } });
  }
  url.searchParams.set('filter', JSON.stringify(filter));

  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
  const body = await r.json().catch(()=> ({}));
  if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(body)}`);
  return Array.isArray(body.data) ? body.data : [];
}

// ---- Upsert naar events_staging ----
async function upsertStaging(rows) {
  if (!rows.length) return;
  const payload = rows.map((r) => ({
    event_key:   r.event_key || null,
    lead_id:     r.lead_id   || null,
    status:      r.status    || null,
    revenue:     parseMoney(r.revenue),
    cost:        parseMoney(r.cost),     // << alleen punt->komma normalisatie & nummer
    currency:    r.currency  || null,
    offer_id:    r.offer_id   || null,
    campaign_id: r.campaign_id|| null,
    affiliate_id:r.affiliate_id || null,
    sub_id:      r.sub_id     || null,
    t_id:        r.t_id       || null,
    created_at:  r.created_at ? toISO(r.created_at) : new Date().toISOString(),
    raw:         r.raw || null,
  }));
  const { error } = await sb.from('events_staging').upsert(payload, { onConflict: 'event_key' });
  if (error) throw error;
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE' });
    }

    // Optionele backfill: ?since=YYYY-MM-DD
    const since = req.query?.since;
    if (since && /^\d{4}-\d{2}-\d{2}$/.test(since)) {
      await setSyncCursor({ last_created_at: `${since}T00:00:00Z`, last_event_key: null });
    }

    const { last_created_at: startCA, last_event_key: startEK } = await getSyncCursor();
    let lastCreatedAt = startCA || null;
    let lastEventKey  = startEK || null;
    let total = 0;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, lastEventKey });
      if (!batch.length) break;

      await upsertStaging(batch);
      total += batch.length;

      const tail = batch[batch.length - 1];
      lastCreatedAt = tail.created_at;
      lastEventKey  = tail.event_key || null;

      await setSyncCursor({
        last_created_at: toISO(lastCreatedAt),
        last_event_key:  lastEventKey
      });

      if (batch.length < BATCH) break;
      await new Promise((r) => setTimeout(r, 120));
    }

    return res.status(200).json({
      ok: true,
      synced: total,
      since: startCA || null,
      last_created_at: lastCreatedAt ? toISO(lastCreatedAt) : null,
      last_event_key: lastEventKey || null,
      note: 'Wrote only to events_staging; totals/views should read from staging.',
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
