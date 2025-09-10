// /api/etl-sync.js
// Incrementiële sync: Directus -> Supabase (ALLEEN staging)
// - created_at > cursor, excl. campaign 925
// - upsert op events_staging(event_key)
// - GEEN writes naar lead_uniques_day_grp (dat is een MV)

import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

const EXCLUDED_CAMPAIGN = '925';
const BATCH = 1000;

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

const toISO = (s) => new Date(s).toISOString();

// cursor
async function getSyncCursor() {
  const { data, error } = await sb.from('sync_state').select('*').eq('id', 'directus-events').maybeSingle();
  if (error) throw error;
  return data || {};
}
async function setSyncCursor({ last_created_at, last_event_key }) {
  const { error } = await sb
    .from('sync_state')
    .upsert({ id: 'directus-events', last_created_at, last_event_key }, { onConflict: 'id' });
  if (error) throw error;
}

// haal pagina uit Directus
async function fetchDirectusPage({ lastCreatedAt, page = 1 }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set('fields', [
    'event_key','lead_id','status','revenue','cost','currency',
    'offer_id','campaign_id','affiliate_id','sub_id','t_id','created_at','raw',
  ].join(','));
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('page', String(page));
  url.searchParams.set('filter', JSON.stringify({
    _and: [
      lastCreatedAt ? { created_at: { _gt: lastCreatedAt } } : {},
      { campaign_id: { _neq: EXCLUDED_CAMPAIGN } },
    ],
  }));
  url.searchParams.set('sort', 'created_at');

  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
  let body; try { body = await r.json(); } catch { body = {}; }
  if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(body)}`);
  return Array.isArray(body.data) ? body.data : [];
}

// staging upsert
async function upsertStaging(rows) {
  if (!rows.length) return;
  const payload = rows.map((r) => ({
    event_key: r.event_key || null,
    lead_id: r.lead_id || null,
    status: r.status || null,
    revenue: r.revenue != null ? Number(r.revenue) : null,
    cost: r.cost != null ? Number(r.cost) : null,
    currency: r.currency || null,
    offer_id: r.offer_id || null,
    campaign_id: r.campaign_id || null,
    affiliate_id: r.affiliate_id || null,
    sub_id: r.sub_id || null,
    t_id: r.t_id || null,
    created_at: r.created_at ? toISO(r.created_at) : new Date().toISOString(),
    raw: r.raw || null,
  }));
  const { error } = await sb.from('events_staging').upsert(payload, { onConflict: 'event_key' });
  if (error) throw error;
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE');

    const secret = req.query?.secret;
    const expected = process.env.ETL_SECRET || process.env.DATABOWL_WEBHOOK_SECRET || '';
    if (!expected || secret !== expected) return res.status(401).json({ error: 'Invalid secret' });

    const since = req.query?.since; // optioneel: forceer vanaf dit moment
    const { last_created_at: startCursor } = await getSyncCursor();
    let lastCreatedAt = since || startCursor || null;

    let page = 1;
    let total = 0;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, page });
      if (!batch.length) break;

      await upsertStaging(batch);

      total += batch.length;
      lastCreatedAt = batch[batch.length - 1].created_at;

      await setSyncCursor({
        last_created_at: toISO(lastCreatedAt),
        last_event_key: batch[batch.length - 1].event_key || null,
      });

      page += 1;
      await new Promise((r) => setTimeout(r, 120)); // kleine pauze
    }

    res.status(200).json({
      ok: true,
      synced: total,
      since: since || startCursor || null,
      last_created_at: lastCreatedAt ? toISO(lastCreatedAt) : null,
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
