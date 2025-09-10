// /api/etl-sync.js
// Incrementiële sync Directus -> Supabase (staging + dedupe)
// Node 18+ (Vercel). Runt on-demand of via cron.

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

const EXCLUDED_CAMPAIGN = '925';
const BATCH = 1000; // Directus page size

// klein hulpfunctietje
const toISO = (s) => new Date(s).toISOString();

// haalt "tot waar" we al gesynct hebben
async function getSyncCursor() {
  const { data, error } = await sb.from('sync_state').select('*').eq('id', 'directus-events').maybeSingle();
  if (error) throw error;
  return data || {};
}

// slaat cursor op
async function setSyncCursor({ last_created_at, last_event_key }) {
  const { error } = await sb
    .from('sync_state')
    .upsert({ id: 'directus-events', last_created_at, last_event_key }, { onConflict: 'id' });
  if (error) throw error;
}

// haalt één pagina uit Directus
async function fetchDirectusPage({ lastCreatedAt, page = 1 }) {
  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  // velden die we nodig hebben
  url.searchParams.set('fields', [
    'event_key',
    'lead_id',
    'status',
    'revenue',
    'cost',
    'currency',
    'offer_id',
    'campaign_id',
    'affiliate_id',
    'sub_id',
    't_id',
    'created_at',
    'raw',
  ].join(','));
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('page', String(page));
  // filter: alleen nieuwe records en campagne 925 eruit
  const f = {
    _and: [
      lastCreatedAt ? { created_at: { _gt: lastCreatedAt } } : {},
      { campaign_id: { _neq: EXCLUDED_CAMPAIGN } },
    ],
  };
  url.searchParams.set('filter', JSON.stringify(f));
  // sorteer oplopend op created_at voor stabiele cursor
  url.searchParams.set('sort', 'created_at');

  const r = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
  });
  const j = await r.json();
  if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(j)}`);

  return Array.isArray(j.data) ? j.data : [];
}

// schrijft batch in staging (idempotent op event_key)
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

// schrijft naar dedupe-tabel (distinct t_id per dag/groep); ON CONFLICT = ignore
async function upsertDedupe(rows) {
  if (!rows.length) return;
  // omzet naar dag + keys
  const dedupPayload = rows
    .filter((r) => r.t_id) // t_id noodzakelijk voor distinct
    .map((r) => ({
      day: new Date(r.created_at).toLocaleDateString('sv-SE', { timeZone: 'Europe/Amsterdam' }), // yyyy-mm-dd
      affiliate_id: r.affiliate_id || null,
      offer_id: r.offer_id || null,
      campaign_id: r.campaign_id || null,
      t_id: r.t_id,
      cost: r.cost != null ? Number(r.cost) : null,
    }));

  // Supabase-js ondersteunt ON CONFLICT DO NOTHING via Upsert met ignoreDuplicates
  const { error } = await sb
    .from('lead_uniques_day_grp')
    .upsert(dedupPayload, {
      onConflict: 'day,affiliate_id,offer_id,campaign_id,t_id',
      ignoreDuplicates: true,
    });
  if (error) throw error;
}

export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const { last_created_at: startCursor } = await getSyncCursor();
    let lastCreatedAt = startCursor || null;
    let total = 0;
    let page = 1;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, page });
      if (!batch.length) break;

      await upsertStaging(batch);
      await upsertDedupe(batch);

      total += batch.length;
      lastCreatedAt = batch[batch.length - 1].created_at;
      await setSyncCursor({ last_created_at: toISO(lastCreatedAt), last_event_key: batch[batch.length - 1].event_key || null });

      // volgende pagina
      page += 1;

      // kleine adempauze om Directus niet te belasten
      await new Promise((r) => setTimeout(r, 150));
    }

    return res.status(200).json({ ok: true, synced: total, since: startCursor || null, last_created_at: lastCreatedAt || null });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
