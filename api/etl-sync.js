// /api/etl-sync.js
// Incrementiële sync: Directus -> Supabase (ALLEEN naar events_staging)
// - created_at > cursor (of ?since=YYYY-MM-DD), excl. campaign 925
// - staging upsert op event_key
// - ROBUUSTE parseMoney voor komma / € / thousand separators
// - NIET schrijven naar materialized views (lead_uniques_*)

import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';
const ETL_SECRET = process.env.ETL_SECRET || ''; // optioneel; als gezet moet ?secret= mee

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, { auth: { persistSession: false } });

const EXCLUDED_CAMPAIGN = '925';
const BATCH = 1000;

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// Parse bedragen robuust (ondersteunt "0,15", "1.234,56", "€ 0.15", enz.)
function parseMoney(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? Math.round(v * 100) / 100 : null;

  let s = String(v).trim().replace(/[€\s]/g, '');
  const lastComma = s.lastIndexOf(',');
  const lastDot = s.lastIndexOf('.');

  if (lastComma > -1 && lastDot > -1) {
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(',', '.'); // "1.234,56" -> "1234.56"
    else s = s.replace(/,/g, '');                                        // "1,234.56" -> "1234.56"
  } else if (lastComma > -1) {
    s = s.replace(/\./g, '').replace(',', '.');                          // "1234,56" / "0,15"
  } else {
    s = s.replace(/,/g, '');                                             // "1,234" -> "1234"
  }

  const n = Number(s);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

const toISO = (s) => new Date(s).toISOString();

// Cursor helpers
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

// Directus pagina ophalen
async function fetchDirectusPage({ lastCreatedAt, page = 1, since }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN env vars');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set('fields', [
    'event_key','lead_id','status','revenue','cost','currency',
    'offer_id','campaign_id','affiliate_id','sub_id','t_id','created_at','raw',
  ].join(','));
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('page', String(page));

  const _and = [{ campaign_id: { _neq: EXCLUDED_CAMPAIGN } }];
  if (since) {
    _and.push({ created_at: { _gte: since } });
  } else if (lastCreatedAt) {
    _and.push({ created_at: { _gt: lastCreatedAt } });
  }
  url.searchParams.set('filter', JSON.stringify({ _and }));
  url.searchParams.set('sort', 'created_at');

  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
  let body; try { body = await r.json(); } catch { body = {}; }
  if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(body)}`);
  return Array.isArray(body.data) ? body.data : [];
}

// Upsert naar staging (en hier stoppen we – géén writes naar MVs)
async function upsertStaging(rows) {
  if (!rows.length) return;
  const payload = rows.map((r) => ({
    event_key: r.event_key || null,
    lead_id: r.lead_id || null,
    status: r.status || null,
    revenue: parseMoney(r.revenue),
    cost: parseMoney(r.cost),
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
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE env vars');

    if (ETL_SECRET) {
      const secret = req.query?.secret || '';
      if (secret !== ETL_SECRET) return res.status(401).json({ error: 'Invalid secret' });
    }

    // Optioneel: ?since=YYYY-MM-DD om volledige her-sync vanaf die dag te forceren
    const since = req.query?.since ? new Date(req.query.since).toISOString() : null;

    const { last_created_at: startCursor } = since ? {} : await getSyncCursor();
    let lastCreatedAt = startCursor || null;
    let page = 1;
    let total = 0;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, page, since });
      if (!batch.length) break;

      await upsertStaging(batch);   // <-- alleen staging

      total += batch.length;
      lastCreatedAt = batch[batch.length - 1].created_at;

      await setSyncCursor({
        last_created_at: toISO(lastCreatedAt),
        last_event_key: batch[batch.length - 1].event_key || null,
      });

      page += 1;
      await new Promise((r) => setTimeout(r, 120));
    }

    res.status(200).json({
      ok: true,
      synced: total,
      since: since || startCursor || null,
      last_created_at: lastCreatedAt ? toISO(lastCreatedAt) : null,
      note: 'Schrijft alleen naar events_staging. Refresh je materialized views om totals te updaten.'
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
