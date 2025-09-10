// /api/etl-sync.js
// Incrementiële sync: Directus -> Supabase (staging + dedupe)
// - created_at > cursor (of ?since=...), excl. campaign 925
// - staging upsert op event_key
// - dedupe upsert met NOT NULL normalisatie (lege string) op PK-velden
// - beveiligd met shared secret (zelfde als databowl-webhook)

import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE =
  process.env.SUPABASE_SERVICE_ROLE ||
  process.env.SUPABASE_SERVICE_ROLE_KEY || ''; // beide namen ondersteunen

const SHARED_SECRET = process.env.DATABOWL_WEBHOOK_SECRET || '';

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

const EXCLUDED_CAMPAIGN = '925';
const BATCH = 1000;          // Directus page size
const SLEEP_MS = 150;        // kleine pauze tussen pagina's

/* ---------- Helpers ---------- */
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const toISO = (s) => new Date(s).toISOString();

function dateKeyNL(iso) {
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Amsterdam',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date(iso));
}

/* ---------- Cursor storage in Supabase ---------- */
async function getSyncCursor() {
  const { data, error } = await sb
    .from('sync_state')
    .select('*')
    .eq('id', 'directus-events')
    .maybeSingle();
  if (error) throw error;
  return data || {};
}

async function setSyncCursor({ last_created_at, last_event_key }) {
  const { error } = await sb
    .from('sync_state')
    .upsert(
      { id: 'directus-events', last_created_at, last_event_key },
      { onConflict: 'id' }
    );
  if (error) throw error;
}

async function clearSyncCursor() {
  const { error } = await sb.from('sync_state').delete().eq('id', 'directus-events');
  if (error) throw error;
}

/* ---------- Directus fetch ---------- */
async function fetchDirectusPage({ lastCreatedAt, page = 1 }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN)
    throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN env vars');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set(
    'fields',
    [
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
    ].join(',')
  );
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('page', String(page));

  // filter via _and; excl. campagne 925 en incremental cursor
  url.searchParams.set(
    'filter',
    JSON.stringify({
      _and: [
        lastCreatedAt ? { created_at: { _gt: lastCreatedAt } } : {},
        { campaign_id: { _neq: EXCLUDED_CAMPAIGN } },
      ],
    })
  );

  url.searchParams.set('sort', 'created_at');

  const r = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
  });

  let body;
  try {
    body = await r.json();
  } catch {
    body = {};
  }

  if (!r.ok) {
    throw new Error(`Directus ${r.status}: ${JSON.stringify(body)}`);
  }

  return Array.isArray(body.data) ? body.data : [];
}

/* ---------- Upserts ---------- */
// 1) staging: "ruw" vastleggen
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

  const { error } = await sb
    .from('events_staging')
    .upsert(payload, { onConflict: 'event_key' });
  if (error) throw error;
}

// 2) dedupe: unieke t_id per dag + keys (alle PK-velden NOT NULL via lege string)
async function upsertDedupe(rows) {
  if (!rows.length) return;

  const payload = rows
    .filter((r) => r.t_id) // t_id is vereist om dedupen te kunnen
    .map((r) => ({
      day: dateKeyNL(r.created_at),
      affiliate_id: r.affiliate_id ?? '',
      offer_id: r.offer_id ?? '',
      campaign_id: r.campaign_id ?? '',
      t_id: r.t_id,
      cost: r.cost != null ? Number(r.cost) : null,
    }));

  if (!payload.length) return;

  const { error } = await sb
    .from('lead_uniques_day_grp')
    .upsert(payload, {
      onConflict: 'day,affiliate_id,offer_id,campaign_id,t_id',
      ignoreDuplicates: true, // safety
    });

  if (error) throw error;
}

/* ---------- Handler ---------- */
export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    // Secret check (zelfde als webhook)
    if (!SHARED_SECRET) {
      return res.status(500).json({ error: 'Missing DATABOWL_WEBHOOK_SECRET env var' });
    }
    const urlSecret = req.query?.secret || req.headers['x-sync-secret'];
    if (!urlSecret || urlSecret !== SHARED_SECRET) {
      return res.status(401).json({ error: 'Invalid secret' });
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
      return res
        .status(500)
        .json({ error: 'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE env vars' });
    }

    // Query overrides
    const sinceOverride = req.query?.since; // YYYY-MM-DD of ISO
    const doReset = req.query?.reset === '1';
    const dryRun = req.query?.dryrun === '1';

    if (doReset) await clearSyncCursor();

    const { last_created_at: storedCursor } = await getSyncCursor();
    const startCursor = sinceOverride
      ? new Date(sinceOverride).toISOString()
      : storedCursor || null;

    let lastCreatedAt = startCursor || null;
    let page = 1;
    let total = 0;
    let lastEventKey = null;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, page });
      if (!batch.length) break;

      if (!dryRun) {
        await upsertStaging(batch);
        await upsertDedupe(batch);
      }

      total += batch.length;
      lastCreatedAt = batch[batch.length - 1].created_at;
      lastEventKey = batch[batch.length - 1].event_key || null;

      if (!dryRun) {
        await setSyncCursor({
          last_created_at: toISO(lastCreatedAt),
          last_event_key: lastEventKey,
        });
      }

      page += 1;
      await sleep(SLEEP_MS);
    }

    res.status(200).json({
      ok: true,
      synced: total,
      since: startCursor || null,
      last_created_at: lastCreatedAt ? toISO(lastCreatedAt) : null,
      last_event_key: lastEventKey,
      dry_run: !!dryRun,
      reset_cursor: !!doReset,
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
