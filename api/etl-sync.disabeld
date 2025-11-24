// /api/etl-sync.js
// Incrementiële sync Directus -> Supabase (events_staging), tijd/ID-seek + cursor.
// Ontworpen om korte runs te doen (geen Vercel timeout). Iedere call verwerkt
// een beperkt aantal pagina's en geeft next_cursor terug voor de volgende call.

import { createClient } from '@supabase/supabase-js';

const DIRECTUS_URL   = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';

const SUPABASE_URL   = process.env.SUPABASE_URL || '';
const SUPABASE_KEY   = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_SERVICE_ROLE_KEY || '';

const sb = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

const EXCLUDED_CAMPAIGN = '925';

// ===== helpers =====
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}
const toISO = (s) => new Date(s).toISOString();

// Robuuste geld-parser (punten/komma’s)
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

// ===== cursor in Supabase (we hergebruiken last_event_key om last_id op te slaan) =====
async function getSyncCursor() {
  const { data, error } = await sb
    .from('sync_state')
    .select('last_created_at,last_event_key')
    .eq('id', 'directus-events')
    .maybeSingle();
  if (error) throw error;
  const lastId = data?.last_event_key != null ? Number(data.last_event_key) : null;
  return { last_created_at: data?.last_created_at || null, last_id: Number.isFinite(lastId) ? lastId : null };
}
async function setSyncCursor({ last_created_at, last_id }) {
  const { error } = await sb.from('sync_state').upsert({
    id: 'directus-events',
    last_created_at,
    last_event_key: last_id != null ? String(last_id) : null
  }, { onConflict: 'id' });
  if (error) throw error;
}

// ===== Directus fetch met seek op (created_at, id) =====
async function fetchDirectusPage({ lastCreatedAt, lastId, limit }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set('fields', [
    'id',
    'event_key','lead_id','status','revenue','cost','currency',
    'offer_id','campaign_id','affiliate_id','sub_id','t_id','created_at','raw',
  ].join(','));
  url.searchParams.set('limit', String(limit));
  url.searchParams.set('sort', 'created_at,id');

  const filter = { _and: [ { campaign_id: { _neq: EXCLUDED_CAMPAIGN } } ] };

  if (lastCreatedAt && lastId != null) {
    filter._and.push({
      _or: [
        { created_at: { _gt: lastCreatedAt } },
        { _and: [
          { created_at: { _eq: lastCreatedAt } },
          { id:         { _gt: lastId } }
        ] }
      ]
    });
  } else if (lastCreatedAt) {
    filter._and.push({ created_at: { _gt: lastCreatedAt } });
  }

  url.searchParams.set('filter', JSON.stringify(filter));

  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`Directus ${r.status}: ${JSON.stringify(body)}`);
  return Array.isArray(body.data) ? body.data : [];
}

// ===== staging upsert in kleine chunks (om zware payloads te vermijden) =====
async function upsertStaging(rows, chunkSize = 500) {
  for (let i = 0; i < rows.length; i += chunkSize) {
    const slice = rows.slice(i, i + chunkSize).map((r) => ({
      event_key:   r.event_key || null,
      lead_id:     r.lead_id   || null,
      status:      r.status    || null,
      revenue:     parseMoney(r.revenue),
      cost:        parseMoney(r.cost),
      currency:    r.currency  || null,
      offer_id:    r.offer_id  || null,
      campaign_id: r.campaign_id || null,
      affiliate_id:r.affiliate_id || null,
      sub_id:      r.sub_id    || null,
      t_id:        r.t_id      || null,
      created_at:  r.created_at ? toISO(r.created_at) : new Date().toISOString(),
      raw:         r.raw || null,
    }));
    if (!slice.length) continue;
    const { error } = await sb.from('events_staging').upsert(slice, { onConflict: 'event_key' });
    if (error) throw error;
  }
}

// ===== handler =====
export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE' });
    }

    // Tuning via query:
    //  - batch: hoeveel items per pagina uit Directus (default 1000, verlaag bij timeouts)
    //  - pages: maximaal aantal pagina's per run (default 3)
    //  - since=YYYY-MM-DD: zet cursor terug naar begin van die dag (eenmalige backfill)
    //  - cursor=<base64>{"last_created_at":"...","last_id":123}
    const batch = Math.max(100, Math.min(2000, Number(req.query?.batch) || 1000));
    const maxPages = Math.max(1, Math.min(50, Number(req.query?.pages) || 3));

    const since = req.query?.since;
    let cursor = req.query?.cursor ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8')) : null;

    if (since && /^\d{4}-\d{2}-\d{2}$/.test(since)) {
      await setSyncCursor({ last_created_at: `${since}T00:00:00Z`, last_id: 0 });
      cursor = { last_created_at: `${since}T00:00:00Z`, last_id: 0 };
    }

    let { last_created_at: lastCA, last_id: lastId } =
      cursor || await getSyncCursor();

    if (lastId == null) lastId = 0;

    let total = 0;
    let pagesDone = 0;
    let tailCreatedAt = lastCA;
    let tailId = lastId;

    while (pagesDone < maxPages) {
      const pageRows = await fetchDirectusPage({ lastCreatedAt: tailCreatedAt, lastId: tailId, limit: batch });
      if (!pageRows.length) break;

      await upsertStaging(pageRows, 500);
      total += pageRows.length;

      const tail = pageRows[pageRows.length - 1];
      tailCreatedAt = tail.created_at;
      tailId = Number(tail.id) || tailId;

      await setSyncCursor({ last_created_at: toISO(tailCreatedAt), last_id: tailId });

      pagesDone += 1;

      if (pageRows.length < batch) break; // niets meer te halen
    }

    const has_more = pagesDone === maxPages; // we hebben de harde limiet geraakt; er is waarschijnlijk meer
    const next_cursor_obj = { last_created_at: tailCreatedAt || lastCA, last_id: tailId };
    const next_cursor = Buffer
      .from(JSON.stringify(next_cursor_obj), 'utf8')
      .toString('base64');

    return res.status(200).json({
      ok: true,
      synced: total,
      pages_done: pagesDone,
      batch,
      has_more,
      next_cursor,
      cursor_hint: 'Call this endpoint opnieuw met ?cursor=<value> (of gebruik ?pages=… om meer pagina’s per run te doen).'
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
