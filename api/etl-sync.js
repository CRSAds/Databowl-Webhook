// /api/etl-sync.js
// Incrementiële sync: Directus -> Supabase (alleen events_staging)
// Seek pagination: (created_at, id)  — geen tekstvergelijking meer.
// Cursor wordt bewaard in sync_state (last_created_at + last_id-as-string in last_event_key).

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

// Robuuste geld-parser: "0.12", "0,12", "1.234,56", "1,234.56" -> decimaal
function parseMoney(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? Math.round(v * 100) / 100 : null;
  let s = String(v).trim().replace(/[€\s]/g, '');
  const lastComma = s.lastIndexOf(',');
  const lastDot   = s.lastIndexOf('.');
  if (lastComma > -1 && lastDot > -1) {
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(',', '.'); // 1.234,56 -> 1234.56
    else                     s = s.replace(/,/g, '');                    // 1,234.56 -> 1234.56
  } else if (lastComma > -1) {
    s = s.replace(/\./g, '').replace(',', '.');                          // 1234,56 -> 1234.56
  } else {
    s = s.replace(/,/g, '');                                             // 1,234 -> 1234
  }
  const n = Number(s);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

/* ---------------- cursor in supabase ---------------- */
async function getSyncCursor() {
  const { data, error } = await sb
    .from('sync_state')
    .select('last_created_at,last_event_key')
    .eq('id', 'directus-events')
    .maybeSingle();
  if (error) throw error;
  // last_event_key hergebruiken als last_id (string); parse naar number als mogelijk
  const lastId = data?.last_event_key != null ? Number(data.last_event_key) : null;
  return { last_created_at: data?.last_created_at || null, last_id: Number.isFinite(lastId) ? lastId : null };
}
async function setSyncCursor({ last_created_at, last_id }) {
  const { error } = await sb
    .from('sync_state')
    .upsert(
      {
        id: 'directus-events',
        last_created_at,
        // last_event_key = last_id (string) zodat het met bestaande schema's werkt
        last_event_key: last_id != null ? String(last_id) : null
      },
      { onConflict: 'id' }
    );
  if (error) throw error;
}

/* ---------------- Directus fetch met seek op (created_at,id) ---------------- */
async function fetchDirectusPage({ lastCreatedAt, lastId }) {
  if (!DIRECTUS_URL || !DIRECTUS_TOKEN) throw new Error('Missing DIRECTUS_URL or DIRECTUS_TOKEN');

  const url = new URL(`${DIRECTUS_URL}/items/Databowl_lead_events`);
  url.searchParams.set('fields', [
    'id', // << nodig voor seek
    'event_key','lead_id','status','revenue','cost','currency',
    'offer_id','campaign_id','affiliate_id','sub_id','t_id','created_at','raw',
  ].join(','));
  url.searchParams.set('limit', String(BATCH));
  url.searchParams.set('sort', 'created_at,id'); // << numeric id

  // basisfilter (campagne exclude)
  const filter = { _and: [ { campaign_id: { _neq: EXCLUDED_CAMPAIGN } } ] };

  // seek: created_at > lastCreatedAt, of (created_at == lastCreatedAt en id > lastId)
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

/* ---------------- Upsert naar events_staging ---------------- */
async function upsertStaging(rows) {
  if (!rows.length) return;
  const payload = rows.map((r) => ({
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
  const { error } = await sb.from('events_staging').upsert(payload, { onConflict: 'event_key' });
  if (error) throw error;
}

/* ---------------- Handler ---------------- */
export default async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(500).json({ error: 'Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE' });
    }

    // Optionele backfill: ?since=YYYY-MM-DD (zet cursor terug naar begin van die dag)
    const since = req.query?.since;
    if (since && /^\d{4}-\d{2}-\d{2}$/.test(since)) {
      await setSyncCursor({ last_created_at: `${since}T00:00:00Z`, last_id: 0 });
    }

    const { last_created_at: startCA, last_id: startId } = await getSyncCursor();
    let lastCreatedAt = startCA || null;
    let lastId        = startId != null ? startId : 0;
    let total = 0;

    while (true) {
      const batch = await fetchDirectusPage({ lastCreatedAt, lastId });
      if (!batch.length) break;

      await upsertStaging(batch);
      total += batch.length;

      const tail = batch[batch.length - 1];
      lastCreatedAt = tail.created_at;
      lastId        = Number(tail.id) || lastId;

      await setSyncCursor({
        last_created_at: toISO(lastCreatedAt),
        last_id: lastId,
      });

      if (batch.length < BATCH) break;
      // zachte pauze om rate limits te ontzien
      await new Promise((r) => setTimeout(r, 120));
    }

    return res.status(200).json({
      ok: true,
      synced: total,
      since: startCA || null,
      last_created_at: lastCreatedAt ? toISO(lastCreatedAt) : null,
      last_id: lastId,
      note: 'Schrijft alleen naar events_staging; totals/views lezen uit staging.',
    });
  } catch (e) {
    console.error('[etl-sync] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
