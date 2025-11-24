// api/databowl-webhook.js
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';

// ==== ENV ====
const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';
const SHARED_SECRET = process.env.DATABOWL_WEBHOOK_SECRET || '';

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
        auth: { persistSession: false },
      })
    : null;

// ==== HELPERS ====
const normMoney = (v) =>
  v == null || v === '' ? null : Number.parseFloat(v).toFixed(2);

const asISOFromUnix = (s) => {
  const n = Number(s);
  return Number.isFinite(n)
    ? new Date(n * 1000).toISOString()
    : new Date().toISOString();
};

const FIELD_IDS = {
  offer_id: '1687',
  affiliate_id: '1684',
  supplier_id_via_data: '1685',
  sub_id: null,
  t_id: '1322',
};

const STATUS_MAP = {
  '1': 'Received',
  '2': 'Rejected',
  '3': 'Flagged',
  '4': 'Pending',
  '5': 'Quarantine',
  '6': 'Accepted',
  '7': 'Client Rejected',
  '8': 'Sale',
};

function makeKey(obj) {
  const h = crypto.createHash('sha256');
  h.update(
    [
      obj.lead_id ?? '',
      obj.status ?? '',
      obj.created_at ?? '',
      obj.revenue ?? '',
      obj.cost ?? '',
    ].join('|')
  );
  return h.digest('hex');
}

// ==== MAP DATABOWL → INTERNAL EVENT OBJECT ====
function mapPayload(body) {
  const b = body || {};

  if (b.event === 'lead_update' && b.payload) {
    const p = b.payload;
    const data = p.data || {};

    const lead_id = p.leadId ?? null;
    const campaign_id = p.campaignId != null ? String(p.campaignId) : null;
    const supplier_id = p.supplierId != null ? String(p.supplierId) : null;

    const offer_id = FIELD_IDS.offer_id ? data[FIELD_IDS.offer_id] ?? null : null;
    const affiliate_id = FIELD_IDS.affiliate_id
      ? data[FIELD_IDS.affiliate_id] ?? null
      : null;
    const sub_id = FIELD_IDS.sub_id ? data[FIELD_IDS.sub_id] ?? null : null;
    const t_id = FIELD_IDS.t_id ? data[FIELD_IDS.t_id] ?? null : null;

    const statusId = p.statusId != null ? String(p.statusId) : null;
    const status = statusId
      ? STATUS_MAP[statusId] || `Unknown (${String(statusId)})`
      : 'unknown';

    const created_at = p.receivedAt
      ? asISOFromUnix(p.receivedAt)
      : new Date().toISOString();

    const revenue = normMoney(p.normalRevenue ?? p.revenue);
    const cost = normMoney(p.normalCost ?? p.cost);

    const email = data?.['1'] || null;
    const email_hash = email
      ? crypto.createHash('sha256').update(email.toLowerCase()).digest('hex')
      : null;

    return {
      lead_id,
      status,
      revenue,
      cost,
      currency: 'EUR',
      offer_id,
      campaign_id,
      supplier_id,
      affiliate_id,
      sub_id,
      t_id,
      created_at,
      raw: { original: b, email_hash, status_id: statusId },
    };
  }

  return {};
}

// ==== DIRECTUS INSERT ====
async function createEventDirectus(event) {
  const r = await fetch(`${DIRECTUS_URL}/items/Lead_omzet`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${DIRECTUS_TOKEN}`,
    },
    body: JSON.stringify(event),
  });

  if (r.ok) return { created: await r.json() };

  const txt = await r.text();
  try {
    const j = JSON.parse(txt);
    const nonUnique = j?.errors?.some(
      (e) => e?.extensions?.code === 'RECORD_NOT_UNIQUE'
    );
    if (nonUnique) return { skipped: true };
  } catch {}
  throw new Error(`Directus create ${r.status}: ${txt}`);
}

// ==== SUPABASE INSERT ====
async function insertSupabase(event) {
  if (!supabase) return { skipped: 'no_supabase_config' };

  const { error } = await supabase.from('lead_omzet').insert({
    event_key: event.event_key || null,
    lead_id: event.lead_id || null,
    status: event.status || null,
    revenue: event.revenue != null ? Number(event.revenue) : null,
    cost: event.cost != null ? Number(event.cost) : null,
    currency: event.currency || 'EUR',
    offer_id: event.offer_id || null,
    campaign_id: event.campaign_id || null,
    supplier_id: event.supplier_id || null,
    affiliate_id: event.affiliate_id || null,
    sub_id: event.sub_id || null,
    t_id: event.t_id || null,
    created_at: event.created_at || new Date().toISOString(),
    day: event.day,
    is_shortform: event.is_shortform ? 1 : 0,
  });

  if (error) throw error;
  return { inserted: true };
}

// ==== MAIN HANDLER ====
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    if (req.method !== 'POST')
      return res.status(405).json({ error: 'Method not allowed' });

    const secret = req.query?.secret;
    if (!secret || secret !== SHARED_SECRET) {
      return res.status(401).json({ error: 'Invalid secret' });
    }

    const rawBody =
      req.body && Object.keys(req.body).length
        ? req.body
        : await new Promise((resolve) => {
            let d = '';
            req.on('data', (c) => (d += c));
            req.on('end', () => {
              try {
                resolve(JSON.parse(d));
              } catch {
                resolve({});
              }
            });
          });

    const event = mapPayload(rawBody);
    if (!event.lead_id)
      return res.status(400).json({ error: 'Missing lead_id' });

    event.day = event.created_at.slice(0, 10);

    // === NEW: Shortform & Zero-cost filtering ===
    const isShortform = event.campaign_id === '925';
    event.is_shortform = isShortform ? 1 : 0;

    const costNum = event.cost != null ? Number(event.cost) : 0;

    if (!isShortform && costNum <= 0) {
      return res.status(200).json({
        skipped: true,
        reason: 'ignored_zero_cost_non_shortform',
      });
    }

    // Idempotency key
    const event_key = makeKey(event);
    event.event_key = event_key;

    // ==== DIRECTUS → SOURCE OF TRUTH ====
    const directusResult = await createEventDirectus(event);

    // ==== SUPABASE (best effort) ====
    try {
      await insertSupabase(event);
    } catch (err) {
      console.error('Supabase insert failed:', err);
    }

    return res.status(200).json({
      ok: true,
      directus: directusResult,
    });
  } catch (err) {
    console.error('[webhook] error:', err);
    return res.status(500).json({ error: String(err.message || err) });
  }
}
