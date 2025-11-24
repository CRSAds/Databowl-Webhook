// api/databowl-webhook.js
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';

// === ENV ===
const DIRECTUS_URL = (process.env.DIRECTUS_URL || '').replace(/\/+$/, '');
const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN || '';
const SHARED_SECRET = process.env.DATABOWL_WEBHOOK_SECRET || '';

// Supabase
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
        auth: { persistSession: false },
      })
    : null;

// === Utils ===
const normMoney = (v) =>
  v == null || v === '' ? null : Number.parseFloat(v).toFixed(2);

const asISOFromUnix = (s) => {
  const n = Number(s);
  return Number.isFinite(n)
    ? new Date(n * 1000).toISOString()
    : new Date().toISOString();
};

// Pas aan zodra je nummers definitief zijn in Databowl
const FIELD_IDS = {
  offer_id: '1687',
  affiliate_id: '1684',
  supplier_id_via_data: '1685',
  sub_id: null,
  t_id: '1322', // f_1322_transaction_id → Directus t_id
};

// statusId → label
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

// Idempotency key
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

// Mapping Databowl payload → event object
function mapPayload(body) {
  const b = body || {};

  // Nieuw Databowl formaat: { event:"lead_update", payload:{...} }
  if (b.event === 'lead_update' && b.payload && typeof b.payload === 'object') {
    const p = b.payload;
    const data = p.data || {};

    const lead_id = p.leadId ?? null;
    const campaign_id =
      p.campaignId != null ? String(p.campaignId) : null;
    const supplier_id =
      p.supplierId != null ? String(p.supplierId) : null;

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
    const currency = 'EUR';

    const email = data?.['1'] || null;
    const email_hash = email
      ? crypto
          .createHash('sha256')
          .update(String(email).toLowerCase())
          .digest('hex')
      : null;

    return {
      lead_id,
      status,
      revenue,
      cost,
      currency,
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

  // Oud fallback-formaat
  const msg = b.message || b.lead_message || {};
  const ld = b.lead || b.data || {};
  const fin = b.finance || {};
  const meta = b.meta || b.metadata || {};

  const statusRaw = msg.status ?? b.status ?? ld.status ?? null;
  const status =
    statusRaw != null && String(Number(statusRaw)) === String(statusRaw)
      ? STATUS_MAP[String(statusRaw)] || `Unknown (${String(statusRaw)})`
      : statusRaw || 'unknown';

  const created_at =
    msg.created_at ??
    ld.created_at ??
    b.created_at ??
    new Date().toISOString();

  const revenue = normMoney(fin.revenue ?? b.revenue ?? ld.revenue);
  const cost = normMoney(fin.cost ?? b.cost ?? ld.cost);
  const currency = (fin.currency ?? b.currency ?? 'EUR') || 'EUR';

  const offer_id = ld.offer_id ?? b.offer_id ?? meta.offer_id ?? null;
  const campaign_id =
    ld.campaign_id ?? b.campaign_id ?? meta.campaign_id ?? null;
  const supplier_id =
    ld.supplier_id ?? b.supplier_id ?? meta.supplier_id ?? null;
  const affiliate_id =
    ld.affiliate_id ?? b.affiliate_id ?? meta.affiliate_id ?? null;
  const sub_id =
    ld.sub_id ?? ld.subid ?? b.sub_id ?? meta.sub_id ?? null;
  const t_id =
    (FIELD_IDS.t_id &&
      (ld[FIELD_IDS.t_id] || (b.data && b.data[FIELD_IDS.t_id]))) ??
    ld.t_id ??
    b.t_id ??
    meta.t_id ??
    null;

  const lead_id = ld.id ?? b.lead_id ?? msg.lead_id ?? null;

  const email = ld.email ?? b.email ?? null;
  const email_hash = email
    ? crypto
        .createHash('sha256')
        .update(String(email).toLowerCase())
        .digest('hex')
    : null;

  return {
    lead_id,
    status,
    revenue,
    cost,
    currency,
    offer_id,
    campaign_id,
    supplier_id,
    affiliate_id,
    sub_id,
    t_id,
    created_at,
    raw: { original: b, email_hash },
  };
}

// Directus create (alleen voor coreg leads)
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

  let txt = await r.text();
  try {
    const j = JSON.parse(txt);
    const nonUnique = j?.errors?.some(
      (e) => e?.extensions?.code === 'RECORD_NOT_UNIQUE'
    );
    if (nonUnique) return { skipped: true };
  } catch {}
  throw new Error(`Directus create ${r.status}: ${txt}`);
}

// Supabase insert (shortform + coreg)
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
    day: event.day || event.created_at?.slice(0, 10),
    is_shortform: event.is_shortform || false,
  });

  if (error) throw error;
  return { inserted: true };
}

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    if (req.method !== 'POST')
      return res.status(405).json({ error: 'Method not allowed' });

    const secret = req.query?.secret;
    if (!secret || secret !== SHARED_SECRET)
      return res.status(401).json({ error: 'Invalid secret' });

    const body =
      req.body && Object.keys(req.body).length
        ? req.body
        : await new Promise((resolve) => {
            let d = '';
            req.on('data', (c) => (d += c));
            req.on('end', () => {
              try {
                resolve(JSON.parse(d || '{}'));
              } catch {
                resolve({});
              }
            });
          });

    const event = mapPayload(body);

    if (!event.lead_id)
      return res.status(400).json({ error: 'Missing lead_id' });

    // DAY veld
    event.day = event.created_at.slice(0, 10);

    // Detect shortform (campagne 925)
    if (event.campaign_id === '925') {
      event.is_shortform = true;
      event.cost = 0;
      event.revenue = 0;
    }

    // Skip Directus insert voor shortform
    let directusResult = { skipped: 'shortform' };
    if (!event.is_shortform) {
      directusResult = await createEventDirectus(event);
    }

    // Idempotency key
    const event_key = makeKey(event);
    event.event_key = event_key;

    // Supabase opslag (alles)
    try {
      await insertSupabase(event);
    } catch (e) {
      console.error('[Supabase insert failed]', e);
    }

    return res.status(200).json({
      ok: true,
      directus: directusResult,
      shortform: event.is_shortform || false,
    });
  } catch (e) {
    console.error('[databowl-webhook error]', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
