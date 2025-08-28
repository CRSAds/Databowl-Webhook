// api/databowl-webhook.js
import crypto from 'crypto';

// === ENV ===
const DIRECTUS_URL_RAW = process.env.DIRECTUS_URL || '';          // bv. https://cms.core.909play.com
const DIRECTUS_TOKEN   = process.env.DIRECTUS_TOKEN || '';        // static token van "Databowl API Role"
const SHARED_SECRET    = process.env.DATABOWL_WEBHOOK_SECRET || '';// eigen geheim, meegeven als ?secret=...

// Normaliseer URL (zonder trailing slash)
const DIRECTUS_URL = DIRECTUS_URL_RAW.replace(/\/+$/, '');

// === Directus helpers ===
async function findExistingByKey(key) {
  const url = `${DIRECTUS_URL}/items/Databowl_lead_events?filter[raw][key][_eq]=${encodeURIComponent(key)}&limit=1`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } });
  if (!r.ok) throw new Error(`Directus find ${r.status}: ${await r.text()}`);
  const j = await r.json();
  return j?.data?.[0] || null;
}

async function createEvent(event) {
  const r = await fetch(`${DIRECTUS_URL}/items/Databowl_lead_events`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${DIRECTUS_TOKEN}`,
    },
    body: JSON.stringify(event),
  });
  if (!r.ok) throw new Error(`Directus create ${r.status}: ${await r.text()}`);
  return r.json();
}

// === Idempotency key ===
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

// === Mapping Databowl payload → Directus item ===
function mapPayload(body) {
  const b   = body || {};
  const msg = b.message || b.lead_message || {};
  const ld  = b.lead || b.data || {};
  const fin = b.finance || {};
  const meta = b.meta || b.metadata || {};

  const status     = msg.status ?? b.status ?? ld.status ?? 'unknown';
  const created_at = msg.created_at ?? ld.created_at ?? b.created_at ?? new Date().toISOString();

  const normMoney = (v) => (v == null || v === '' ? null : Number.parseFloat(v).toFixed(2)); // "12.34"

  const revenue  = normMoney(fin.revenue ?? b.revenue ?? ld.revenue);
  const cost     = normMoney(fin.cost ?? b.cost ?? ld.cost);
  const currency = (fin.currency ?? b.currency ?? 'EUR') || 'EUR';

  const offer_id     = ld.offer_id     ?? b.offer_id     ?? meta.offer_id     ?? null;
  const campaign_id  = ld.campaign_id  ?? b.campaign_id  ?? meta.campaign_id  ?? null;
  const supplier_id  = ld.supplier_id  ?? b.supplier_id  ?? meta.supplier_id  ?? null;
  const affiliate_id = ld.affiliate_id ?? b.affiliate_id ?? meta.affiliate_id ?? null;
  const sub_id       = ld.sub_id ?? ld.subid ?? b.sub_id ?? meta.sub_id ?? null;
  const t_id         = ld.t_id   ?? b.t_id   ?? meta.t_id   ?? null;

  const lead_id = ld.id ?? b.lead_id ?? msg.lead_id ?? null;

  // PII-zuinig: hash e-mail alleen in raw
  const email = ld.email ?? b.email ?? null;
  const email_hash = email
    ? crypto.createHash('sha256').update(String(email).toLowerCase()).digest('hex')
    : null;

  return {
    lead_id,
    status,
    revenue,   // "12.34" → past in decimal(10,2)
    cost,      // "1.99"
    currency,
    offer_id,
    campaign_id,
    supplier_id,
    affiliate_id,
    sub_id,
    t_id,
    created_at,
    raw: { original: b, email_hash }, // key wordt zo toegevoegd
  };
}

// === Handler ===
export default async function handler(req, res) {
  // CORS (alleen nodig voor browser-tests; Databowl zelf is server→server)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

    // Secret check (query ?secret=...)
    const secret = req.query?.secret;
    if (!secret || secret !== SHARED_SECRET) {
      return res.status(401).json({ error: 'Invalid secret' });
    }

    // Body parse (JSON of raw)
    const body =
      req.body && Object.keys(req.body).length
        ? req.body
        : await new Promise((resolve) => {
            let d = '';
            req.on('data', (c) => (d += c));
            req.on('end', () => {
              try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); }
            });
          });

    const event = mapPayload(body);
    if (!event.lead_id) return res.status(400).json({ error: 'Missing lead_id in payload' });

    // Idempotency
    const key = makeKey(event);
    event.raw.key = key;

    const existing = await findExistingByKey(key);
    if (existing) return res.status(200).json({ ok: true, skipped: true, id: existing.id });

    const created = await createEvent(event);
    return res.status(200).json({ ok: true, created });
  } catch (e) {
    console.error('[databowl-webhook] error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
