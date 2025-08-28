// api/databowl-webhook.js
import crypto from 'crypto';

// === ENV ===
const DIRECTUS_URL_RAW = process.env.DIRECTUS_URL || '';
const DIRECTUS_TOKEN   = process.env.DIRECTUS_TOKEN || '';
const SHARED_SECRET    = process.env.DATABOWL_WEBHOOK_SECRET || '';

const DIRECTUS_URL = DIRECTUS_URL_RAW.replace(/\/+$/, '');

// (Optioneel) veld-ID mapping vanuit Databowl "data" map (keyed met nummers als strings)
// Pas aan op jullie account zodra definitief:
const FIELD_IDS = {
  offer_id:     '1687', // ← in jouw voorbeeld: "1687":"878" (waarschijnlijk offer)
  affiliate_id: '1684', // ← "1684":"78" (vaak affiliate/publisher-subcode)
  supplier_id_via_data: '1685', // ← "1685":"34" (komt ook los als supplierId)
  sub_id:       null,
  t_id:         null,
};

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

// === Helpers ===
const normMoney = (v) => (v == null || v === '' ? null : Number.parseFloat(v).toFixed(2)); // "12.34"
const asISOFromUnix = (s) => {
  const n = Number(s);
  return Number.isFinite(n) ? new Date(n * 1000).toISOString() : new Date().toISOString();
};

// === Mapping Databowl payload → Directus item ===
function mapPayload(body) {
  const b = body || {};

  // 1) NIEUW FORMAAT: { event: "lead_update", payload: {...} }
  if (b.event === 'lead_update' && b.payload && typeof b.payload === 'object') {
    const p = b.payload;
    const data = p.data || {}; // genummerde velden als strings

    // Identifiers (zeker)
    const lead_id     = p.leadId ?? null;
    const campaign_id = p.campaignId != null ? String(p.campaignId) : null;
    const supplier_id = p.supplierId != null ? String(p.supplierId) : null;

    // Mogelijk uit "data" mappen (pas FIELD_IDS aan zodra bevestigd)
    const offer_id     = FIELD_IDS.offer_id ? data[FIELD_IDS.offer_id] ?? null : null;
    const affiliate_id = FIELD_IDS.affiliate_id ? data[FIELD_IDS.affiliate_id] ?? null : null;
    const sub_id       = FIELD_IDS.sub_id ? data[FIELD_IDS.sub_id] ?? null : null;
    const t_id         = FIELD_IDS.t_id ? data[FIELD_IDS.t_id] ?? null : null;

    // Status: we hebben hier een numerieke statusId; zet die in status als string
    const status = p.statusId != null ? String(p.statusId) : 'unknown';

    // Timestamps / finance
    const created_at = p.receivedAt ? asISOFromUnix(p.receivedAt) : new Date().toISOString();
    const revenue    = normMoney(p.normalRevenue ?? p.revenue); // veldnamen kunnen per include verschillen
    const cost       = normMoney(p.normalCost ?? p.cost);
    const currency   = 'EUR'; // niet meegegeven in deze payload → default

    // E-mail zit (meestal) in data["1"]; hash voor raw
    const email = data?.['1'] || null;
    const email_hash = email
      ? crypto.createHash('sha256').update(String(email).toLowerCase()).digest('hex')
      : null;

    return {
      lead_id,
      status,      // bv "7" (accepted) — echte label kun je later mappen in Insights als je wil
      revenue,     // "0.00" etc.
      cost,
      currency,
      offer_id,
      campaign_id,
      supplier_id,
      affiliate_id,
      sub_id,
      t_id,
      created_at,
      raw: {
        original: b,
        email_hash,
        status_id: p.statusId ?? null, // extra context
      },
    };
  }

  // 2) OUD FORMAAT (universele fallback): { lead:{...}, message:{...}, finance:{...}, ... }
  const msg = b.message || b.lead_message || {};
  const ld  = b.lead || b.data || {};
  const fin = b.finance || {};
  const meta = b.meta || b.metadata || {};

  const status     = msg.status ?? b.status ?? ld.status ?? 'unknown';
  const created_at = msg.created_at ?? ld.created_at ?? b.created_at ?? new Date().toISOString();

  const revenue  = normMoney(fin.revenue ?? b.revenue ?? ld.revenue);
  const cost     = normMoney(fin.cost ?? b.cost ?? ld.cost);
  const currency = (fin.currency ?? b.currency ?? 'EUR') || 'EUR';

  const offer_id2     = ld.offer_id     ?? b.offer_id     ?? meta.offer_id     ?? null;
  const campaign_id2  = ld.campaign_id  ?? b.campaign_id  ?? meta.campaign_id  ?? null;
  const supplier_id2  = ld.supplier_id  ?? b.supplier_id  ?? meta.supplier_id  ?? null;
  const affiliate_id2 = ld.affiliate_id ?? b.affiliate_id ?? meta.affiliate_id ?? null;
  const sub_id2       = ld.sub_id ?? ld.subid ?? b.sub_id ?? meta.sub_id ?? null;
  const t_id2         = ld.t_id   ?? b.t_id   ?? meta.t_id   ?? null;

  const lead_id2 = ld.id ?? b.lead_id ?? msg.lead_id ?? null;

  const email2 = ld.email ?? b.email ?? null;
  const email_hash2 = email2
    ? crypto.createHash('sha256').update(String(email2).toLowerCase()).digest('hex')
    : null;

  return {
    lead_id: lead_id2,
    status,
    revenue,
    cost,
    currency,
    offer_id: offer_id2,
    campaign_id: campaign_id2,
    supplier_id: supplier_id2,
    affiliate_id: affiliate_id2,
    sub_id: sub_id2,
    t_id: t_id2,
    created_at,
    raw: { original: b, email_hash: email_hash2 },
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
    if (!event.lead_id) {
      // Extra context meegeven voor debuggen
      return res.status(400).json({ error: 'Missing lead_id in payload', hint: Object.keys(body) });
    }

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
