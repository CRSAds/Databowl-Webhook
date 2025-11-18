// api/looker-leads.js

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// simpele CSV-encoder
function toCSV(rows, columns) {
  const escape = (value) => {
    if (value == null) return "";
    const s = String(value);
    if (/[",\n]/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };

  const header = columns.join(",");
  const lines = rows.map((row) =>
    columns.map((col) => escape(row[col])).join(",")
  );
  return [header, ...lines].join("\n");
}

export default async function handler(req, res) {
  try {
    const DIRECTUS_URL = process.env.DIRECTUS_URL;
    const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

    if (!DIRECTUS_URL || !DIRECTUS_TOKEN) {
      return res
        .status(500)
        .json({ error: "Missing DIRECTUS_URL or DIRECTUS_TOKEN env vars" });
    }

    const { from, to, offer, affiliate, campaign, format } = req.query;

    // -------------------------------------
    // 1. Filters opbouwen
    // -------------------------------------
    const filters = [];

    if (from && to) filters.push(`filter[day][_between]=${from},${to}`);
    if (offer) filters.push(`filter[offer_id][_eq]=${offer}`);
    if (affiliate) filters.push(`filter[affiliate_id][_eq]=${affiliate}`);
    if (campaign) filters.push(`filter[campaign_id][_eq]=${campaign}`);

    const qs = filters.length ? `&${filters.join("&")}` : "";

    const leadUrl = `${DIRECTUS_URL}/items/Lead_omzet?limit=-1&fields=lead_id,status,revenue,cost,currency,offer_id,campaign_id,supplier_id,affiliate_id,sub_id,t_id,created_at,day${qs}`;

    // -------------------------------------
    // 2. Directus API calls (correct fields)
    // -------------------------------------
    const [leadRes, coRes, ccRes, caRes] = await Promise.all([
      fetch(leadUrl, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),

      // co_sponsors → cid + title
      fetch(`${DIRECTUS_URL}/items/co_sponsors?limit=-1&fields=cid,title`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),

      // coreg_campaigns → cid + Sponsor (MET hoofdletter S)
      fetch(
        `${DIRECTUS_URL}/items/coreg_campaigns?limit=-1&fields=cid,Sponsor`,
        { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } }
      ),

      // coreg_answers → cid + Label (MET hoofdletter L)
      fetch(
        `${DIRECTUS_URL}/items/coreg_answers?limit=-1&fields=cid,Label`,
        { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } }
      ),
    ]);

    if (!leadRes.ok) {
      const text = await leadRes.text();
      throw new Error(`Directus Lead_omzet error ${leadRes.status}: ${text}`);
    }

    const [leadJson, coJson, ccJson, caJson] = await Promise.all([
      leadRes.json(),
      coRes.ok ? coRes.json() : { data: [] },
      ccRes.ok ? ccRes.json() : { data: [] },
      caRes.ok ? caRes.json() : { data: [] },
    ]);

    const leads = leadJson.data || [];
    const co = coJson.data || [];
    const cc = ccJson.data || [];
    const ca = caJson.data || [];

    // -------------------------------------
    // 3. Maps bouwen voor snelle lookup
    // -------------------------------------
    const coMap = Object.fromEntries(
      co
        .filter((s) => s && s.cid != null)
        .map((s) => [String(s.cid), s.title?.trim() || ""])
    );

    const ccMap = Object.fromEntries(
      cc
        .filter((s) => s && s.cid != null)
        .map((s) => [String(s.cid), s.Sponsor?.trim() || ""])
    );

    const caMap = Object.fromEntries(
      ca
        .filter((s) => s && s.cid != null)
        .map((s) => [String(s.cid), s.Label?.trim() || ""])
    );

    // -------------------------------------
    // 4. Flatten rows + sponsornaam bepalen
    // -------------------------------------
    const rows = leads.map((item) => {
      const cid = item.campaign_id != null ? String(item.campaign_id) : "";
      const created = item.day || (item.created_at || "").slice(0, 10);

      const sponsor_name =
        ccMap[cid] || // beste optie
        caMap[cid] || // tweede optie
        coMap[cid] || // fallback
        "";

      return {
        date: created || "",
        campaign_id: cid,
        offer_id: item.offer_id || "",
        affiliate_id: item.affiliate_id || "",
        supplier_id: item.supplier_id || "",
        sub_id: item.sub_id || "",
        sponsor_name,
        cost: toNumber(item.cost),
        revenue: toNumber(item.revenue),
        currency: item.currency || "EUR",
        leads: 1,
        t_id: item.t_id || "",
        status: item.status || "",
        lead_id: item.lead_id || "",
      };
    });

    // -------------------------------------
    // 5. JSON or CSV output
    // -------------------------------------
    const isCsv = String(format || "").toLowerCase() === "csv";

    if (!isCsv) {
      return res.status(200).json(rows);
    }

    const columns = [
      "date",
      "campaign_id",
      "offer_id",
      "affiliate_id",
      "supplier_id",
      "sub_id",
      "sponsor_name",
      "cost",
      "revenue",
      "currency",
      "leads",
      "t_id",
      "status",
      "lead_id",
    ];

    const csv = toCSV(rows, columns);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      'inline; filename="looker-leads.csv"'
    );
    return res.status(200).send(csv);
  } catch (err) {
    console.error("[looker-leads] error:", err);
    if (!res.headersSent) {
      res.status(500).json({
        error: String(err?.message || err),
      });
    }
  }
}
