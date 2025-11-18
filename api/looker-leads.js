export default async function handler(req, res) {
  try {
    const DIRECTUS_URL = process.env.DIRECTUS_URL;
    const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

    const { from, to, offer, affiliate, campaign } = req.query;

    const filters = [];
    if (from && to) filters.push(`filter[day][_between]=${from},${to}`);
    if (offer) filters.push(`filter[offer_id][_eq]=${offer}`);
    if (affiliate) filters.push(`filter[affiliate_id][_eq]=${affiliate}`);
    if (campaign) filters.push(`filter[campaign_id][_eq]=${campaign}`);

    const qs = filters.length ? `&${filters.join("&")}` : "";

    // 1. Fetch lead omzet
    const leadRes = await fetch(
      `${DIRECTUS_URL}/items/Lead_omzet?limit=-1${qs}`,
      { headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` } }
    );
    const leads = (await leadRes.json()).data || [];

    // 2. Fetch sponsor sources
    const [coRes, ccRes, caRes] = await Promise.all([
      fetch(`${DIRECTUS_URL}/items/co_sponsors?limit=-1`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
      fetch(`${DIRECTUS_URL}/items/coreg_campaigns?limit=-1`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
      fetch(`${DIRECTUS_URL}/items/coreg_answers?limit=-1`, {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }),
    ]);

    const co = (await coRes.json()).data || [];
    const cc = (await ccRes.json()).data || [];
    const ca = (await caRes.json()).data || [];

    // 3. Build lookup maps
    const coMap = Object.fromEntries(co.map(s => [String(s.cid), s.title]));
    const ccMap = Object.fromEntries(cc.map(s => [String(s.cid), s.sponsor]));
    const caMap = Object.fromEntries(ca.map(s => [String(s.cid), s.label]));

    // 4. Merge data
    const rows = leads.map(item => {
      const cid = String(item.campaign_id || "");

      let sponsor_name =
        ccMap[cid] ||      // coreg campaigns
        caMap[cid] ||      // coreg answers
        coMap[cid] ||      // co sponsors
        "";

      return {
        date: item.day || "",
        offer_id: item.offer_id || "",
        affiliate_id: item.affiliate_id || "",
        campaign_id: cid,
        sponsor_name,
        cost: Number(item.cost) || 0,
        revenue: Number(item.revenue) || 0,
        leads: 1,
        t_id: item.t_id || ""
      };
    });

    res.status(200).json(rows);

  } catch (err) {
    console.error("[looker-leads] error:", err);
    res.status(500).json({ error: String(err) });
  }
}
