export default async function handler(req, res) {
  try {
    const DIRECTUS_URL = process.env.DIRECTUS_URL;
    const DIRECTUS_TOKEN = process.env.DIRECTUS_TOKEN;

    const { from, to, offer, affiliate, campaign } = req.query;

    // Build Directus filter query
    const filters = [];

    if (from && to) filters.push(`filter[day][_between]=${from},${to}`);
    if (offer) filters.push(`filter[offer_id][_eq]=${offer}`);
    if (affiliate) filters.push(`filter[affiliate_id][_eq]=${affiliate}`);
    if (campaign) filters.push(`filter[campaign_id][_eq]=${campaign}`);

    const qs = filters.length ? `&${filters.join("&")}` : "";

    // Fetch Lead_omzet from Directus
    const r = await fetch(
      `${DIRECTUS_URL}/items/Lead_omzet?limit=-1${qs}`,
      {
        headers: { Authorization: `Bearer ${DIRECTUS_TOKEN}` },
      }
    );

    const json = await r.json();

    // Flatten rows for Looker Studio
    const rows = (json.data || []).map((item) => ({
      date: item.day,
      offer_id: item.offer_id,
      affiliate_id: item.affiliate_id,
      campaign_id: item.campaign_id,
      cost: parseFloat(item.cost || 0),
      revenue: parseFloat(item.revenue || 0),
      leads: 1,
      t_id: item.t_id
    }));

    return res.status(200).json(rows);

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err) });
  }
}
