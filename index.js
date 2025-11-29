// index.js
import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import crypto from "crypto";

const app = express();

// Capture the raw body for HMAC verification
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf.toString("utf8");
    },
  })
);

const bigquery = new BigQuery();

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.PROJECT_ID;
const DATASET_ID = "retail_mvp";
const ORDERS_TABLE_ID = "events_orders";
const PRODUCTS_TABLE_ID = "events_v4_product_metrics_table";

const SHOPIFY_WEBHOOK_SECRET = process.env.SHOPIFY_WEBHOOK_SECRET;

// ---- Shopify HMAC verification ----
function verifyShopifyHmac(req) {
  if (!SHOPIFY_WEBHOOK_SECRET) {
    console.warn(
      "SHOPIFY_WEBHOOK_SECRET is not set. Skipping HMAC validation (DEV ONLY)."
    );
    return true; // for safety, you can change this to false once secret is set
  }

  const hmacHeader = req.get("X-Shopify-Hmac-Sha256") || "";
  const rawBody = req.rawBody || "";

  const generatedHash = crypto
    .createHmac("sha256", SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");

  const hashBuffer = Buffer.from(generatedHash, "utf8");
  const headerBuffer = Buffer.from(hmacHeader, "utf8");

  if (hashBuffer.length !== headerBuffer.length) {
    return false;
  }

  try {
    return crypto.timingSafeEqual(hashBuffer, headerBuffer);
  } catch (e) {
    console.error("Error comparing HMAC:", e);
    return false;
  }
}

// ---- Helper: BigQuery insert with simple retry ----
async function insertWithRetry(table, rows, label) {
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await table.insert(rows);
      console.log(
        `Inserted ${rows.length} row(s) into ${DATASET_ID}.${table.id} (${label})`
      );
      return;
    } catch (err) {
      console.error(
        `Attempt ${attempt} failed inserting into ${DATASET_ID}.${table.id} (${label}):`,
        err
      );
      if (attempt === maxAttempts) {
        throw err;
      }
    }
  }
}

// ---- Webhook endpoint ----
app.post("/", async (req, res) => {
  const start = Date.now();

  try {
    // 1) HMAC verification
    if (!verifyShopifyHmac(req)) {
      console.warn("Invalid Shopify HMAC. Rejecting webhook.");
      return res.status(401).send("Invalid signature");
    }

    const order = req.body;

    if (!order || !order.id || !Array.isArray(order.line_items)) {
      console.warn("Invalid payload:", order);
      return res.status(400).send("Invalid payload");
    }

    const orderId = order.id.toString();
    console.log(
      `Received order ${orderId} with ${order.line_items.length} line items`
    );

    const dataset = bigquery.dataset(DATASET_ID);
    const ordersTable = dataset.table(ORDERS_TABLE_ID);
    const productsTable = dataset.table(PRODUCTS_TABLE_ID);

    // 2) Idempotency: has this order already been processed?
    const [checkRows] = await bigquery.query({
      query: `
        SELECT 1
        FROM \`${PROJECT_ID}.${DATASET_ID}.${ORDERS_TABLE_ID}\`
        WHERE order_id = @orderId
        LIMIT 1
      `,
      params: { orderId },
    });

    if (checkRows.length > 0) {
      console.log(
        `Order ${orderId} already processed earlier. Skipping inserts and returning 200.`
      );
      return res.status(200).send("Already processed");
    }

    // 3) Prepare common fields
    const occurred_at = order.created_at || new Date().toISOString();
    const shop_id = order?.source_name || "SHOPIFY";
    const currency = order.currency || "USD";
    const source = "shopify_webhook";

    // 4) Prepare order-level row
    const orderRow = {
      order_id: orderId,
      occurred_at,
      shop_id,
      total_price: parseFloat(order.total_price || 0),
      total_discount: parseFloat(order.total_discounts || 0),
      currency,
      source,
    };

    // 5) Prepare product-level rows
    const productRows = order.line_items.map((item) => ({
      order_id: orderId,
      occurred_at,
      shop_id,
      product_id: item.product_id ? item.product_id.toString() : null,
      product_title: item.title || null,
      variant_id: item.variant_id ? item.variant_id.toString() : null,
      variant_title: item.variant_title || null,
      quantity: item.quantity || 0,
      price: parseFloat(item.price || 0),
      total_discount: parseFloat(item.total_discount || 0),
      vendor: item.vendor || null,
      currency,
      source,
    }));

    // 6) Write to BigQuery with retries
    await insertWithRetry(ordersTable, [orderRow], "orders");
    await insertWithRetry(productsTable, productRows, "product_metrics");

    const durationMs = Date.now() - start;
    console.log(
      `Order ${orderId} stored successfully: 1 order row, ${productRows.length} product rows in ${durationMs} ms`
    );

    return res.status(200).send("OK");
  } catch (err) {
    console.error("Unhandled error while processing webhook:", err);
    // Returning 500 lets Shopify retry later; idempotency will protect us.
    return res.status(500).send("Internal error");
  }
});

// ---- Start server ----
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`shopify-webhook-handler listening on port ${PORT}`);
});
