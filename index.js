import express from "express";
import bodyParser from "body-parser";
import { BigQuery } from "@google-cloud/bigquery";

const app = express();

// Accept raw JSON from Shopify
app.use(bodyParser.json({ type: "*/*" }));

// BigQuery client
const bigquery = new BigQuery();
const DATASET_ID = "retail_mvp";

// Order-level table
const ORDERS_TABLE_ID = "events_orders";

// Product-level table (already created in your project)
const PRODUCTS_TABLE_ID = "events_v4_product_metrics_table";

// Utility: safe numeric string for BigQuery NUMERIC
function toNumericString(value) {
  if (value === null || value === undefined || value === "") return "0";
  // Shopify often sends numeric fields as strings
  return String(value);
}

// Insert rows helper
async function insertRows(tableId, rows) {
  if (!rows || rows.length === 0) return;

  const table = bigquery.dataset(DATASET_ID).table(tableId);

  await table.insert(rows);
  console.log(
    `Inserted ${rows.length} row(s) into ${DATASET_ID}.${tableId}`
  );
}

// Basic health check
app.get("/", (req, res) => {
  res.status(200).send("shopify-webhook-handler is running");
});

// Shopify orders/create webhook
app.post("/", async (req, res) => {
  try {
    const order = req.body;

    if (!order || !order.id || !Array.isArray(order.line_items)) {
      console.error("Invalid payload", JSON.stringify(order).slice(0, 500));
      return res.status(400).send("Invalid payload");
    }

    console.log(`Received order ${order.id} with ${order.line_items.length} items`);

    // -------------------------
    // 1) Build order-level row
    // -------------------------
    const occurredAt = order.created_at
      ? new Date(order.created_at)
      : new Date();

    // You can adjust how shop_id is derived; for now, use shop's domain fallback:
    const shopId =
      order.location_id?.toString() ||
      order.source_name ||
      "DEMO_SHOP";

    const orderRow = {
      order_id: order.id.toString(),
      occurred_at: occurredAt,
      shop_id: shopId,
      total_price: toNumericString(order.total_price),
      subtotal_price: toNumericString(order.subtotal_price),
      total_tax: toNumericString(order.total_tax),
      total_discounts: toNumericString(order.total_discounts),
      currency: order.currency || "USD",
      source: "shopify_webhook"
    };

    // --------------------------
    // 2) Build product-level rows
    // --------------------------
    const productRows = order.line_items.map((item) => ({
      order_id: order.id.toString(),
      occurred_at: occurredAt,
      shop_id: shopId,
      product_id: item.product_id ? item.product_id.toString() : null,
      product_title: item.title || null,
      variant_id: item.variant_id ? item.variant_id.toString() : null,
      variant_title: item.variant_title || null,
      quantity: item.quantity || 0,
      price: toNumericString(item.price),
      total_discount: toNumericString(item.total_discount),
      vendor: item.vendor || null,
      currency: order.currency || "USD",
      source: "shopify_webhook"
    }));

    // --------------------------
    // 3) Insert into BigQuery
    // --------------------------
    await insertRows(ORDERS_TABLE_ID, [orderRow]);
    await insertRows(PRODUCTS_TABLE_ID, productRows);

    console.log(
      `Order ${order.id} stored: 1 order row, ${productRows.length} product rows`
    );

    res.status(200).send("OK");
  } catch (err) {
    console.error("Error handling Shopify webhook", err);
    res.status(500).send("Internal server error");
  }
});

// Start server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`shopify-webhook-handler listening on port ${PORT}`);
});
