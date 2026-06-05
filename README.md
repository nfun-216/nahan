# Project Nahan (پروژه نهان)

**Nahan** (Persian for *Hidden/Concealed*) is a secure, lightweight, and customizable network gateway designed to run entirely on Cloudflare Workers. It acts as an obfuscated reverse proxy (VLESS / Trojan over WebSocket) configured via an intuitive, embedded web dashboard.

By disguising its web interface as an "IoT Telemetry Hub," unauthorized visitors are seamlessly redirected to maintenance hosts (like ubuntu.com), ensuring your gateway remains completely hidden and protected from active probing.

## 🚀 Features

*   **Full Embedded Dashboard:** Change configurations, view QR codes, and copy secure links without touching code.
*   **Dual Protocol Support:** Run in `Alpha mode` (VLESS) or `Beta mode` (Trojan).
*   **Dynamic KV Storage:** Settings are saved to a Cloudflare KV namespace, surviving worker updates.
*   **Stealth Mode:** Unauthorized network requests or standard browser visits are seamlessly proxied to dummy websites (`ubuntu.com`, `docker.com`).
*   **Multi-Language UI:** Built-in English and Persian (Farsi) support with Dark/Light mode.

## 🛠️ Deployment Instructions

### 1. Create a KV Namespace
1. Go to your Cloudflare Dashboard.
2. Navigate to **Workers & Pages** -> **KV**.
3. Create a new namespace and name it `IOT_DB`.

### 2. Deploy the Worker
1. Go to **Workers & Pages** -> **Overview** -> **Create Application** -> **Create Worker**.
2. Name your worker (e.g., `nahan-gateway`) and deploy it.
3. Click **Edit code** and paste the provided `index.js` (or `_worker.js`) script.
4. Click **Deploy**.

### 3. Bind the KV Namespace
1. In your Worker's settings page, go to **Settings** -> **Variables**.
2. Under **KV Namespace Bindings**, add a new binding:
   * **Variable name:** `IOT_DB`
   * **KV namespace:** Select the `IOT_DB` namespace you created in step 1.
3. Save and deploy again.

## ⚙️ Configuration

Once deployed, access the dashboard to configure your node. See the [HELP.md](HELP.md) guide for a full walkthrough.
