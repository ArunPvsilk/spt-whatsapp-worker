# SPT Distribution — Secure WhatsApp Firestore Queue Worker

Zero-cost, secure WhatsApp messaging worker for SPT Distribution ERP.
Runs on any office PC, laptop, or server without exposing open ports, public IP, or paying for cloud servers.

---

## 🎯 How It Works

1. **Flutter Mobile App (Field / Office):** When an order, POD delivery receipt, payment receipt, customer ledger statement, or alert is created, the app creates a document in Cloud Firestore (`whatsapp_outbox` collection).
2. **Office PC Worker:** This script listens via Firestore `onSnapshot` (outbound connection only). When a pending message arrives, it claims it atomically via Firestore transaction, sends the message over WhatsApp using Baileys WebSockets, and marks the document as `sent`.

---

## 🚀 Quick Setup (One-time, 5 minutes)

### Step 1: Install Dependencies
Open terminal / command prompt in this directory:
```bash
cd server/whatsapp-worker
npm install
```

### Step 2: Download Firebase Service Account Key
1. Go to [Firebase Console](https://console.firebase.google.com).
2. Select your project (**spt-distribution**).
3. Click **Project Settings (⚙️ icon)** > **Service Accounts** tab.
4. Click **Generate new private key**.
5. Save the downloaded JSON file as `service-account-key.json` in this folder (`server/whatsapp-worker/service-account-key.json`).

> 🔒 **Security Notice:** `service-account-key.json` and `auth_info/` are already added to `.gitignore`. Never commit them to Git.

### Step 3: Run the Worker
```bash
node worker.js
```
- A QR code will appear in your terminal.
- Open **WhatsApp** on the company phone > **Linked Devices** > **Link a Device** > scan the QR code.
- Once connected, you will see:
  ```
  🎉 [WhatsApp] Connected successfully!
  🚀 [Worker] Outbox queue listener is active and processing...
  ```

---

## 🔄 Running Permanently in Background (PM2)

To keep the worker running automatically and restart on PC reboot:

```bash
# Install PM2 globally (one-time)
npm install -g pm2

# Start worker with PM2
pm2 start worker.js --name spt-whatsapp-worker

# Save PM2 process list so it starts on boot
pm2 save
pm2 startup
```

To monitor logs anytime:
```bash
pm2 logs spt-whatsapp-worker
```
