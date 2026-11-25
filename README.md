# منظومة إدارة المخلفات الخطرة / Hazardous Waste Management System
Run: `node server.js` (Node 22+, no npm install). Open http://localhost:3000
Env: PORT, ADMIN_PASSWORD, NO_DEMO=1 (no demo data), HTTPS=1 (Secure cookie behind HTTPS), DB_FILE.
Demo logins: admin/admin123 (manager), khaled/emp123 (employee), noura/mkt123 (marketer). Change them first.
Hosting: any VPS (Ubuntu) + Node 22 + pm2/systemd, behind Caddy or nginx with HTTPS. Back up data.db daily.
Prototype limits: SQLite single file, no email/SMS, no PDF contract/quote generation, no file uploads, no Arabic-hijri dates, basic audit log (stored, no viewer).

## Persistent free hosting (Render + Turso)
Set env on Render: TURSO_URL (libsql://...), TURSO_TOKEN, NO_DEMO=1, ADMIN_PASSWORD (12+ chars), HTTPS=1. Start command: node server.js. Without TURSO_URL it uses local SQLite (data lost on Render free restarts).
NO_DEMO=1 creates only the admin account (no demo users).
