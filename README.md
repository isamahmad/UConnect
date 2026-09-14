# UConnect on Vercel

This folder is UConnect set up for [Vercel](https://vercel.com). Anyone with the link, the access code and their own PIN can sign in, view and edit. No Claude account is needed.

It is made of three parts:

- **Website:** the `public` folder, served by Vercel.
- **Server:** one secure function in `api/index.js`. It handles sign-in, data, documents and AI.
- **Database:** a [Neon](https://vercel.com/marketplace/neon) Postgres database, added to Vercel in one click. It stores everything, including uploaded documents.

## Before you start: the Vercel plan

Vercel's free Hobby plan is for [personal, non-commercial use only](https://vercel.com/docs/plans/hobby). UConnect is used for business, so put it on the **Pro plan**. This costs $20 a month per developer seat, and only the person managing the deployment needs a seat. People signing into UConnect don't need Vercel accounts. You can [trial Pro first](https://vercel.com/docs/plans/hobby#upgrading-to-pro).

This is a licensing question, not a technical one. UConnect deploys and runs on Hobby too: it uses a single function region and a 120 second AI timeout, and Hobby now allows [one region](https://vercel.com/docs/functions/configuring-functions/region#limits) and [up to 300 seconds](https://vercel.com/docs/functions/configuring-functions/duration#duration-limits). Use Hobby to try it, then move to Pro before it holds real contact data.

| Item | Why | Cost |
|---|---|---|
| [GitHub](https://github.com) account | Holds the code so Vercel can deploy it | Free |
| [Vercel](https://vercel.com) Pro | Hosts the site and server function | $20 a month per seat |
| [Neon Postgres](https://vercel.com/marketplace/neon) | The database, added from inside Vercel | Free plan is enough to start ([pricing](https://neon.com/pricing)) |
| [Claude API key](https://platform.claude.com/settings/keys) (optional) | Turns on AI matching, deck reading and drafts | Pay per use |

## Put it online (about 15 minutes)

1. **Upload the code to GitHub.**
   1. Create a new **private** repository called `uconnect`.
   2. Choose "uploading an existing file".
   3. Drag in everything from this folder (the `api`, `public` and `seed` folders, plus `package.json` and `vercel.json`), then commit.
2. **Import it into Vercel.**
   1. In Vercel, choose **Add New → Project** and import the `uconnect` repository.
   2. Leave the framework as **Other** and don't change the build settings, because `vercel.json` handles them.
   3. Press **Deploy**. The site shows "No database is connected" until the next step.
3. **Add the database.**
   1. Open the project's **Storage** tab and create a **Neon** Postgres database.
   2. Choose a **London or Europe** region if offered.
   3. Connect it to the project. This adds the `DATABASE_URL` setting for you.
4. **Add the settings.** Under **Settings → Environment Variables**, add:
   - `ACCESS_CODE` = `BusyKids26`
   - `ANTHROPIC_API_KEY` = your key (optional)
5. **Redeploy.** Open **Deployments**, choose the latest one, then **⋯ → Redeploy**, so the new settings take effect.
6. **First sign-in.**
   1. Open your `https://uconnect-….vercel.app` link and enter the access code.
   2. Create the two super admin accounts. Each person chooses their own PIN.
   3. The database tables and example records are created automatically on first visit.
7. **Tidy up.**
   - Delete the `ACCESS_CODE` variable. Only a scrambled hash of it is stored in the database, so it isn't needed after first start.
   - From then on, change the code inside UConnect under **Security → Access code & policies**.
   - Optionally add your own domain under **Settings → Domains**, for example `uconnect.yourfirm.com`.

Leave Vercel's **Deployment Protection** at its default. That setting only protects preview links, not your main site.

## If the site loads but won't let you in

Open `https://your-site.vercel.app/api/healthz` in a browser. It answers in plain words and tells you exactly what is missing.

| What it says | What it means | What to do |
|---|---|---|
| `"database": "missing"` | Step 3 wasn't done, or was done but the project wasn't redeployed afterwards | Storage tab → create or connect a Neon database → Deployments → ⋯ → Redeploy |
| `"database": "error"` | The database is connected but not answering | Check it isn't paused in the Storage tab, then redeploy |
| `"initialised": false` | No access code has been set | Add `ACCESS_CODE` under Settings → Environment Variables, then redeploy |
| `"ai": false` | AI features are off | Optional. Add `ANTHROPIC_API_KEY`, then redeploy |
| `"ok": true` and `"initialised": true` | Everything is working | Reload the site and enter the access code |

Two things catch people out:

- **Adding a database or a variable does nothing until you redeploy.** Vercel only picks up settings when it builds.
- **Connect the database to Production**, not just Preview. The Storage tab shows which environments it is attached to.

## Move your data across from the Claude version

1. In the Claude version, go to **Security → Data & backups → Download full backup**.
2. In the new site, go to **Security → Data & backups → Restore from backup** and choose that file.
3. Vault documents aren't inside the backup, so upload them again.
4. User accounts aren't copied. Add each person under **Security → Users**.

To start without example records, add `SEED_EXAMPLES` = `false` before the first visit. You can also press **Remove example data** on the banner at any time.

## How the Vercel version behaves

- **Live updates:** each open browser checks for changes every 3 seconds. Edits, deal moves and who's online appear for everyone within a few seconds.
- **Documents:** stored privately in the database in pieces, up to 25 MB each, and opened only by signed-in users. Large vaults use database storage, so check your Neon plan if you upload hundreds of big files.
- **AI requests:** allowed up to 2 minutes (set in `vercel.json`).

## How it's secured

- **Access code and PINs are checked on the server.** They're stored only as salted PBKDF2 hashes and never sent to the browser.
- **Sessions:**
  - Sessions use a random key in a secure cookie that page scripts can't read.
  - The screen locks after 15 minutes idle, and the server refuses data until the PIN is entered again.
  - Sessions end after 12 hours.
- **Lockouts:**
  - 5 wrong codes from one internet address lock that address out for 15 minutes.
  - 5 wrong PINs lock that account for 15 minutes.
- **Audit log:** every sign-in, sign-out, lock, failed attempt, edit, import, export and admin action is recorded with the person, time, device and IP address.
- **Roles:** only super admins can delete records, manage users, change the code or change policies. The server enforces this.
- **Deactivating someone** ends their sessions on every device. Super admins can see active sessions and sign anyone out.
- **Browser protection:** strict browser security headers, the site can't be embedded in other pages, and search engines are told not to index it.

## Looking after it

- **Backups:** Neon keeps restore points ([Neon docs](https://neon.com/docs)). Also download a full backup from **Security** each month and keep it somewhere safe, because it contains personal data.
- **Change the access code** whenever someone leaves or it may have been shared.
- **Updates:** replace files in GitHub and Vercel redeploys on its own. The data stays in Neon.

## Settings reference

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | added by Neon | Database connection |
| `ACCESS_CODE` | none | First-start access code. Remove after first sign-in. |
| `ANTHROPIC_API_KEY` | none | Turns on the AI features |
| `ANTHROPIC_MODEL` | `claude-sonnet-5` | Model for matching, deck reading and drafts |
| `ANTHROPIC_MODEL_QUICK` | `claude-haiku-4-5-20251001` | Model for bulk tidy-ups and brief reading |
| `SEED_EXAMPLES` | `true` | Set to `false` to start with no example records |
| `APP_TIMEZONE` | `Europe/London` | Time zone used to group the audit log by day |
