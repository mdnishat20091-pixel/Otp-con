# Facebook Verification Assistant (Telegram Bot)

Facebook-এর **অফিসিয়াল** ফোন নম্বর যাচাই ও অ্যাকাউন্ট পুনরুদ্ধার প্রক্রিয়ায় ধাপে ধাপে সহায়তা করার ১০০% বাংলা Telegram Bot।

> 🔒 **নিরাপত্তা নীতি:** Bot কখনো Facebook পাসওয়ার্ড বা OTP চায়/সংরক্ষণ করে না। OTP-এর মতো বার্তা (৪–৮ সংখ্যা) পাঠালে Bot সেটি মুছে সতর্ক করে। Bot গোপনে অ্যাকাউন্ট খোঁজে না এবং Facebook-এর কোনো নিরাপত্তা বাইপাস করে না — ব্যবহারকারীকে শুধু অফিসিয়াল পেজে পাঠায়।

## প্রযুক্তি
Python 3.12 • aiogram 3 • PostgreSQL (asyncpg) • Docker • Railway / VPS

## ফিচার
- **নতুন অ্যাকাউন্ট যাচাই** ও **অ্যাকাউন্ট পুনরুদ্ধার** — সম্পূর্ণ আলাদা দুটি ফিচার (আলাদা Handler, Keyboard, State, Session টেবিল)
- নেটওয়ার্ক সেটিংস (Connection/Network Test/Proxy তথ্য/Server Status), প্রোফাইল, ইতিহাস, সেটিংস
- অ্যাডমিন ড্যাশবোর্ড: ব্যবহারকারী ব্লক/আনব্লক, পরিসংখ্যান, সবাইকে Notification, Activity ও Application Logs, Cloud/Database Status, Backup, Bot Settings, Admin Management, Maintenance Mode, Server Restart
- Rate Limiting, Ban, Maintenance, অ্যাডমিন-অনলি সুরক্ষা, অননুমোদিত অ্যাক্সেসের অ্যালার্ট
- প্রতি ৫ মিনিটে Health Monitoring (Telegram, Database, CPU, Memory, Uptime, Error) ও সমস্যায় অ্যাডমিন অ্যালার্ট
- স্বয়ংক্রিয় Backup (gzip JSON) — অ্যাডমিনের Telegram-এ পাঠানো হয়
- `/health` HTTP এন্ডপয়েন্ট (Railway Health Check)

## ফোল্ডার কাঠামো
```
app/            main.py, config.py, database.py
  handlers/     start, verification, recovery, profile, history, network, admin
  keyboards/    main_menu, verification, recovery, admin
  services/     status, security, logging, notifications, backup, ui
  states/       verification, recovery, admin
database/       models.py (স্কিমা), migrations.py
Dockerfile • docker-compose.yml • railway.toml • requirements.txt • .env.example
```

## Environment Variables
| নাম | আবশ্যক | বিবরণ |
|---|---|---|
| `BOT_TOKEN` | ✅ | @BotFather থেকে পাওয়া Token |
| `DATABASE_URL` | ✅ | PostgreSQL সংযোগ URL |
| `ADMIN_IDS` | প্রস্তাবিত | অ্যাডমিনদের Telegram ID, কমা দিয়ে (যেমন `12345,67890`)। ID পেতে @userinfobot |
| `TIMEZONE` | | ডিফল্ট `Asia/Dhaka` |
| `LOG_LEVEL` | | ডিফল্ট `INFO` |
| `WEBHOOK_URL` | | দিলে Webhook মোড, না দিলে Polling |
| `PROXY_URL` | | Telegram API ব্লক থাকলে বৈধ প্রক্সি |
| `BACKUP_INTERVAL_HOURS` | | ডিফল্ট `24` |
| `RATE_LIMIT_COUNT` / `RATE_LIMIT_WINDOW` | | ডিফল্ট ১০ অনুরোধ / ১০ সেকেন্ড |

⚠️ **কোনো Secret কখনো GitHub-এ আপলোড করবেন না।** `.env` ফাইল `.gitignore`-এ আছে।

## লোকাল পরীক্ষা
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # মান বসান
export $(grep -v '^#' .env | xargs)
python -m app.main
```

## Railway-তে Deploy (প্রধান পদ্ধতি)
1. প্রজেক্টটি GitHub-এ আপলোড করুন (`git init && git add . && git commit -m init && git push`)।
2. Railway → **New Project → Deploy from GitHub repo** → রিপোজিটরি বেছে নিন।
3. **+ New → Database → PostgreSQL** যোগ করুন (Persistent Volume স্বয়ংক্রিয়)।
4. Bot সার্ভিসের **Variables** ট্যাবে দিন: `BOT_TOKEN`, `ADMIN_IDS`, `TIMEZONE`, `LOG_LEVEL`, এবং `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`।
5. `railway.toml` ও `Dockerfile` দেখে Railway নিজে Build ও Deploy করবে।
6. **Deployments → Logs** দেখুন: "Bot চালু হয়েছে" লেখা থাকলে সফল। অ্যাডমিনরা Telegram-এ "🟢 Bot চালু হয়েছে" পাবেন।

> একই `BOT_TOKEN` দিয়ে একসঙ্গে একাধিক Polling Instance চালাবেন না (Telegram Conflict ত্রুটি হবে)। লোকালে পরীক্ষার সময় Railway-র Bot বন্ধ রাখুন বা আলাদা Token নিন।

## VPS-এ Deploy
```bash
git clone <আপনার-রিপো> && cd facebook_verification_bot
cp .env.example .env          # BOT_TOKEN, ADMIN_IDS ইত্যাদি বসান
export POSTGRES_PASSWORD='শক্তিশালী-পাসওয়ার্ড'   # অথবা .env-এ রাখুন
docker compose up -d --build
docker compose logs -f bot
```
`restart: unless-stopped` থাকায় Server রিবুটেও Bot স্বয়ংক্রিয়ভাবে চালু হয়।

## Deploy-এর পর পরীক্ষা তালিকা
- [ ] `/start` মেনু আসছে
- [ ] প্রতিটি বাটন সাড়া দিচ্ছে (যাচাই, Recovery, নেটওয়ার্ক, ইতিহাস, প্রোফাইল, সেটিংস)
- [ ] `/admin` শুধু অ্যাডমিনের জন্য; অন্য অ্যাকাউন্টে "⛔" আসে ও অ্যাডমিন অ্যালার্ট যায়
- [ ] `/health` ও Database Status ✅
- [ ] Railway-তে Restart দিলেও ব্যবহারকারী/ইতিহাস থেকে যাচ্ছে
- [ ] ইচ্ছাকৃত ত্রুটিতে Logs (Railway Logs ও `/logs`) পাওয়া যাচ্ছে
- [ ] `/backup` → "এখনই Backup নিন" ফাইল পাঠায়

## Backup পুনরুদ্ধার
Backup `.json.gz` ফাইলে সব Non-sensitive টেবিলের ডেটা থাকে। প্রয়োজনে `gunzip` করে JSON থেকে ডেটা আমদানি করা যায়। Railway-র নিজস্ব PostgreSQL Backup সুবিধাও চালু রাখার পরামর্শ।

## সমস্যা সমাধান
| সমস্যা | সমাধান |
|---|---|
| `অনুপস্থিত Environment Variable` | `BOT_TOKEN` ও `DATABASE_URL` সেট করুন |
| `TelegramConflictError` | একই Token দিয়ে অন্য কোথাও Bot চলছে — বন্ধ করুন |
| Health Check ব্যর্থ | Database URL ঠিক আছে কি না দেখুন |
| Telegram-এ সংযোগ হচ্ছে না | বৈধ `PROXY_URL` দিন বা Server-এর নেটওয়ার্ক দেখুন |
