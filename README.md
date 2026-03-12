# Northwind Analytics — Demo Data Platform

A realistic B2B SaaS dataset for **Northwind Analytics**, a fictional BI/analytics company. Designed as a living demo environment for [Omni BI](https://omni.co).

The dataset includes 3 years of historical data across 10 tables: companies, subscriptions, invoices, product usage, support tickets, sales deals, NPS surveys, product events, employees, and plans. A daily GitHub Actions pipeline keeps the data fresh automatically.

---

## Quick Setup (Step by Step)

### 1. Create a Free PostgreSQL Database

1. Go to [neon.tech](https://neon.tech) and sign up for a free account
2. Click **New Project**
3. Choose a project name (e.g. `northwind-demo`)
4. Select a region close to you (e.g. **AWS eu-central-1 Frankfurt** if you're in Ireland/UK)
5. Click **Create Project**
6. On the dashboard, find your **Connection string** — it looks like:
   ```
   postgresql://username:password@ep-something-123.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
7. Copy this connection string — you'll need it in the next steps

### 2. Create a GitHub Repository

1. Go to [github.com/new](https://github.com/new) and create a new repository (e.g. `northwind-demo`)
2. Upload all the files from this project to the repository:
   - `schema.sql`
   - `generate_data.py`
   - `load_data.py`
   - `requirements.txt`
   - `.gitignore`
   - `.github/workflows/daily_data.yml`
   - This `README.md`

### 3. Add Your Database Connection as a Secret

1. In your GitHub repository, go to **Settings** > **Secrets and variables** > **Actions**
2. Click **New repository secret**
3. Name: `DATABASE_URL`
4. Value: paste your Neon connection string from step 1
5. Click **Add secret**

### 4. Run the Initial Data Load

You have two options:

#### Option A: Run via GitHub Actions (Easiest)

1. In your GitHub repo, go to **Actions** > **Daily Data Pipeline**
2. Click **Run workflow**
3. Set mode to **init**
4. Click **Run workflow**
5. Wait ~2-3 minutes for it to complete (watch the green checkmark)

#### Option B: Run Locally

```bash
# Install Python dependencies
pip install -r requirements.txt

# Generate all historical data (~500 companies, 3 years)
python generate_data.py --mode full

# Set your database connection string
export DATABASE_URL='postgresql://user:pass@host/neondb?sslmode=require'

# Load everything into PostgreSQL
python load_data.py --mode init
```

### 5. Verify Your Data

1. In [Neon Console](https://console.neon.tech), open your project
2. Click **SQL Editor**
3. Run a few test queries:

```sql
-- Check row counts
SELECT 'companies' AS table_name, COUNT(*) FROM companies
UNION ALL SELECT 'subscriptions', COUNT(*) FROM subscriptions
UNION ALL SELECT 'invoices', COUNT(*) FROM invoices
UNION ALL SELECT 'product_usage', COUNT(*) FROM product_usage
UNION ALL SELECT 'support_tickets', COUNT(*) FROM support_tickets
UNION ALL SELECT 'deals', COUNT(*) FROM deals;

-- MRR by plan tier
SELECT p.tier, SUM(s.mrr) AS total_mrr
FROM subscriptions s
JOIN plans p ON s.plan_id = p.plan_id
WHERE s.status = 'active'
GROUP BY p.tier
ORDER BY total_mrr DESC;
```

### 6. Connect Omni to Your Database

1. In Omni, go to **Connections** and add a new PostgreSQL connection
2. Enter your Neon connection details (host, database, username, password)
3. Follow the Omni setup guide: [docs.omni.co/connect-data/setup/postgres](https://docs.omni.co/connect-data/setup/postgres)
4. Once connected, you'll see all 10 tables ready to explore

### 7. Automatic Daily Updates

The GitHub Actions workflow runs every day at 6:00 AM UTC. It:
1. Generates today's incremental data (new usage, tickets, deals, events)
2. Loads it into your Neon PostgreSQL database

No action needed — your demo stays fresh automatically.

---

## Data Model

| Table | Description | ~Rows |
|---|---|---|
| `employees` | Northwind Analytics internal team | ~43 |
| `plans` | Subscription plan tiers | 4 |
| `companies` | Customer accounts | ~500 |
| `subscriptions` | Active/cancelled subscriptions | ~550 |
| `invoices` | Billing records | ~6,000 |
| `product_usage` | Daily usage metrics per company | ~250,000 |
| `support_tickets` | Customer support tickets | ~25,000 |
| `deals` | Sales pipeline | ~3,000 |
| `events` | Product events (sampled) | ~500,000 |
| `nps_surveys` | NPS survey responses | ~3,000 |

### Key Relationships

```
employees ──< companies (assigned_csm, assigned_rep)
plans ──< subscriptions
companies ──< subscriptions ──< invoices
companies ──< product_usage
companies ──< support_tickets
companies ──< events
companies ──< nps_surveys
employees ──< support_tickets (assigned_agent)
employees ──< deals (owner)
```

### Built-in Business Patterns

- **Seasonality**: Q4 revenue/deal spikes, summer slowdowns, Monday dips, weekend near-zero usage
- **Cohort behavior**: companies start as trial → convert (or churn) → upgrade over time
- **Correlation**: healthy companies use more, get better NPS scores, churn less
- **Company size**: larger companies → higher plans, more usage, more tickets
- **Churn**: 5-8% annual rate, higher for smaller companies
- **Sales pipeline**: ~25% win rate, realistic stage durations

---

## File Structure

```
northwind-demo/
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── schema.sql              # PostgreSQL schema (10 tables)
├── generate_data.py        # Data generator (full + daily modes)
├── load_data.py            # Database loader (init + daily modes)
├── .gitignore              # Ignores data/ and __pycache__
├── .github/
│   └── workflows/
│       └── daily_data.yml  # Daily cron pipeline
└── data/                   # Generated CSVs (gitignored)
```
