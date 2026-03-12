-- Northwind Analytics Demo Data Platform
-- Schema for a fictional B2B SaaS company selling a BI/analytics product
-- Designed as a demo environment for Omni BI

-- Clean slate
DROP TABLE IF EXISTS nps_surveys CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS support_tickets CASCADE;
DROP TABLE IF EXISTS product_usage CASCADE;
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS subscriptions CASCADE;
DROP TABLE IF EXISTS deals CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS plans CASCADE;
DROP TABLE IF EXISTS employees CASCADE;

-- -------------------------------------------------------------------
-- EMPLOYEES — internal Northwind Analytics team
-- -------------------------------------------------------------------
CREATE TABLE employees (
    employee_id   SERIAL PRIMARY KEY,
    full_name     TEXT NOT NULL,
    role          TEXT NOT NULL,            -- e.g. "Account Executive", "CSM", "Support Engineer"
    department    TEXT NOT NULL CHECK (department IN ('sales', 'cs', 'support', 'engineering', 'marketing', 'leadership')),
    hire_date     DATE NOT NULL,
    region        TEXT NOT NULL CHECK (region IN ('North America', 'EMEA', 'APAC', 'LATAM')),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE
);

-- -------------------------------------------------------------------
-- PLANS — subscription tiers Northwind Analytics offers
-- -------------------------------------------------------------------
CREATE TABLE plans (
    plan_id        SERIAL PRIMARY KEY,
    plan_name      TEXT NOT NULL UNIQUE,
    tier           TEXT NOT NULL CHECK (tier IN ('starter', 'growth', 'enterprise')),
    monthly_price  NUMERIC(10,2) NOT NULL,
    annual_price   NUMERIC(10,2) NOT NULL,   -- discounted annual total
    max_users      INT NOT NULL,
    features       TEXT NOT NULL              -- comma-separated feature list
);

-- -------------------------------------------------------------------
-- COMPANIES — customer accounts
-- -------------------------------------------------------------------
CREATE TABLE companies (
    company_id    SERIAL PRIMARY KEY,
    company_name  TEXT NOT NULL,
    industry      TEXT NOT NULL,
    employee_count INT NOT NULL,
    region        TEXT NOT NULL CHECK (region IN ('North America', 'EMEA', 'APAC', 'LATAM')),
    country       TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    status        TEXT NOT NULL CHECK (status IN ('active', 'churned', 'trial')),
    assigned_csm  INT REFERENCES employees(employee_id),
    assigned_rep  INT REFERENCES employees(employee_id)
);

CREATE INDEX idx_companies_status ON companies(status);
CREATE INDEX idx_companies_region ON companies(region);
CREATE INDEX idx_companies_industry ON companies(industry);
CREATE INDEX idx_companies_created_at ON companies(created_at);

-- -------------------------------------------------------------------
-- SUBSCRIPTIONS — links companies to plans
-- -------------------------------------------------------------------
CREATE TABLE subscriptions (
    subscription_id  SERIAL PRIMARY KEY,
    company_id       INT NOT NULL REFERENCES companies(company_id),
    plan_id          INT NOT NULL REFERENCES plans(plan_id),
    start_date       DATE NOT NULL,
    end_date         DATE,                   -- NULL = still active
    mrr              NUMERIC(10,2) NOT NULL,  -- monthly recurring revenue
    arr              NUMERIC(10,2) NOT NULL,  -- annual recurring revenue
    billing_cycle    TEXT NOT NULL CHECK (billing_cycle IN ('monthly', 'annual')),
    status           TEXT NOT NULL CHECK (status IN ('active', 'cancelled', 'paused'))
);

CREATE INDEX idx_subscriptions_company ON subscriptions(company_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status);
CREATE INDEX idx_subscriptions_start ON subscriptions(start_date);

-- -------------------------------------------------------------------
-- INVOICES — billing records
-- -------------------------------------------------------------------
CREATE TABLE invoices (
    invoice_id      SERIAL PRIMARY KEY,
    subscription_id INT NOT NULL REFERENCES subscriptions(subscription_id),
    company_id      INT NOT NULL REFERENCES companies(company_id),
    amount          NUMERIC(10,2) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'USD',
    issued_date     DATE NOT NULL,
    due_date        DATE NOT NULL,
    paid_date       DATE,
    status          TEXT NOT NULL CHECK (status IN ('paid', 'pending', 'overdue', 'void'))
);

CREATE INDEX idx_invoices_company ON invoices(company_id);
CREATE INDEX idx_invoices_subscription ON invoices(subscription_id);
CREATE INDEX idx_invoices_issued ON invoices(issued_date);
CREATE INDEX idx_invoices_status ON invoices(status);

-- -------------------------------------------------------------------
-- PRODUCT_USAGE — daily usage metrics per company
-- -------------------------------------------------------------------
CREATE TABLE product_usage (
    usage_id          SERIAL PRIMARY KEY,
    company_id        INT NOT NULL REFERENCES companies(company_id),
    usage_date        DATE NOT NULL,
    daily_active_users INT NOT NULL DEFAULT 0,
    queries_run       INT NOT NULL DEFAULT 0,
    dashboards_viewed INT NOT NULL DEFAULT 0,
    reports_exported  INT NOT NULL DEFAULT 0,
    api_calls         INT NOT NULL DEFAULT 0,
    sessions          INT NOT NULL DEFAULT 0,
    UNIQUE (company_id, usage_date)
);

CREATE INDEX idx_usage_company ON product_usage(company_id);
CREATE INDEX idx_usage_date ON product_usage(usage_date);

-- -------------------------------------------------------------------
-- SUPPORT_TICKETS — customer support interactions
-- -------------------------------------------------------------------
CREATE TABLE support_tickets (
    ticket_id      SERIAL PRIMARY KEY,
    company_id     INT NOT NULL REFERENCES companies(company_id),
    created_at     TIMESTAMP NOT NULL,
    resolved_at    TIMESTAMP,
    category       TEXT NOT NULL CHECK (category IN (
        'bug', 'feature_request', 'how_to', 'billing', 'performance', 'integration', 'security', 'data_issue'
    )),
    priority       TEXT NOT NULL CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    status         TEXT NOT NULL CHECK (status IN ('open', 'in_progress', 'resolved', 'closed')),
    csat_score     INT CHECK (csat_score BETWEEN 1 AND 5),
    assigned_agent INT REFERENCES employees(employee_id)
);

CREATE INDEX idx_tickets_company ON support_tickets(company_id);
CREATE INDEX idx_tickets_created ON support_tickets(created_at);
CREATE INDEX idx_tickets_status ON support_tickets(status);

-- -------------------------------------------------------------------
-- DEALS — sales pipeline
-- -------------------------------------------------------------------
CREATE TABLE deals (
    deal_id        SERIAL PRIMARY KEY,
    company_name   TEXT NOT NULL,
    deal_name      TEXT NOT NULL,
    stage          TEXT NOT NULL CHECK (stage IN (
        'prospecting', 'qualification', 'demo', 'proposal', 'negotiation', 'closed_won', 'closed_lost'
    )),
    amount         NUMERIC(10,2) NOT NULL,
    close_date     DATE,
    probability    INT NOT NULL CHECK (probability BETWEEN 0 AND 100),
    owner          INT REFERENCES employees(employee_id),
    source         TEXT NOT NULL CHECK (source IN (
        'inbound', 'outbound', 'referral', 'partner', 'event', 'organic'
    )),
    created_at     TIMESTAMP NOT NULL,
    days_in_stage  INT NOT NULL DEFAULT 0,
    lost_at_stage  TEXT CHECK (lost_at_stage IN (
        'prospecting', 'qualification', 'demo', 'proposal', 'negotiation'
    ))
);

CREATE INDEX idx_deals_stage ON deals(stage);
CREATE INDEX idx_deals_owner ON deals(owner);
CREATE INDEX idx_deals_close_date ON deals(close_date);
CREATE INDEX idx_deals_created ON deals(created_at);

-- -------------------------------------------------------------------
-- EVENTS — product events for funnel/behavioral analysis
-- -------------------------------------------------------------------
CREATE TABLE events (
    event_id        SERIAL PRIMARY KEY,
    company_id      INT NOT NULL REFERENCES companies(company_id),
    event_type      TEXT NOT NULL,            -- e.g. 'page_view', 'dashboard_created', 'query_saved', 'invite_sent'
    event_timestamp TIMESTAMP NOT NULL,
    user_id         TEXT NOT NULL,            -- opaque user identifier within the company
    properties      JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX idx_events_company ON events(company_id);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_timestamp ON events(event_timestamp);

-- -------------------------------------------------------------------
-- NPS_SURVEYS — Net Promoter Score responses
-- -------------------------------------------------------------------
CREATE TABLE nps_surveys (
    survey_id     SERIAL PRIMARY KEY,
    company_id    INT NOT NULL REFERENCES companies(company_id),
    score         INT NOT NULL CHECK (score BETWEEN 0 AND 10),
    response_date DATE NOT NULL,
    feedback_text TEXT,
    category      TEXT NOT NULL CHECK (category IN ('detractor', 'passive', 'promoter'))
);

CREATE INDEX idx_nps_company ON nps_surveys(company_id);
CREATE INDEX idx_nps_date ON nps_surveys(response_date);
CREATE INDEX idx_nps_category ON nps_surveys(category);
