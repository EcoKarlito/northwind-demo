#!/usr/bin/env python3
"""
Northwind Analytics — Demo Data Generator
==========================================
Generates 3 years of realistic B2B SaaS data for a fictional BI/analytics company.

Usage:
    python generate_data.py --mode full     # Generate all historical data (default)
    python generate_data.py --mode daily    # Generate only today's incremental data

Outputs CSV files to the data/ directory for bulk loading into PostgreSQL.
"""

import argparse
import csv
import json
import math
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta

from faker import Faker

# ---------------------------------------------------------------------------
# Constants & Configuration
# ---------------------------------------------------------------------------
SEED = 42
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
START_DATE = date(2023, 3, 1)
END_DATE = date.today()  # dynamically set to today
TARGET_COMPANIES = 500

# Industries that would buy a BI/analytics tool
INDUSTRIES = [
    "Technology", "Financial Services", "Healthcare", "E-Commerce", "Manufacturing",
    "Media & Entertainment", "Education", "Logistics", "Real Estate", "Retail",
    "Energy", "Telecommunications", "Professional Services", "Insurance", "Hospitality",
]

REGIONS_WEIGHTS = {
    "North America": 0.45,
    "EMEA": 0.30,
    "APAC": 0.15,
    "LATAM": 0.10,
}

COUNTRIES_BY_REGION = {
    "North America": ["United States", "Canada", "Mexico"],
    "EMEA": ["United Kingdom", "Germany", "France", "Netherlands", "Sweden", "Ireland", "Spain", "Italy"],
    "APAC": ["Australia", "Japan", "Singapore", "India", "South Korea"],
    "LATAM": ["Brazil", "Argentina", "Colombia", "Chile"],
}

# Subscription plans — Northwind Analytics pricing
PLANS = [
    {"plan_id": 1, "plan_name": "Starter",        "tier": "starter",    "monthly_price": 499,   "annual_price": 4990,   "max_users": 10,  "features": "dashboards,basic_queries,email_support,5_data_sources"},
    {"plan_id": 2, "plan_name": "Growth",          "tier": "growth",     "monthly_price": 1499,  "annual_price": 14990,  "max_users": 50,  "features": "dashboards,advanced_queries,api_access,slack_support,25_data_sources,custom_reports"},
    {"plan_id": 3, "plan_name": "Enterprise",      "tier": "enterprise", "monthly_price": 3999,  "annual_price": 39990,  "max_users": 500, "features": "dashboards,advanced_queries,api_access,dedicated_csm,unlimited_data_sources,custom_reports,sso,audit_log,sla"},
    {"plan_id": 4, "plan_name": "Enterprise Plus",  "tier": "enterprise", "monthly_price": 7999,  "annual_price": 79990,  "max_users": 9999,"features": "everything_in_enterprise,embedded_analytics,white_label,custom_integrations,on_prem_option"},
]

# Employee size buckets determine which plan a company tends to pick
# Small (1-100) -> Starter/Growth, Medium (101-1000) -> Growth/Enterprise, Large (1001+) -> Enterprise/Ent+
COMPANY_SIZE_PLAN_MAP = {
    "small":  [1, 1, 1, 2, 2],          # mostly Starter, some Growth
    "medium": [1, 2, 2, 2, 3],          # mostly Growth, some Starter/Enterprise
    "large":  [2, 3, 3, 3, 4],          # mostly Enterprise, some Growth/Ent+
}

# Support ticket categories and their relative weights
TICKET_CATEGORIES = {
    "how_to": 0.30, "bug": 0.20, "feature_request": 0.15, "integration": 0.12,
    "billing": 0.08, "performance": 0.07, "data_issue": 0.05, "security": 0.03,
}

# Deal sources and their relative weights
DEAL_SOURCES = {
    "inbound": 0.30, "outbound": 0.25, "organic": 0.15,
    "referral": 0.12, "partner": 0.10, "event": 0.08,
}

# Deal stages with typical probability and days-in-stage ranges
DEAL_STAGES = {
    "prospecting":   {"prob": 10, "days": (3, 14)},
    "qualification": {"prob": 25, "days": (5, 21)},
    "demo":          {"prob": 40, "days": (3, 14)},
    "proposal":      {"prob": 60, "days": (5, 21)},
    "negotiation":   {"prob": 80, "days": (7, 30)},
    "closed_won":    {"prob": 100, "days": (0, 0)},
    "closed_lost":   {"prob": 0,   "days": (0, 0)},
}

STAGE_ORDER = ["prospecting", "qualification", "demo", "proposal", "negotiation"]

# Product event types for funnel analysis (with realistic frequency weights)
EVENT_TYPES = [
    "page_view", "login", "dashboard_viewed", "dashboard_created",
    "query_run", "query_saved", "report_exported", "invite_sent",
    "data_source_connected", "alert_created", "api_key_generated",
    "settings_changed", "file_uploaded", "chart_created",
]
EVENT_TYPE_WEIGHTS = [
    25, 18, 15, 3,   # page_view, login, dashboard_viewed, dashboard_created
    12, 4, 5, 1,     # query_run, query_saved, report_exported, invite_sent
    2, 2, 1,          # data_source_connected, alert_created, api_key_generated
    4, 5, 3,          # settings_changed, file_uploaded, chart_created
]

# US holidays (month, day) — usage drops on these days
US_HOLIDAYS = [
    (1, 1), (1, 15), (2, 19), (5, 27), (7, 4), (9, 1),
    (11, 27), (11, 28), (12, 24), (12, 25), (12, 31),
]

# NPS feedback templates by category
NPS_FEEDBACK = {
    "promoter": [
        "Love the product! Makes our reporting so much easier.",
        "Best BI tool we've used. The dashboards are fantastic.",
        "Our team can't live without Northwind Analytics now.",
        "Great customer support and the product keeps getting better.",
        "Transformed how we make data-driven decisions.",
        "The self-serve analytics feature is a game changer.",
        "Easy to set up and our whole team adopted it quickly.",
        None,  # some don't leave feedback
    ],
    "passive": [
        "Good product but could use more integrations.",
        "Works well for basic use cases. Advanced features need polish.",
        "Decent value for the price. Nothing extraordinary.",
        "UI could be more intuitive for non-technical users.",
        None,
    ],
    "detractor": [
        "Too expensive for what you get.",
        "Performance issues with large datasets.",
        "Missing key integrations we need.",
        "Support response times are too slow.",
        "Hard to onboard new team members.",
        None,
    ],
}

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def weighted_choice(options_weights: dict) -> str:
    """Pick a random key from a dict of {option: weight}."""
    items = list(options_weights.keys())
    weights = list(options_weights.values())
    return random.choices(items, weights=weights, k=1)[0]


def company_size_bucket(emp_count: int) -> str:
    if emp_count <= 100:
        return "small"
    elif emp_count <= 1000:
        return "medium"
    else:
        return "large"


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def is_holiday(d: date) -> bool:
    return (d.month, d.day) in US_HOLIDAYS


def seasonality_factor(d: date) -> float:
    """
    Returns a multiplier (0.7–1.3) that models business seasonality.
    Q4 is strongest (budget season), summer is weakest.
    """
    month = d.month
    if month in (10, 11, 12):  # Q4 spike
        return 1.2 + random.uniform(0, 0.1)
    elif month in (6, 7, 8):   # Summer slowdown
        return 0.8 + random.uniform(0, 0.1)
    elif month in (1, 2, 3):   # Q1 moderate
        return 0.95 + random.uniform(0, 0.1)
    else:                       # Q2 normal
        return 1.0 + random.uniform(0, 0.05)


def usage_multiplier(d: date) -> float:
    """
    Combined multiplier for usage on a given day.
    Accounts for weekends, holidays, and seasonality.
    """
    mult = seasonality_factor(d)
    if is_weekend(d):
        mult *= random.uniform(0.05, 0.15)  # minimal weekend usage for B2B
    elif is_holiday(d):
        mult *= random.uniform(0.10, 0.25)
    elif d.weekday() == 0:  # Monday dip
        mult *= random.uniform(0.80, 0.90)
    return mult


def days_between(d1: date, d2: date) -> int:
    return (d2 - d1).days


def csv_writer(filename: str, headers: list):
    """Return a CSV writer + file handle for a given filename in DATA_DIR."""
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filename)
    f = open(filepath, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    return writer, f


# ---------------------------------------------------------------------------
# Data Generators
# ---------------------------------------------------------------------------

class NorthwindDataGenerator:
    """Generates all tables for the Northwind Analytics demo dataset."""

    def __init__(self, start_date: date, end_date: date, daily_mode: bool = False):
        self.start_date = start_date
        self.end_date = end_date
        self.daily_mode = daily_mode

        # Counters for serial IDs
        self.next_company_id = 1
        self.next_subscription_id = 1
        self.next_invoice_id = 1
        self.next_usage_id = 1
        self.next_ticket_id = 1
        self.next_deal_id = 1
        self.next_event_id = 1
        self.next_survey_id = 1
        self.next_employee_id = 1

        # In-memory state for cross-table relationships
        self.employees = []
        self.companies = []
        self.subscriptions = []  # active subscriptions keyed by company_id
        self.company_sub_map = {}  # company_id -> current subscription
        self.company_health = {}   # company_id -> health score (0-1)

        # Accumulators for CSV rows
        self.all_employees = []
        self.all_companies = []
        self.all_subscriptions = []
        self.all_invoices = []
        self.all_usage = []
        self.all_tickets = []
        self.all_deals = []
        self.all_events = []
        self.all_nps = []

        # Summary stats
        self.stats = defaultdict(int)

    # -------------------------------------------------------------------
    # Employees
    # -------------------------------------------------------------------
    def generate_employees(self):
        """Generate ~40 internal Northwind Analytics employees."""
        roles = {
            "sales": [
                ("Account Executive", 6), ("SDR", 4), ("VP Sales", 1), ("Sales Manager", 2),
            ],
            "cs": [
                ("Customer Success Manager", 5), ("CS Director", 1), ("Onboarding Specialist", 2),
            ],
            "support": [
                ("Support Engineer", 5), ("Support Lead", 1), ("Technical Support", 3),
            ],
            "engineering": [
                ("Software Engineer", 4), ("Engineering Manager", 1), ("Data Engineer", 2),
            ],
            "marketing": [
                ("Marketing Manager", 1), ("Content Marketer", 1), ("Demand Gen", 1),
            ],
            "leadership": [
                ("CEO", 1), ("CTO", 1), ("CFO", 1),
            ],
        }

        for dept, role_list in roles.items():
            for role_name, count in role_list:
                for _ in range(count):
                    region = weighted_choice(REGIONS_WEIGHTS)
                    # Employees hired over the 3-year window, leadership earlier
                    if dept == "leadership":
                        hire_date = self.start_date - timedelta(days=random.randint(365, 1000))
                    else:
                        hire_date = self.start_date + timedelta(
                            days=random.randint(0, days_between(self.start_date, self.end_date))
                        )

                    emp = {
                        "employee_id": self.next_employee_id,
                        "full_name": fake.name(),
                        "role": role_name,
                        "department": dept,
                        "hire_date": hire_date.isoformat(),
                        "region": region,
                        "is_active": True,
                    }
                    self.next_employee_id += 1
                    self.all_employees.append(emp)
                    self.employees.append(emp)

        self.stats["employees"] = len(self.all_employees)

    def _get_employees_by_dept(self, dept: str) -> list:
        return [e for e in self.employees if e["department"] == dept]

    def _apply_churn_health_penalty(self):
        """Reduce health for churned companies so usage-churn correlation is strong."""
        for company in self.companies:
            if company["status"] == "churned":
                cid = company["company_id"]
                # Heavily penalize health so DAU/usage is visibly lower
                self.company_health[cid] = max(0.1, self.company_health[cid] * 0.35)

    # -------------------------------------------------------------------
    # Companies — acquired gradually over 3 years
    # -------------------------------------------------------------------
    def generate_companies(self):
        """
        Generate ~500 companies, spread over the 3-year window.
        Acquisition rate increases over time (more in later months).
        """
        total_days = days_between(self.start_date, self.end_date)
        csm_list = self._get_employees_by_dept("cs")
        rep_list = self._get_employees_by_dept("sales")

        for i in range(TARGET_COMPANIES):
            # Distribute company creation dates with slight acceleration
            # Use a power curve so more companies appear later
            progress = (i / TARGET_COMPANIES)
            day_offset = int(progress ** 0.85 * total_days)
            created_date = self.start_date + timedelta(days=day_offset + random.randint(-10, 10))
            created_date = max(self.start_date, min(created_date, self.end_date))

            region = weighted_choice(REGIONS_WEIGHTS)
            country = random.choice(COUNTRIES_BY_REGION[region])
            industry = random.choice(INDUSTRIES)

            # Employee count follows a log-normal-ish distribution
            # Mostly small/medium companies, some large enterprises
            emp_count = int(random.lognormvariate(5.0, 1.2))
            emp_count = max(10, min(emp_count, 50000))

            company = {
                "company_id": self.next_company_id,
                "company_name": fake.company(),
                "industry": industry,
                "employee_count": emp_count,
                "region": region,
                "country": country,
                "created_at": datetime.combine(created_date, datetime.min.time()).isoformat(),
                "status": "trial",  # all start as trial
                "assigned_csm": random.choice(csm_list)["employee_id"] if csm_list else None,
                "assigned_rep": random.choice(rep_list)["employee_id"] if rep_list else None,
            }
            self.next_company_id += 1
            self.all_companies.append(company)
            self.companies.append(company)

            # Initial health score — larger companies tend to be more engaged
            size = company_size_bucket(emp_count)
            base_health = {"small": 0.55, "medium": 0.65, "large": 0.75}[size]
            self.company_health[company["company_id"]] = base_health + random.uniform(-0.1, 0.15)

        self.stats["companies"] = len(self.all_companies)

    # -------------------------------------------------------------------
    # Subscriptions — trial conversion, upgrades, churn
    # -------------------------------------------------------------------
    def generate_subscriptions(self):
        """
        For each company, simulate their subscription journey:
        - Start on trial (~14 days)
        - Convert to paid (70-80% conversion) or churn
        - Some upgrade over time, few downgrade
        - Annual churn rate ~5-8%, higher for smaller companies
        """
        for company in self.companies:
            cid = company["company_id"]
            created = date.fromisoformat(company["created_at"][:10])
            emp_count = company["employee_count"]
            size = company_size_bucket(emp_count)
            health = self.company_health[cid]

            # Trial period: 14 days
            trial_end = created + timedelta(days=14)

            # Trial-to-paid conversion rate depends on company size and health
            conversion_rate = 0.65 + (health - 0.5) * 0.3  # 0.50–0.80
            if random.random() > conversion_rate:
                # Churned during trial
                company["status"] = "churned"
                self.stats["churned_trial"] += 1
                continue

            # Pick initial plan based on company size
            plan_id = random.choice(COMPANY_SIZE_PLAN_MAP[size])
            plan = PLANS[plan_id - 1]
            billing_cycle = random.choices(["monthly", "annual"], weights=[0.4, 0.6])[0]

            if billing_cycle == "monthly":
                mrr = float(plan["monthly_price"])
            else:
                mrr = round(float(plan["annual_price"]) / 12, 2)
            arr = round(mrr * 12, 2)

            sub_start = trial_end
            company["status"] = "active"

            # Simulate the subscription lifecycle through the entire period
            current_plan_id = plan_id
            current_date = sub_start

            while current_date < self.end_date:
                plan = PLANS[current_plan_id - 1]
                if billing_cycle == "monthly":
                    mrr = float(plan["monthly_price"])
                else:
                    mrr = round(float(plan["annual_price"]) / 12, 2)
                arr = round(mrr * 12, 2)

                # Determine how long this subscription lasts
                # Check for churn — annual rate 5-8% → monthly ~0.4-0.7%
                monthly_churn_rate = {"small": 0.007, "medium": 0.005, "large": 0.003}[size]
                monthly_churn_rate *= (1.3 - health)  # less healthy = more churn

                # Check for upgrade — healthier companies upgrade more
                monthly_upgrade_rate = health * 0.015  # up to ~1.5%/month for healthy companies

                # Run month by month until churn, upgrade, or end of data
                sub_end = None
                sub_status = "active"
                months_on_plan = 0

                while current_date < self.end_date:
                    months_on_plan += 1
                    next_month = current_date + timedelta(days=30)

                    # Churn check (don't churn in first 2 months — honeymoon period)
                    if months_on_plan > 2 and random.random() < monthly_churn_rate:
                        sub_end = current_date
                        sub_status = "cancelled"
                        company["status"] = "churned"
                        break

                    # Upgrade check (only if not already on highest plan)
                    if current_plan_id < 4 and months_on_plan > 3 and random.random() < monthly_upgrade_rate:
                        sub_end = current_date
                        sub_status = "cancelled"  # old sub ends
                        break

                    current_date = next_month

                # Record this subscription
                sub = {
                    "subscription_id": self.next_subscription_id,
                    "company_id": cid,
                    "plan_id": current_plan_id,
                    "start_date": sub_start.isoformat() if isinstance(sub_start, date) else sub_start,
                    "end_date": sub_end.isoformat() if sub_end else None,
                    "mrr": mrr,
                    "arr": arr,
                    "billing_cycle": billing_cycle,
                    "status": sub_status if sub_end else "active",
                }
                self.next_subscription_id += 1
                self.all_subscriptions.append(sub)
                self.subscriptions.append(sub)

                if sub_end and company["status"] != "churned":
                    # This was an upgrade — start new sub on next plan
                    current_plan_id = min(current_plan_id + 1, 4)
                    sub_start = sub_end
                    current_date = sub_end
                    # Update health — upgraders tend to be healthier
                    self.company_health[cid] = min(1.0, self.company_health[cid] + 0.05)
                elif sub_end:
                    # Churned
                    break
                else:
                    # Still active, reached end_date
                    self.company_sub_map[cid] = sub
                    break

        self.stats["subscriptions"] = len(self.all_subscriptions)
        self.stats["active_companies"] = sum(1 for c in self.companies if c["status"] == "active")
        self.stats["churned_companies"] = sum(1 for c in self.companies if c["status"] == "churned")

    # -------------------------------------------------------------------
    # Invoices — generated from subscriptions
    # -------------------------------------------------------------------
    def generate_invoices(self):
        """
        Generate invoices for each subscription period.
        Monthly billing = one invoice per month.
        Annual billing = one invoice per year (at the annual price).
        Most invoices are paid, some are overdue, very few are void.
        """
        for sub in self.all_subscriptions:
            start = date.fromisoformat(sub["start_date"])
            end = date.fromisoformat(sub["end_date"]) if sub["end_date"] else self.end_date
            plan = PLANS[sub["plan_id"] - 1]
            cid = sub["company_id"]

            if sub["billing_cycle"] == "monthly":
                interval_days = 30
                amount = float(plan["monthly_price"])
            else:
                interval_days = 365
                amount = float(plan["annual_price"])

            current = start
            while current < end:
                due_date = current + timedelta(days=30)

                # Determine payment status
                r = random.random()
                if r < 0.88:
                    status = "paid"
                    paid_date = (current + timedelta(days=random.randint(1, 25))).isoformat()
                elif r < 0.95:
                    status = "pending"
                    paid_date = None
                elif r < 0.98:
                    status = "overdue"
                    paid_date = None
                else:
                    status = "void"
                    paid_date = None

                # For older invoices that are "pending", flip them to paid
                if status == "pending" and current < self.end_date - timedelta(days=45):
                    status = "paid"
                    paid_date = (current + timedelta(days=random.randint(25, 40))).isoformat()

                inv = {
                    "invoice_id": self.next_invoice_id,
                    "subscription_id": sub["subscription_id"],
                    "company_id": cid,
                    "amount": amount,
                    "currency": "USD",
                    "issued_date": current.isoformat(),
                    "due_date": due_date.isoformat(),
                    "paid_date": paid_date,
                    "status": status,
                }
                self.next_invoice_id += 1
                self.all_invoices.append(inv)

                current += timedelta(days=interval_days)

        self.stats["invoices"] = len(self.all_invoices)

    # -------------------------------------------------------------------
    # Product Usage — daily metrics per active company
    # -------------------------------------------------------------------
    def generate_usage(self, date_range=None):
        """
        Generate daily usage rows for each company that has an active subscription.
        Usage scales with company size and health. Weekends/holidays have low usage.
        """
        if date_range is None:
            date_range = (self.start_date, self.end_date)

        # Build a map of which companies are active on which dates
        # For efficiency, we iterate day by day
        total_days = days_between(date_range[0], date_range[1])

        for day_offset in range(total_days):
            current_day = date_range[0] + timedelta(days=day_offset)
            mult = usage_multiplier(current_day)

            for company in self.companies:
                cid = company["company_id"]
                created = date.fromisoformat(company["created_at"][:10])

                # Skip if company doesn't exist yet
                if current_day < created:
                    continue

                # Skip if company has churned before this date
                if company["status"] == "churned":
                    # Find the churn date from subscriptions
                    churn_date = None
                    for sub in self.all_subscriptions:
                        if sub["company_id"] == cid and sub["status"] == "cancelled" and sub["end_date"]:
                            cd = date.fromisoformat(sub["end_date"])
                            if churn_date is None or cd > churn_date:
                                churn_date = cd
                    if churn_date and current_day > churn_date + timedelta(days=7):
                        continue

                # Determine base usage from company size and health
                emp = company["employee_count"]
                health = self.company_health.get(cid, 0.5)
                size = company_size_bucket(emp)

                # Base DAU as fraction of employee count (analytics tool used by subset)
                dau_pct = {"small": 0.15, "medium": 0.08, "large": 0.03}[size]
                base_dau = max(1, int(emp * dau_pct * health))

                # Apply day-level multiplier
                dau = max(0, int(base_dau * mult * random.uniform(0.7, 1.3)))

                if dau == 0:
                    # Still generate a row but with zeros on weekends
                    if not is_weekend(current_day):
                        dau = random.randint(0, 2)

                # Other metrics scale with DAU
                queries = int(dau * random.uniform(3, 12))
                dashboards = int(dau * random.uniform(1, 5))
                reports = int(dau * random.uniform(0.1, 1.0))
                api_calls = int(dau * random.uniform(5, 30)) if health > 0.4 else 0
                sessions = int(dau * random.uniform(1.2, 2.5))

                usage = {
                    "usage_id": self.next_usage_id,
                    "company_id": cid,
                    "usage_date": current_day.isoformat(),
                    "daily_active_users": dau,
                    "queries_run": queries,
                    "dashboards_viewed": dashboards,
                    "reports_exported": reports,
                    "api_calls": api_calls,
                    "sessions": sessions,
                }
                self.next_usage_id += 1
                self.all_usage.append(usage)

        self.stats["usage_rows"] = len(self.all_usage)

    # -------------------------------------------------------------------
    # Support Tickets — ~2-5 per company per month
    # -------------------------------------------------------------------
    def generate_tickets(self, date_range=None):
        """
        Generate support tickets. Rate scales with company size.
        Enterprise customers create more tickets and get higher priority.
        """
        if date_range is None:
            date_range = (self.start_date, self.end_date)

        support_agents = self._get_employees_by_dept("support")

        for company in self.companies:
            cid = company["company_id"]
            created = date.fromisoformat(company["created_at"][:10])
            emp = company["employee_count"]
            size = company_size_bucket(emp)
            health = self.company_health.get(cid, 0.5)

            # Tickets per month based on size
            monthly_rate = {"small": 2.5, "medium": 4.0, "large": 6.0}[size]
            # Unhealthier companies generate more tickets
            monthly_rate *= (1.5 - health)

            total_days_active = days_between(
                max(created, date_range[0]),
                min(self.end_date, date_range[1])
            )

            if total_days_active <= 0:
                continue

            # Convert monthly rate to expected tickets over the active period
            expected = monthly_rate * (total_days_active / 30.0) * random.uniform(0.6, 1.4)
            # Use Poisson-like sampling for short periods so daily mode still generates tickets
            num_tickets = int(expected)
            if random.random() < (expected - num_tickets):
                num_tickets += 1

            for _ in range(num_tickets):
                # Random date within the company's active period
                active_start = max(created, date_range[0])
                active_end = date_range[1]
                if active_start >= active_end:
                    continue
                span = max(1, days_between(active_start, active_end))
                ticket_date = active_start + timedelta(
                    days=random.randint(0, span - 1)
                )

                category = weighted_choice(TICKET_CATEGORIES)

                # Priority — enterprise gets more high/urgent
                if size == "large":
                    priority = random.choices(
                        ["low", "medium", "high", "urgent"],
                        weights=[0.1, 0.3, 0.4, 0.2]
                    )[0]
                elif size == "medium":
                    priority = random.choices(
                        ["low", "medium", "high", "urgent"],
                        weights=[0.2, 0.4, 0.3, 0.1]
                    )[0]
                else:
                    priority = random.choices(
                        ["low", "medium", "high", "urgent"],
                        weights=[0.3, 0.4, 0.2, 0.1]
                    )[0]

                # Resolution time depends on priority
                resolve_hours = {
                    "urgent": random.randint(1, 8),
                    "high": random.randint(4, 48),
                    "medium": random.randint(12, 120),
                    "low": random.randint(24, 240),
                }[priority]

                # Compute created_at timestamp first, then resolve relative to it
                created_hour = random.randint(8, 18)
                created_minute = random.randint(0, 59)
                created_at = datetime.combine(ticket_date, datetime.min.time()).replace(
                    hour=created_hour, minute=created_minute
                )
                resolved_at = created_at + timedelta(hours=resolve_hours)

                # Status — most are resolved/closed, some recent ones are open
                if ticket_date > self.end_date - timedelta(days=3):
                    status = random.choices(
                        ["open", "in_progress", "resolved", "closed"],
                        weights=[0.4, 0.3, 0.2, 0.1]
                    )[0]
                else:
                    status = random.choices(
                        ["open", "in_progress", "resolved", "closed"],
                        weights=[0.02, 0.03, 0.35, 0.60]
                    )[0]

                if status in ("open", "in_progress"):
                    resolved_at = None

                # CSAT score — correlates with resolution time and health
                csat = None
                if status in ("resolved", "closed") and random.random() < 0.65:
                    base_csat = 3.5 + health * 1.5
                    csat = max(1, min(5, round(base_csat + random.uniform(-1.5, 1.0))))

                ticket = {
                    "ticket_id": self.next_ticket_id,
                    "company_id": cid,
                    "created_at": created_at.isoformat(),
                    "resolved_at": resolved_at.isoformat() if resolved_at else None,
                    "category": category,
                    "priority": priority,
                    "status": status,
                    "csat_score": csat,
                    "assigned_agent": random.choice(support_agents)["employee_id"] if support_agents else None,
                }
                self.next_ticket_id += 1
                self.all_tickets.append(ticket)

        self.stats["tickets"] = len(self.all_tickets)

    # -------------------------------------------------------------------
    # Deals — sales pipeline
    # -------------------------------------------------------------------
    def generate_deals(self, date_range=None):
        """
        Generate sales deals. ~25% win rate.
        Deal amounts correlate with company size.
        Q4 has more deals (budget season).
        """
        if date_range is None:
            date_range = (self.start_date, self.end_date)

        sales_reps = self._get_employees_by_dept("sales")
        total_days = days_between(date_range[0], date_range[1])

        # ~30-40 new deals per month growing over time
        base_monthly_deals = 30
        expected = base_monthly_deals * (total_days / 30.0) * random.uniform(0.9, 1.1)
        total_deals = int(expected)
        # Poisson-like rounding for short periods (e.g. daily mode)
        if random.random() < (expected - total_deals):
            total_deals += 1

        for i in range(total_deals):
            # Spread deals across the time range with Q4 weighting
            day_offset = random.randint(0, max(1, days_between(date_range[0], date_range[1]) - 1))
            created_date = date_range[0] + timedelta(days=day_offset)

            # Q4 boost — 50% more deals
            if created_date.month in (10, 11, 12) and random.random() < 0.33:
                continue  # skip some non-Q4 deals to create Q4 concentration

            # Company name — mix of existing companies and net new prospects
            if random.random() < 0.3 and self.companies:
                company = random.choice(self.companies)
                deal_company_name = company["company_name"]
                # Amount correlates to company size
                emp = company["employee_count"]
            else:
                deal_company_name = fake.company()
                emp = int(random.lognormvariate(5.0, 1.2))

            size = company_size_bucket(emp)
            base_amount = {"small": 6000, "medium": 36000, "large": 120000}[size]
            amount = round(base_amount * random.uniform(0.5, 2.0), 2)

            source = weighted_choice(DEAL_SOURCES)
            owner = random.choice(sales_reps)["employee_id"] if sales_reps else None

            # Simulate deal progression through stages
            # ~25% end up closed_won, ~35% closed_lost, rest still in pipeline
            r = random.random()
            lost_at_stage = None  # track where lost deals dropped out
            if created_date < self.end_date - timedelta(days=90):
                # Older deals should be closed
                if r < 0.25:
                    final_stage = "closed_won"
                else:
                    final_stage = "closed_lost"
                    # Realistic funnel: most lost deals drop at early stages
                    lost_at_stage = random.choices(
                        STAGE_ORDER,
                        weights=[10, 20, 30, 25, 15],  # more drop at demo/proposal
                        k=1,
                    )[0]
            elif created_date < self.end_date - timedelta(days=30):
                if r < 0.20:
                    final_stage = "closed_won"
                elif r < 0.50:
                    final_stage = "closed_lost"
                    lost_at_stage = random.choices(
                        STAGE_ORDER,
                        weights=[10, 20, 30, 25, 15],
                        k=1,
                    )[0]
                else:
                    final_stage = random.choice(STAGE_ORDER[2:])  # demo, proposal, negotiation
            else:
                # Recent deals still in early stages
                final_stage = random.choice(STAGE_ORDER[:3])  # prospecting, qualification, demo

            # Calculate days in current stage
            stage_info = DEAL_STAGES[final_stage]
            if final_stage in ("closed_won", "closed_lost"):
                # Sum up time through stages
                total_deal_days = 0
                # For lost deals, only sum up to the stage where they were lost
                end_stage = "negotiation" if final_stage == "closed_won" else lost_at_stage
                for s in STAGE_ORDER:
                    s_info = DEAL_STAGES[s]
                    total_deal_days += random.randint(s_info["days"][0], s_info["days"][1])
                    if s == end_stage:
                        break
                close_date = (created_date + timedelta(days=total_deal_days)).isoformat()
                days_in_stage = 0
                probability = stage_info["prob"]
            else:
                close_date = (created_date + timedelta(days=random.randint(30, 120))).isoformat()
                days_in_stage = random.randint(stage_info["days"][0], stage_info["days"][1])
                probability = stage_info["prob"]

            deal = {
                "deal_id": self.next_deal_id,
                "company_name": deal_company_name,
                "deal_name": f"{deal_company_name} - {random.choice(['Annual', 'Enterprise', 'Growth', 'Expansion', 'Renewal', 'New Business'])} Deal",
                "stage": final_stage,
                "amount": amount,
                "close_date": close_date,
                "probability": probability,
                "owner": owner,
                "source": source,
                "created_at": datetime.combine(created_date, datetime.min.time()).replace(
                    hour=random.randint(8, 17)
                ).isoformat(),
                "days_in_stage": days_in_stage,
                "lost_at_stage": lost_at_stage,
            }
            self.next_deal_id += 1
            self.all_deals.append(deal)

        self.stats["deals"] = len(self.all_deals)

    # -------------------------------------------------------------------
    # Events — product events for funnel analysis
    # -------------------------------------------------------------------
    def generate_events(self, date_range=None):
        """
        Generate product events. Sampled to keep data size manageable.
        ~10-50 events per company per day when active.
        We only generate a sample (1 in 5 days) to keep under 400MB.
        """
        if date_range is None:
            date_range = (self.start_date, self.end_date)

        total_days = days_between(date_range[0], date_range[1])

        for day_offset in range(total_days):
            current_day = date_range[0] + timedelta(days=day_offset)

            # Sample: only generate events for ~20% of days in full mode, 100% in daily
            if not self.daily_mode and random.random() > 0.20:
                continue

            if is_weekend(current_day):
                continue

            for company in self.companies:
                cid = company["company_id"]
                created = date.fromisoformat(company["created_at"][:10])

                if current_day < created:
                    continue
                if company["status"] == "churned":
                    continue

                health = self.company_health.get(cid, 0.5)
                emp = company["employee_count"]
                size = company_size_bucket(emp)

                # Number of events per day
                base_events = {"small": 8, "medium": 20, "large": 40}[size]
                num_events = max(1, int(base_events * health * random.uniform(0.5, 1.5)))

                # Cap to keep total data manageable
                num_events = min(num_events, 30)

                for _ in range(num_events):
                    event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]
                    hour = random.randint(7, 20)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)

                    # Build properties based on event type
                    props = {}
                    if event_type == "page_view":
                        props["page"] = random.choice(["/dashboard", "/explore", "/reports", "/settings", "/data-sources"])
                    elif event_type == "query_run":
                        props["duration_ms"] = random.randint(100, 30000)
                        props["rows_returned"] = random.randint(0, 100000)
                    elif event_type == "dashboard_viewed":
                        props["dashboard_id"] = f"dash_{random.randint(1, 50)}"
                    elif event_type == "report_exported":
                        props["format"] = random.choice(["pdf", "csv", "xlsx"])

                    # User ID — companies have multiple users
                    max_users = {"small": 5, "medium": 20, "large": 80}[size]
                    user_num = random.randint(1, max(1, int(max_users * health)))

                    event = {
                        "event_id": self.next_event_id,
                        "company_id": cid,
                        "event_type": event_type,
                        "event_timestamp": datetime(
                            current_day.year, current_day.month, current_day.day,
                            hour, minute, second
                        ).isoformat(),
                        "user_id": f"user_{cid}_{user_num}",
                        "properties": json.dumps(props),
                    }
                    self.next_event_id += 1
                    self.all_events.append(event)

        self.stats["events"] = len(self.all_events)

    # -------------------------------------------------------------------
    # NPS Surveys — quarterly surveys
    # -------------------------------------------------------------------
    def generate_nps(self, date_range=None):
        """
        Generate NPS survey responses. Surveys go out quarterly.
        ~40-60% response rate. Scores correlate with health/plan tier.
        """
        if date_range is None:
            date_range = (self.start_date, self.end_date)

        # Survey months: March, June, September, December
        survey_months = [3, 6, 9, 12]

        for company in self.companies:
            cid = company["company_id"]
            created = date.fromisoformat(company["created_at"][:10])
            health = self.company_health.get(cid, 0.5)

            for year in range(date_range[0].year, date_range[1].year + 1):
                for month in survey_months:
                    survey_date = date(year, month, 15)

                    if survey_date < created or survey_date < date_range[0] or survey_date > date_range[1]:
                        continue

                    if company["status"] == "churned":
                        # Check if they churned before this survey
                        churn_date = None
                        for sub in self.all_subscriptions:
                            if sub["company_id"] == cid and sub["status"] == "cancelled" and sub["end_date"]:
                                cd = date.fromisoformat(sub["end_date"])
                                if churn_date is None or cd > churn_date:
                                    churn_date = cd
                        if churn_date and survey_date > churn_date:
                            continue

                    # Response rate ~50%
                    if random.random() > 0.50:
                        continue

                    # NPS score correlates with health
                    # Health 0.8+ → likely promoter (9-10)
                    # Health 0.5-0.8 → likely passive (7-8)
                    # Health <0.5 → likely detractor (0-6)
                    # Map health (0.3–1.0) to score range that produces realistic NPS (+10 to +50)
                    # health=0.8 → ~9.5, health=0.6 → ~8, health=0.4 → ~6.5
                    base_score = 4.5 + health * 7.0  # range ~7.3 to ~11.5 before clamping
                    score = max(0, min(10, round(base_score + random.uniform(-2, 1.5))))

                    if score >= 9:
                        category = "promoter"
                    elif score >= 7:
                        category = "passive"
                    else:
                        category = "detractor"

                    feedback = random.choice(NPS_FEEDBACK[category])

                    survey = {
                        "survey_id": self.next_survey_id,
                        "company_id": cid,
                        "score": score,
                        "response_date": survey_date.isoformat(),
                        "feedback_text": feedback,
                        "category": category,
                    }
                    self.next_survey_id += 1
                    self.all_nps.append(survey)

        self.stats["nps_surveys"] = len(self.all_nps)

    # -------------------------------------------------------------------
    # Write CSVs
    # -------------------------------------------------------------------
    def write_csvs(self):
        """Write all accumulated data to CSV files in the data/ directory."""
        tables = {
            "employees.csv": (self.all_employees, [
                "employee_id", "full_name", "role", "department", "hire_date", "region", "is_active",
            ]),
            "plans.csv": (PLANS, [
                "plan_id", "plan_name", "tier", "monthly_price", "annual_price", "max_users", "features",
            ]),
            "companies.csv": (self.all_companies, [
                "company_id", "company_name", "industry", "employee_count", "region", "country",
                "created_at", "status", "assigned_csm", "assigned_rep",
            ]),
            "subscriptions.csv": (self.all_subscriptions, [
                "subscription_id", "company_id", "plan_id", "start_date", "end_date",
                "mrr", "arr", "billing_cycle", "status",
            ]),
            "invoices.csv": (self.all_invoices, [
                "invoice_id", "subscription_id", "company_id", "amount", "currency",
                "issued_date", "due_date", "paid_date", "status",
            ]),
            "product_usage.csv": (self.all_usage, [
                "usage_id", "company_id", "usage_date", "daily_active_users", "queries_run",
                "dashboards_viewed", "reports_exported", "api_calls", "sessions",
            ]),
            "support_tickets.csv": (self.all_tickets, [
                "ticket_id", "company_id", "created_at", "resolved_at", "category",
                "priority", "status", "csat_score", "assigned_agent",
            ]),
            "deals.csv": (self.all_deals, [
                "deal_id", "company_name", "deal_name", "stage", "amount", "close_date",
                "probability", "owner", "source", "created_at", "days_in_stage", "lost_at_stage",
            ]),
            "events.csv": (self.all_events, [
                "event_id", "company_id", "event_type", "event_timestamp", "user_id", "properties",
            ]),
            "nps_surveys.csv": (self.all_nps, [
                "survey_id", "company_id", "score", "response_date", "feedback_text", "category",
            ]),
        }

        for filename, (rows, headers) in tables.items():
            writer, f = csv_writer(filename, headers)
            for row in rows:
                writer.writerow(row)
            f.close()
            print(f"  {filename}: {len(rows):,} rows")

    # -------------------------------------------------------------------
    # Full generation
    # -------------------------------------------------------------------
    def generate_all(self):
        """Generate all data for the full historical period."""
        print("Generating Northwind Analytics demo data...")
        print(f"  Period: {self.start_date} to {self.end_date}")
        print()

        print("[1/8] Generating employees...")
        self.generate_employees()

        print("[2/8] Generating companies...")
        self.generate_companies()

        print("[3/8] Generating subscriptions...")
        self.generate_subscriptions()

        print("[4/8] Generating invoices...")
        self.generate_invoices()

        # Degrade health for churned companies so usage shows a clear drop signal
        self._apply_churn_health_penalty()

        print("[5/8] Generating product usage (this takes a moment)...")
        self.generate_usage()

        print("[6/8] Generating support tickets...")
        self.generate_tickets()

        print("[7/8] Generating deals...")
        self.generate_deals()

        print("[8/8] Generating NPS surveys...")
        self.generate_nps()

        # Skip events in full mode to keep under size limit; generate sampled events
        print("[bonus] Generating product events (sampled)...")
        self.generate_events()

        print()
        print("Writing CSV files...")
        self.write_csvs()

        self.print_summary()

    def generate_daily(self):
        """Generate only today's incremental data."""
        today = self.end_date
        yesterday = today - timedelta(days=1)
        date_range = (yesterday, today)

        print(f"Generating daily increment for {today}...")

        # We need the base state. Load existing company/subscription state from CSVs
        self._load_existing_state()

        print("  Generating daily usage...")
        self.generate_usage(date_range=date_range)

        print("  Generating daily tickets...")
        self.generate_tickets(date_range=date_range)

        print("  Generating daily deals...")
        self.generate_deals(date_range=date_range)

        print("  Generating daily events...")
        self.generate_events(date_range=date_range)

        # NPS only on survey months
        if today.month in (3, 6, 9, 12) and today.day == 15:
            print("  Generating NPS surveys...")
            self.generate_nps(date_range=date_range)

        print()
        print("Writing daily CSV files...")
        self._write_daily_csvs()

        self.print_summary()

    def _load_existing_state(self):
        """Load company and subscription state from existing CSVs for daily mode."""
        companies_path = os.path.join(DATA_DIR, "companies.csv")
        subs_path = os.path.join(DATA_DIR, "subscriptions.csv")
        employees_path = os.path.join(DATA_DIR, "employees.csv")

        if os.path.exists(employees_path):
            with open(employees_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["employee_id"] = int(row["employee_id"])
                    self.employees.append(row)

        if os.path.exists(companies_path):
            with open(companies_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["company_id"] = int(row["company_id"])
                    row["employee_count"] = int(row["employee_count"])
                    self.companies.append(row)
                    # Reconstruct health scores
                    size = company_size_bucket(row["employee_count"])
                    base_health = {"small": 0.55, "medium": 0.65, "large": 0.75}[size]
                    self.company_health[row["company_id"]] = base_health + random.uniform(-0.1, 0.15)

        if os.path.exists(subs_path):
            with open(subs_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["subscription_id"] = int(row["subscription_id"])
                    row["company_id"] = int(row["company_id"])
                    row["plan_id"] = int(row["plan_id"])
                    self.all_subscriptions.append(row)

        # Set ID counters based on existing data
        if self.companies:
            self.next_company_id = max(c["company_id"] for c in self.companies) + 1
        if self.all_subscriptions:
            self.next_subscription_id = max(s["subscription_id"] for s in self.all_subscriptions) + 1

        # Read max IDs from other CSVs to set counters
        for fname, attr in [
            ("invoices.csv", "next_invoice_id"),
            ("product_usage.csv", "next_usage_id"),
            ("support_tickets.csv", "next_ticket_id"),
            ("deals.csv", "next_deal_id"),
            ("events.csv", "next_event_id"),
            ("nps_surveys.csv", "next_survey_id"),
        ]:
            fpath = os.path.join(DATA_DIR, fname)
            if os.path.exists(fpath):
                max_id = 0
                with open(fpath, "r") as f:
                    reader = csv.DictReader(f)
                    id_col = list(reader.fieldnames)[0]  # first column is always the ID
                    for row in reader:
                        max_id = max(max_id, int(row[id_col]))
                setattr(self, attr, max_id + 1)

    def _write_daily_csvs(self):
        """Write daily increment CSVs (append-style naming)."""
        today = self.end_date.isoformat()
        daily_files = {
            f"product_usage_daily.csv": (self.all_usage, [
                "usage_id", "company_id", "usage_date", "daily_active_users", "queries_run",
                "dashboards_viewed", "reports_exported", "api_calls", "sessions",
            ]),
            f"support_tickets_daily.csv": (self.all_tickets, [
                "ticket_id", "company_id", "created_at", "resolved_at", "category",
                "priority", "status", "csat_score", "assigned_agent",
            ]),
            f"deals_daily.csv": (self.all_deals, [
                "deal_id", "company_name", "deal_name", "stage", "amount", "close_date",
                "probability", "owner", "source", "created_at", "days_in_stage", "lost_at_stage",
            ]),
            f"events_daily.csv": (self.all_events, [
                "event_id", "company_id", "event_type", "event_timestamp", "user_id", "properties",
            ]),
        }

        if self.all_nps:
            daily_files[f"nps_surveys_daily.csv"] = (self.all_nps, [
                "survey_id", "company_id", "score", "response_date", "feedback_text", "category",
            ])

        for filename, (rows, headers) in daily_files.items():
            writer, f = csv_writer(filename, headers)
            for row in rows:
                writer.writerow(row)
            f.close()
            print(f"  {filename}: {len(rows):,} rows")

    def print_summary(self):
        """Print summary statistics."""
        print()
        print("=" * 50)
        print("  GENERATION SUMMARY")
        print("=" * 50)
        for key, val in sorted(self.stats.items()):
            print(f"  {key:.<30} {val:>10,}")
        print("=" * 50)

        # Estimate total CSV size
        total_size = 0
        for fname in os.listdir(DATA_DIR):
            if fname.endswith(".csv"):
                total_size += os.path.getsize(os.path.join(DATA_DIR, fname))
        print(f"  Total CSV size: {total_size / 1024 / 1024:.1f} MB")
        if total_size > 350 * 1024 * 1024:
            print("  ⚠ WARNING: Data is approaching the 400MB Neon free tier limit!")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Northwind Analytics Data Generator")
    parser.add_argument(
        "--mode",
        choices=["full", "daily"],
        default="full",
        help="'full' generates all historical data; 'daily' generates today's increment only",
    )
    args = parser.parse_args()

    gen = NorthwindDataGenerator(
        start_date=START_DATE,
        end_date=END_DATE,
        daily_mode=(args.mode == "daily"),
    )

    if args.mode == "full":
        gen.generate_all()
    else:
        gen.generate_daily()


if __name__ == "__main__":
    main()
