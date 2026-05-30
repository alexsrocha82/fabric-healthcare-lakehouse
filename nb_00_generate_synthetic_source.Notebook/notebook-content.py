# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "96a64e5c-e76a-41e7-86fc-aad44f7f95fc",
# META       "default_lakehouse_name": "lh_healthcare",
# META       "default_lakehouse_workspace_id": "41842032-fc55-48e8-82d5-cb6347b22db0",
# META       "known_lakehouses": [
# META         {
# META           "id": "96a64e5c-e76a-41e7-86fc-aad44f7f95fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install faker -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
Synthetic Healthcare Source Data Generator (CDC-style)
======================================================
Simulates a daily extraction from an operational (OLTP) healthcare source
system, landing change files into the Lakehouse `Files` area.

How it works
------------
* Per entity, per day, it emits ONLY the rows that changed that day, tagged
  with a column `op` in {I, U, D} (Insert / Update / Delete) and a watermark
  `_extracted_at`. This mirrors a real CDC feed (Debezium, SQL CDC, etc.).
* It persists the *current full snapshot* of each entity to a private state
  folder, so updates/deletes on later days hit records that already exist.
  This is what makes downstream SCD2 realistic instead of synthetic.
* PII fields (full_name, ssn, email, phone, birth_date, address) are emitted
  in clear text ON PURPOSE. Protecting them is the job of the Silver layer
  (hashing/pseudonymization) and the serving Warehouse (RLS/CLS/DDM).

Output layout (Lakehouse Files):
    Files/landing/<entity>/load_date=YYYY-MM-DD/<entity>.csv   <- medallion input
    Files/_generator_state/<entity>.parquet                    <- private state

Entities: departments, insurance_plans, providers, patients, encounters

Usage (in a Fabric notebook with lh_healthcare attached as default):
    %pip install faker -q          # cell 1
    # paste this module            # cell 2
    reset_simulation()             # cell 3 (only if you want a clean start)
    run_simulation("2024-01-01", num_days=14)
"""

import os
import shutil
import hashlib
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
LAKEHOUSE_ROOT = "/lakehouse/default/Files"
LANDING_ROOT = f"{LAKEHOUSE_ROOT}/landing"
STATE_ROOT = f"{LAKEHOUSE_ROOT}/_generator_state"

BASE_SEED = 42
LOCALE = "en_US"

# Initial population (created on the first simulated day)
N_PATIENTS_INITIAL = 500
N_PROVIDERS_INITIAL = 40
N_ENCOUNTERS_DAY1 = 200

# Daily churn ranges (min, max) for days after the first
DAILY_NEW_PATIENTS = (20, 40)
DAILY_UPDATED_PATIENTS = (10, 25)
DAILY_DELETED_PATIENTS = (0, 3)
DAILY_NEW_PROVIDERS = (0, 1)
DAILY_UPDATED_PROVIDERS = (0, 2)
DAILY_NEW_ENCOUNTERS = (80, 150)
DAILY_UPDATED_ENCOUNTERS = (10, 30)

INSURANCE_PLANS = ["Medicare", "Medicaid", "Blue Cross Blue Shield", "Aetna", "UnitedHealthcare", "Self-Pay"]

# (department_name, clinic_location, region) -> region drives RLS later
DEPARTMENTS = [
    ("Cardiology", "New York - Manhattan Clinic", "Northeast"),
    ("Pediatrics", "Chicago - Downtown Clinic", "Midwest"),
    ("Orthopedics", "Houston - Medical Center", "South"),
    ("Neurology", "Los Angeles - Westside Clinic", "West"),
    ("Oncology", "Boston - Longwood Clinic", "Northeast"),
    ("Emergency", "Phoenix - Central Clinic", "West"),
    ("General Practice", "Atlanta - Midtown Clinic", "South"),
    ("Radiology", "Seattle - Lakeview Clinic", "West"),
]

SPECIALTIES = {
    "Cardiology": "Cardiologist",
    "Pediatrics": "Pediatrician",
    "Orthopedics": "Orthopedic Surgeon",
    "Neurology": "Neurologist",
    "Oncology": "Oncologist",
    "Emergency": "Emergency Physician",
    "General Practice": "General Practitioner",
    "Radiology": "Radiologist",
}

ICD10 = [
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("I10", "Essential hypertension"),
    ("J45.909", "Unspecified asthma"),
    ("M54.5", "Low back pain"),
    ("F41.1", "Generalized anxiety disorder"),
    ("J06.9", "Acute upper respiratory infection"),
    ("K21.9", "Gastro-esophageal reflux disease"),
    ("N39.0", "Urinary tract infection"),
    ("E78.5", "Hyperlipidemia, unspecified"),
    ("R51.9", "Headache"),
    ("J11.1", "Influenza with respiratory manifestations"),
    ("E66.9", "Obesity, unspecified"),
]

ENCOUNTER_TYPES = ["consultation", "emergency", "follow_up", "telemedicine", "exam"]
PROCEDURES = ["CONS-01", "LAB-02", "IMG-03", "PROC-04", "VAC-05"]


# ----------------------------------------------------------------------
# Low-level helpers
# ----------------------------------------------------------------------
def _ensure_dirs():
    os.makedirs(LANDING_ROOT, exist_ok=True)
    os.makedirs(STATE_ROOT, exist_ok=True)


def _stable_seed(*parts):
    """Deterministic seed independent of PYTHONHASHSEED (reproducible runs)."""
    key = "-".join(str(p) for p in parts)
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return BASE_SEED + int(digest[:8], 16)


def _make_rng(load_date_str):
    seed = _stable_seed(load_date_str, "day")
    rnd = random.Random(seed)
    fake = Faker(LOCALE)
    fake.seed_instance(seed)
    return rnd, fake


def _state_path(entity):
    return f"{STATE_ROOT}/{entity}.parquet"


def _load_records(entity):
    path = _state_path(entity)
    if os.path.exists(path):
        return pd.read_parquet(path).to_dict("records")
    return []


def _save_records(entity, records):
    pd.DataFrame(records).to_parquet(_state_path(entity), index=False)


def _write_landing(entity, load_date_str, rows):
    """Write a CDC change file. `rows` is a list of dicts already carrying `op`."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df["_extracted_at"] = f"{load_date_str} 23:59:59"
    # Put op first and _extracted_at last for readability
    cols = ["op"] + [c for c in df.columns if c not in ("op", "_extracted_at")] + ["_extracted_at"]
    df = df[cols]
    out_dir = f"{LANDING_ROOT}/{entity}/load_date={load_date_str}"
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(f"{out_dir}/{entity}.csv", index=False, encoding="utf-8")
    return len(df)


def _next_seq(records, key, prefix):
    if not records:
        return 1
    return max(int(r[key].replace(prefix, "")) for r in records) + 1


# ----------------------------------------------------------------------
# Record builders
# ----------------------------------------------------------------------
def _make_department(seq, name, loc, region, ts):
    return {
        "department_id": f"DEP{seq:02d}",
        "department_name": name,
        "clinic_location": loc,
        "region": region,
        "is_active": True,
        "source_created_at": ts,
        "source_updated_at": ts,
    }


def _make_insurance(seq, name, ts):
    return {
        "insurance_id": f"INS{seq:02d}",
        "insurance_name": name,
        "is_active": True,
        "source_created_at": ts,
        "source_updated_at": ts,
    }


def _make_provider(seq, fake, rnd, dept_ids, ts):
    dept_id, dept_name = rnd.choice(dept_ids)
    return {
        "provider_id": f"PRV{seq:04d}",
        "full_name": f"Dr. {fake.first_name()} {fake.last_name()}",     # PII
        "specialty": SPECIALTIES.get(dept_name, "General Practitioner"),
        "department_id": dept_id,
        "license_number": f"MD-{rnd.randint(100000, 999999)}",          # PII
        "hire_date": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
        "is_active": True,
        "source_created_at": ts,
        "source_updated_at": ts,
    }


def _make_patient(seq, fake, rnd, provider_ids, ts):
    first, last = fake.first_name(), fake.last_name()
    return {
        "patient_id": f"PAT{seq:06d}",
        "full_name": f"{first} {last}",                                 # PII
        "ssn": fake.ssn(),                                             # PII
        "birth_date": fake.date_of_birth(minimum_age=0, maximum_age=95).isoformat(),
        "gender": rnd.choice(["F", "M"]),
        "email": f"{first}.{last}@example.com".lower(),                # PII
        "phone": fake.msisdn()[-10:],                                 # PII
        "address_street": fake.street_address(),                      # PII
        "address_city": fake.city(),
        "address_state": fake.state_abbr(),
        "address_zip": fake.zipcode(),
        "insurance_plan": rnd.choice(INSURANCE_PLANS),
        "primary_provider_id": rnd.choice(provider_ids) if provider_ids else None,
        "is_active": True,
        "source_created_at": ts,
        "source_updated_at": ts,
    }


def _make_encounter(seq, fake, rnd, patient_ids, providers, ts, load_date_str):
    provider = rnd.choice(providers)
    icd_code, icd_desc = rnd.choice(ICD10)
    status = rnd.choices(["scheduled", "completed"], weights=[0.45, 0.55])[0]
    return {
        "encounter_id": f"ENC{seq:08d}",
        "patient_id": rnd.choice(patient_ids),
        "provider_id": provider["provider_id"],
        "department_id": provider["department_id"],
        "encounter_date": load_date_str,
        "encounter_type": rnd.choice(ENCOUNTER_TYPES),
        "status": status,
        "diagnosis_code": icd_code,
        "diagnosis_desc": icd_desc,
        "procedure_code": rnd.choice(PROCEDURES),
        "total_amount": round(rnd.uniform(50, 2000), 2),
        "source_created_at": ts,
        "source_updated_at": ts,
    }


# ----------------------------------------------------------------------
# Daily generation
# ----------------------------------------------------------------------
def generate_day(load_date_str, day_index):
    _ensure_dirs()
    rnd, fake = _make_rng(load_date_str)
    ts = f"{load_date_str} 23:59:00"
    is_first = day_index == 0
    summary = {}

    # ---- Static dimensions: departments + insurance_plans (day 1 only) ----
    if is_first:
        dept_records = [
            _make_department(i, name, loc, region, ts)
            for i, (name, loc, region) in enumerate(DEPARTMENTS, start=1)
        ]
        _save_records("departments", dept_records)
        summary["departments"] = _write_landing(
            "departments", load_date_str,
            [{**r, "op": "I"} for r in dept_records],
        )

        ins_records = [_make_insurance(i, name, ts) for i, name in enumerate(INSURANCE_PLANS, start=1)]
        _save_records("insurance_plans", ins_records)
        summary["insurance_plans"] = _write_landing(
            "insurance_plans", load_date_str,
            [{**r, "op": "I"} for r in ins_records],
        )

    dept_records = _load_records("departments")
    dept_ids = [(d["department_id"], d["department_name"]) for d in dept_records]

    # ---- Providers ----
    providers = _load_records("providers")
    prov_changes = []
    if is_first:
        seq = _next_seq(providers, "provider_id", "PRV")
        for _ in range(N_PROVIDERS_INITIAL):
            rec = _make_provider(seq, fake, rnd, dept_ids, ts)
            providers.append(rec)
            prov_changes.append({**rec, "op": "I"})
            seq += 1
    else:
        seq = _next_seq(providers, "provider_id", "PRV")
        for _ in range(rnd.randint(*DAILY_NEW_PROVIDERS)):
            rec = _make_provider(seq, fake, rnd, dept_ids, ts)
            providers.append(rec)
            prov_changes.append({**rec, "op": "I"})
            seq += 1
        # Updates: a provider transfers department (an SCD2 trigger downstream)
        active = [p for p in providers if p["is_active"]]
        for prov in rnd.sample(active, min(rnd.randint(*DAILY_UPDATED_PROVIDERS), len(active))):
            new_dept_id, new_dept_name = rnd.choice(dept_ids)
            prov["department_id"] = new_dept_id
            prov["specialty"] = SPECIALTIES.get(new_dept_name, prov["specialty"])
            prov["source_updated_at"] = ts
            prov_changes.append({**prov, "op": "U"})
    _save_records("providers", providers)
    summary["providers"] = _write_landing("providers", load_date_str, prov_changes)
    provider_ids = [p["provider_id"] for p in providers if p["is_active"]]

    # ---- Patients ----
    patients = _load_records("patients")
    pat_changes = []
    if is_first:
        seq = _next_seq(patients, "patient_id", "PAT")
        for _ in range(N_PATIENTS_INITIAL):
            rec = _make_patient(seq, fake, rnd, provider_ids, ts)
            patients.append(rec)
            pat_changes.append({**rec, "op": "I"})
            seq += 1
    else:
        seq = _next_seq(patients, "patient_id", "PAT")
        for _ in range(rnd.randint(*DAILY_NEW_PATIENTS)):
            rec = _make_patient(seq, fake, rnd, provider_ids, ts)
            patients.append(rec)
            pat_changes.append({**rec, "op": "I"})
            seq += 1
        # Updates: address / insurance / phone changes -> SCD2 triggers
        active = [p for p in patients if p["is_active"]]
        for pat in rnd.sample(active, min(rnd.randint(*DAILY_UPDATED_PATIENTS), len(active))):
            field = rnd.choice(["address", "insurance_plan", "phone", "primary_provider_id"])
            if field == "address":
                pat["address_street"] = fake.street_address()
                pat["address_city"] = fake.city()
                pat["address_state"] = fake.state_abbr()
                pat["address_zip"] = fake.zipcode()
            elif field == "insurance_plan":
                pat["insurance_plan"] = rnd.choice(INSURANCE_PLANS)
            elif field == "phone":
                pat["phone"] = fake.msisdn()[-10:]
            elif field == "primary_provider_id" and provider_ids:
                pat["primary_provider_id"] = rnd.choice(provider_ids)
            pat["source_updated_at"] = ts
            pat_changes.append({**pat, "op": "U"})
        # Soft deletes (patient deactivated / merged)
        active = [p for p in patients if p["is_active"]]
        for pat in rnd.sample(active, min(rnd.randint(*DAILY_DELETED_PATIENTS), len(active))):
            pat["is_active"] = False
            pat["source_updated_at"] = ts
            pat_changes.append({**pat, "op": "D"})
    _save_records("patients", patients)
    summary["patients"] = _write_landing("patients", load_date_str, pat_changes)
    patient_ids = [p["patient_id"] for p in patients if p["is_active"]]

    # ---- Encounters (fact) ----
    encounters = _load_records("encounters")
    enc_changes = []
    active_providers = [p for p in providers if p["is_active"]]
    n_new = N_ENCOUNTERS_DAY1 if is_first else rnd.randint(*DAILY_NEW_ENCOUNTERS)
    seq = _next_seq(encounters, "encounter_id", "ENC")
    for _ in range(n_new):
        rec = _make_encounter(seq, fake, rnd, patient_ids, active_providers, ts, load_date_str)
        encounters.append(rec)
        enc_changes.append({**rec, "op": "I"})
        seq += 1
    if not is_first:
        # Status updates: scheduled encounters get resolved
        scheduled = [e for e in encounters if e["status"] == "scheduled"]
        for enc in rnd.sample(scheduled, min(rnd.randint(*DAILY_UPDATED_ENCOUNTERS), len(scheduled))):
            enc["status"] = rnd.choice(["completed", "cancelled", "no_show"])
            enc["source_updated_at"] = ts
            enc_changes.append({**enc, "op": "U"})
    _save_records("encounters", encounters)
    summary["encounters"] = _write_landing("encounters", load_date_str, enc_changes)

    return summary


# ----------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------
def reset_simulation():
    """Wipe landing + generator state for a clean re-run."""
    for path in (LANDING_ROOT, STATE_ROOT):
        if os.path.exists(path):
            shutil.rmtree(path)
    _ensure_dirs()
    print("Reset done: landing and generator state cleared.")


def run_simulation(start_date, num_days=14):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    print(f"Generating {num_days} day(s) of synthetic source data from {start_date}\n")
    for i in range(num_days):
        d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
        s = generate_day(d, i)
        changed = ", ".join(f"{k}={v}" for k, v in s.items() if v)
        print(f"  {d}: {changed if changed else 'no changes'}")
    print("\nDone. Inspect Files/landing/ in the Lakehouse explorer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reset_simulation()
run_simulation("2024-01-01", num_days=14)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
