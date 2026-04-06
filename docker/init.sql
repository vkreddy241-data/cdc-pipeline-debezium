-- PostgreSQL init script: creates source tables and seeds sample data
-- Runs automatically on first docker-compose up

CREATE TABLE IF NOT EXISTS claims (
    claim_id        VARCHAR(50) PRIMARY KEY,
    member_id       VARCHAR(50) NOT NULL,
    provider_npi    VARCHAR(20),
    service_date    DATE,
    discharge_date  DATE,
    diagnosis_code  VARCHAR(20),
    procedure_code  VARCHAR(20),
    billed_amount   NUMERIC(18,2),
    allowed_amount  NUMERIC(18,2),
    paid_amount     NUMERIC(18,2),
    claim_status    VARCHAR(20) DEFAULT 'PENDING',
    claim_type      VARCHAR(20),
    plan_id         VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS members (
    member_id       VARCHAR(50) PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    dob             DATE,
    gender          VARCHAR(10),
    state           VARCHAR(5),
    plan_id         VARCHAR(50),
    effective_date  DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS providers (
    provider_npi    VARCHAR(20) PRIMARY KEY,
    provider_name   VARCHAR(200),
    specialty       VARCHAR(100),
    network_flag    VARCHAR(5) DEFAULT 'IN',
    state           VARCHAR(5),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Seed data
INSERT INTO members VALUES
    ('M001','John','Smith','1980-05-10','M','TX','PLAN_A','2024-01-01',NOW(),NOW()),
    ('M002','Jane','Doe','1975-11-22','F','TX','PLAN_B','2024-01-01',NOW(),NOW()),
    ('M003','Bob','Johnson','1990-03-15','M','CA','PLAN_A','2024-01-01',NOW(),NOW())
ON CONFLICT DO NOTHING;

INSERT INTO providers VALUES
    ('NPI001','Dallas Medical Center','General Practice','IN','TX',NOW(),NOW()),
    ('NPI002','TX Pharmacy Group','Pharmacy','IN','TX',NOW(),NOW()),
    ('NPI003','Out-of-State Clinic','Cardiology','OUT','CA',NOW(),NOW())
ON CONFLICT DO NOTHING;

INSERT INTO claims VALUES
    ('C001','M001','NPI001','2024-01-10','2024-01-12','Z00.00','99213',1200.00,1000.00,800.00,'APPROVED','medical','PLAN_A',NOW(),NOW()),
    ('C002','M002','NPI002','2024-01-11',NULL,'Z79.899','90714',150.00,120.00,0.00,'DENIED','pharmacy','PLAN_B',NOW(),NOW()),
    ('C003','M003','NPI003','2024-01-12',NULL,'I10','93000',5000.00,3000.00,2800.00,'APPROVED','medical','PLAN_A',NOW(),NOW())
ON CONFLICT DO NOTHING;

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER claims_updated_at   BEFORE UPDATE ON claims   FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE OR REPLACE TRIGGER members_updated_at  BEFORE UPDATE ON members  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE OR REPLACE TRIGGER providers_updated_at BEFORE UPDATE ON providers FOR EACH ROW EXECUTE FUNCTION update_updated_at();
