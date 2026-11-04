---
title: FinGuard Streaming Fraud Risk Analytics
---

## Executive Scorecards

<BigValue data={kpi_overview} value=total_transactions title="Streaming Transactions" fmt="num0" />
<BigValue data={kpi_overview} value=fraud_rate_pct title="Overall Fraud Rate" fmt="pct2" />
<BigValue data={kpi_overview} value=fraud_exposure_amount title="Fraud Exposure ($)" fmt="usd2" />
<BigValue data={top_risk_channel} value=transaction_channel title="Highest Risk Channel" />
<BigValue data={kpi_overview} value=velocity_attack_count title="Velocity Attacks" fmt="num0" />
<BigValue data={quarantine_kpi} value=quarantined_count title="Quarantined Records" fmt="num0" />

---

## Temporal & Velocity Analytics

> Real-time streaming transaction volume, fraud incidents, and velocity attack patterns across hourly windows and observation dates.

<Grid cols=2>
    <BarChart
        data={hourly_activity}
        x=hour_label
        y={['legit_transactions', 'fraud_transactions']}
        type=stacked
        title="HOURLY STREAMING TRAFFIC & FRAUD VOLUME (UTC)"
        yAxisTitle="Transactions"
        yFmt="num0"
        colorPalette={['#4A7FB5', '#C23D2E']}
    />
    <BarChart
        data={fraud_rule_breakdown}
        x=rule_name
        y=incident_count
        swapXY=true
        title="FRAUD INCIDENTS BY DETECTION RULE"
        yFmt="num0"
        fillColor="#C23D2E"
    />
</Grid>

<LineChart
    data={daily_activity}
    x=transaction_date
    y={['total_transactions', 'fraud_transactions', 'velocity_attacks']}
    title="DAILY STREAMING VOLUME VS. FRAUD & VELOCITY INCIDENTS"
    yAxisTitle="Transactions"
    yFmt="num0"
    markers=true
    colorPalette={['#4A7FB5', '#C23D2E', '#D4890A']}
/>

---

## Multi-Dimensional Risk Breakdown

> Fraud rate (%) and financial exposure across payment channels (`POS`, `ATM`, `Online`), card brands (`Visa`, `Mastercard`, `Amex`), countries, and transaction value tiers (`micro` to `critical`).

<Grid cols=2>
    <BarChart
        data={fraud_by_channel}
        x=transaction_channel
        y=fraud_rate_pct
        swapXY=true
        title="FRAUD RATE BY PAYMENT CHANNEL"
        yFmt="pct1"
        fillColor="#4A7FB5"
    />
    <BarChart
        data={fraud_by_card_brand}
        x=card_brand
        y=fraud_rate_pct
        swapXY=true
        title="FRAUD RATE BY CARD BRAND"
        yFmt="pct1"
        fillColor="#C23D2E"
    />
</Grid>

<Grid cols=2>
    <BarChart
        data={fraud_by_country}
        x=transaction_country
        y=fraud_rate_pct
        swapXY=true
        title="FRAUD RATE BY TRANSACTION COUNTRY"
        yFmt="pct1"
        fillColor="#2B6B5E"
    />
    <BarChart
        data={fraud_by_amount_tier}
        x=amount_tier
        y=fraud_rate_pct
        title="FRAUD RATE BY AMOUNT TIER"
        yFmt="pct1"
        fillColor="#D4890A"
        subtitle="Tiers: micro (<$50), low ($50-$100), medium ($100-$500), high ($500-$2,000), critical (>$2,000)"
    />
</Grid>

### Dimensional Risk Summary Tables

<Grid cols=2>
    <DataTable data={fraud_by_channel} title="PAYMENT CHANNEL SUMMARY">
        <Column id=transaction_channel title="Channel" />
        <Column id=total_transactions title="Total Txns" fmt="num0" />
        <Column id=fraud_transactions title="Fraud Txns" fmt="num0" />
        <Column id=fraud_rate_pct title="Fraud Rate" fmt="pct2" />
        <Column id=total_amount title="Total Amt" fmt="usd2" />
        <Column id=fraud_exposure_amount title="Fraud Exposure" fmt="usd2" />
    </DataTable>

    <DataTable data={fraud_by_card_brand} title="CARD BRAND SUMMARY">
        <Column id=card_brand title="Card Brand" />
        <Column id=total_transactions title="Total Txns" fmt="num0" />
        <Column id=fraud_transactions title="Fraud Txns" fmt="num0" />
        <Column id=fraud_rate_pct title="Fraud Rate" fmt="pct2" />
        <Column id=total_amount title="Total Amt" fmt="usd2" />
        <Column id=fraud_exposure_amount title="Fraud Exposure" fmt="usd2" />
    </DataTable>
</Grid>

<Grid cols=2>
    <DataTable data={fraud_by_country} title="COUNTRY RISK SUMMARY">
        <Column id=transaction_country title="Country" />
        <Column id=total_transactions title="Total Txns" fmt="num0" />
        <Column id=fraud_transactions title="Fraud Txns" fmt="num0" />
        <Column id=fraud_rate_pct title="Fraud Rate" fmt="pct2" />
        <Column id=total_amount title="Total Amt" fmt="usd2" />
        <Column id=fraud_exposure_amount title="Fraud Exposure" fmt="usd2" />
    </DataTable>

    <DataTable data={fraud_by_amount_tier} title="AMOUNT TIER SUMMARY">
        <Column id=amount_tier title="Amount Tier" />
        <Column id=total_transactions title="Total Txns" fmt="num0" />
        <Column id=fraud_transactions title="Fraud Txns" fmt="num0" />
        <Column id=fraud_rate_pct title="Fraud Rate" fmt="pct2" />
        <Column id=total_amount title="Total Amt" fmt="usd2" />
        <Column id=fraud_exposure_amount title="Fraud Exposure" fmt="usd2" />
    </DataTable>
</Grid>

---

## Quarantine & Incident Audit Table

### 1. SecOps Velocity & Fraud Incident Audit (`mart_velocity_incidents`)

> Live audit trail of flagged transactions, compromised cards, and automated remediation actions (`AUTO_FREEZE_CARD`, `HOLD_FOR_STEP_UP_AUTH`, `DECLINE_AND_CREATE_CASE`).

<DataTable data={velocity_incidents} search=true rows=15>
    <Column id=transaction_time_utc title="Timestamp (UTC)" />
    <Column id=card_number_masked title="Masked Card" />
    <Column id=card_brand title="Brand" />
    <Column id=customer_name title="Customer" />
    <Column id=transaction_channel title="Channel" />
    <Column id=transaction_country title="Country" />
    <Column id=amount title="Amount" fmt="usd2" />
    <Column id=batch_card_tx_count title="Batch Card Txns" fmt="num0" />
    <Column id=stream_fraud_reason title="Fraud Reason" />
    <Column id=recommended_action title="Remediation Action" />
    <Column id=risk_score title="Risk Score" fmt="num0" />
    <Column id=risk_tier title="Risk Tier" />
</DataTable>

### 2. Quarantined Malformed Records (`stg_quarantined_transactions`)

> Corrupt or schema-violating records automatically isolated from production marts by the dbt Quarantine Router (`amount <= 0`, invalid currency, or missing mandatory keys).

<DataTable data={quarantined_transactions} search=true rows=10>
    <Column id=transaction_time_utc title="Timestamp (UTC)" />
    <Column id=transaction_id title="Transaction ID" />
    <Column id=card_id title="Card ID" />
    <Column id=customer_id title="Customer ID" />
    <Column id=amount title="Raw Amount" fmt="usd2" />
    <Column id=currency title="Currency" />
    <Column id=transaction_channel title="Channel" />
    <Column id=quarantine_reason title="Quarantine Reason" />
    <Column id=recommended_action title="Action" />
</DataTable>

```sql kpi_overview
SELECT
    total_transactions,
    fraud_transactions,
    legit_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount,
    velocity_attack_count,
    high_amount_spike_count,
    geo_mismatch_count,
    compromised_card_count
FROM bigquery.kpi_overview
```

```sql top_risk_channel
SELECT
    transaction_channel,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    fraud_exposure_amount
FROM bigquery.top_risk_channel
```

```sql quarantine_kpi
SELECT
    COUNT(1) AS quarantined_count
FROM bigquery.quarantined_transactions
```

```sql hourly_activity
SELECT
    transaction_hour,
    hour_label,
    total_transactions,
    fraud_transactions,
    legit_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount,
    velocity_attacks
FROM bigquery.hourly_activity
ORDER BY transaction_hour
```

```sql daily_activity
SELECT
    transaction_date,
    total_transactions,
    fraud_transactions,
    legit_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount,
    velocity_attacks
FROM bigquery.daily_activity
ORDER BY transaction_date
```

```sql fraud_rule_breakdown
SELECT 'Velocity Attack' AS rule_name, velocity_attack_count AS incident_count FROM bigquery.kpi_overview
UNION ALL
SELECT 'High Amount Spike' AS rule_name, high_amount_spike_count AS incident_count FROM bigquery.kpi_overview
UNION ALL
SELECT 'Geo Mismatch' AS rule_name, geo_mismatch_count AS incident_count FROM bigquery.kpi_overview
UNION ALL
SELECT 'Compromised Card' AS rule_name, compromised_card_count AS incident_count FROM bigquery.kpi_overview
ORDER BY incident_count DESC
```

```sql fraud_by_channel
SELECT
    transaction_channel,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount
FROM bigquery.fraud_by_channel
ORDER BY fraud_rate_pct DESC
```

```sql fraud_by_card_brand
SELECT
    card_brand,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount
FROM bigquery.fraud_by_card_brand
ORDER BY fraud_rate_pct DESC
```

```sql fraud_by_country
SELECT
    transaction_country,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount
FROM bigquery.fraud_by_country
ORDER BY fraud_rate_pct DESC, fraud_exposure_amount DESC
```

```sql fraud_by_amount_tier
SELECT
    amount_tier,
    tier_order,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    total_amount,
    fraud_exposure_amount
FROM bigquery.fraud_by_amount_tier
ORDER BY tier_order
```

```sql velocity_incidents
SELECT
    transaction_id,
    transaction_time_utc,
    transaction_date,
    card_number_masked,
    card_brand,
    customer_name,
    merchant_name,
    transaction_channel,
    transaction_country,
    amount,
    batch_card_tx_count,
    stream_fraud_reason,
    recommended_action,
    risk_score,
    risk_tier
FROM bigquery.velocity_incidents
```

```sql quarantined_transactions
SELECT
    transaction_id,
    transaction_time_utc,
    transaction_date,
    card_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    transaction_channel,
    transaction_country,
    quarantine_reason,
    recommended_action
FROM bigquery.quarantined_transactions
```

