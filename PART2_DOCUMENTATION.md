# Part 2: Business Questions & Results

## Question 1: What is the acceptance rate over time?

**SQL Model**: `part_2_question_1_acceptance_rate_by_month.sql`

### Results Summary
- **Date Range**: June 1-30, 2019 (last 30 days)
- **Acceptance Rate Range**: 60.00% - 83.33%
- **Average Acceptance Rate**: ~71%
- **Trend**: Stable with daily fluctuations

### Sample Results
| Date | Total | Accepted | Declined | Rate |
|------|-------|----------|----------|------|
| 2019-06-30 | 30 | 19 | 11 | 63.33% |
| 2019-06-28 | 30 | 25 | 5 | 83.33% |
| 2019-06-27 | 30 | 18 | 12 | 60.00% |
| 2019-06-26 | 30 | 18 | 12 | 60.00% |

### Interpretation
Payment acceptance is **stable and consistent** across June 2019. Daily rates fluctuate between 60-83% with no clear upward or downward trend, suggesting predictable and reliable payment processing from Globepay.

---

## Question 2: List the countries where declined transactions went over $25M

**SQL Model**: `part_2_question_2_countries_high_declined.sql`

### Results Summary
**4 countries exceeded $25M in declined transaction amounts**

| Country | Amount USD | Transaction Count | Avg Amount |
|---------|-----------|-------------------|-----------|
| France (FR) | $32,628,786.68 | 271 | $120,401.43 |
| United Kingdom (UK) | $27,489,495.64 | 258 | $106,548.43 |
| United Arab Emirates (AE) | $26,335,152.43 | 291 | $90,498.81 |
| United States (US) | $25,125,669.78 | 297 | $84,598.21 |

### Interpretation
**Europe and Middle East dominate declined transactions**. France has the highest absolute declined amount ($32.6M), while the US has the most transaction count (297). Average transaction sizes correlate with decline amounts:
- Larger transactions (FR, UK) = higher total declines
- Smaller transactions (US, AE) = lower total declines despite higher counts

This suggests payment size and geography influence decline rates, potentially worth investigating further for fraud patterns or payment method issues.

---

## Question 3: Which transactions are missing chargeback data?

**SQL Model**: `part_2_question_3_transactions_missing_chargebacks.sql`

### Results Summary
**95.9% of transactions are missing chargeback data**

| Status | Count | Percentage |
|--------|-------|-----------|
| Missing Chargeback Data | 5,207 | 95.89% |
| Has Chargeback | 223 | 4.11% |

### Interpretation
The **vast majority of transactions (5,207 out of 5,430) have no associated chargeback record**. This indicates:

1. **Low chargeback rate**: Only 4.1% of transactions result in chargebacks
2. **Healthy payment processing**: Globepay's transaction success rate is high
3. **Data lag consideration**: Some chargebacks may not yet be recorded (depends on chargeback reporting lag from Globepay)
4. **Customer satisfaction proxy**: Low chargeback rate suggests positive customer experience

---
