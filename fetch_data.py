fetch_query = """
        WITH
        alert_cases AS (
            SELECT 
            DISTINCT an.user_id,
            an.created_at,
            CASE 
                WHEN an.analyst_id = 8423054 THEN 'ch_alert [BR]'
                WHEN an.analyst_id = 8832903 THEN 'pep_pix_alert [BR]'
                WHEN an.analyst_id = 15858378 THEN 'gafi_alert [BR]'
                WHEN an.analyst_id = 16368511 THEN 'pix_merchant_alert [BR]'
                WHEN an.analyst_id = 18758930 THEN 'international_cards_alert [BR]'
                WHEN an.analyst_id = 19897830 THEN 'bank_slips_alert [BR]'
                WHEN an.analyst_id = 20583019 THEN 'goverment_corporate_cards_alert [BR]'
                WHEN an.analyst_id = 20698248 THEN 'betting_houses_alert [BR]'
                WHEN an.analyst_id = 25071066 THEN 'gafi_alert [US]'
                WHEN an.analyst_id = 25261377 THEN 'international_cards_alert [US]'
                WHEN an.analyst_id = 24954170 THEN 'ted_transfers_alert [BR]'
            END AS alert_type
            FROM `infinitepay-production.maindb.offense_analyses` an
            JOIN `infinitepay-production.maindb.users` u ON u.id = an.analyst_id
            JOIN `infinitepay-production.maindb.offenses` o ON o.id = an.offense_id
            LEFT JOIN `infinitepay-production.maindb.offense_actions` act ON act.offense_analysis_id = an.id
            WHERE an.analyst_id IN (8423054, 8832903, 15858378, 16368511, 18758930, 19897830, 20583019, 20698248, 25071066, 25261377, 24954170)
            AND an.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
        )

        SELECT
        user_id,
        FORMAT_DATETIME('%d-%m-%Y', created_at) AS date,
        alert_type
        FROM alert_cases
        ORDER BY created_at ASC, alert_type
    """

test_query = """
        WITH
    alert_cases AS (
        SELECT 
            DISTINCT an.user_id,
            an.created_at,
            'Pep_Pix Alert' AS alert_type
        FROM `infinitepay-production.maindb.offense_analyses` an
        JOIN `infinitepay-production.maindb.users` u ON u.id = an.analyst_id
        JOIN `infinitepay-production.maindb.offenses` o ON o.id = an.offense_id
        LEFT JOIN `infinitepay-production.maindb.offense_actions` act ON act.offense_analysis_id = an.id
        WHERE an.analyst_id = 8832903
          AND an.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 DAY)
    )

    SELECT
        user_id,
        FORMAT_DATETIME('%d-%m-%Y', DATETIME(created_at)) AS date,
        alert_type
    FROM alert_cases
    ORDER BY created_at DESC
"""

fetch_combined_query = """
WITH
    traditional_alerts AS (
        SELECT 
            DISTINCT an.user_id,
            FORMAT_TIMESTAMP('%d-%m-%Y', an.created_at) AS alert_date,
            CASE 
                WHEN an.analyst_id = 8423054 THEN 'ch_alert [BR]' 
                WHEN an.analyst_id = 8832903 THEN 'pep_pix_alert [BR]'
                WHEN an.analyst_id = 15858378 THEN 'gafi_alert [US]'
                WHEN an.analyst_id = 16368511 THEN 'pix_merchant_alert [BR]'
                WHEN an.analyst_id = 18758930 THEN 'international_cards_alert [BR]'
                WHEN an.analyst_id = 19897830 THEN 'bank_slips_alert [BR]'
                WHEN an.analyst_id = 20583019 THEN 'goverment_corporate_cards_alert [BR]'
                WHEN an.analyst_id = 20698248 THEN 'betting_houses_alert [BR]'
                WHEN an.analyst_id = 25769012 THEN 'Issuing Transactions Alert'
            END AS alert_type,
            CAST(NULL AS FLOAT64) AS score,
            CAST(NULL AS STRING) AS features
        FROM `infinitepay-production.maindb.offense_analyses` an
        JOIN `infinitepay-production.maindb.users` u ON u.id = an.analyst_id
        JOIN `infinitepay-production.maindb.offenses` o ON o.id = an.offense_id
        LEFT JOIN `infinitepay-production.maindb.offense_actions` act ON act.offense_analysis_id = an.id
        WHERE an.analyst_id IN (
            8423054, 8832903, 15858378, 16368511, 18758930, 
            19897830, 20583019, 20698248, 25769012
        )
        AND an.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    ),
    ai_alerts AS (
        SELECT
            user_id,
            FORMAT_TIMESTAMP('%d-%m-%Y', TIMESTAMP(timestamp)) AS alert_date,
            'AI Alert' AS alert_type,
            score,
            features
        FROM `ai-services-sae.aml_model.predictions`
        WHERE TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        AND label = 1
    )

SELECT * FROM traditional_alerts
UNION ALL
SELECT * FROM ai_alerts
ORDER BY alert_date DESC;
"""