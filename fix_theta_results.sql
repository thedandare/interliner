-- ------------------------------------------------------------------------------
-- File: fix_theta_results.sql
-- Description: SQL script to fix incorrect co_prova values in theta_results table
-- ------------------------------------------------------------------------------

-- First, create a backup of the current theta_results table
CREATE TABLE theta_results_backup AS SELECT * FROM theta_results;

-- Find duplicate student/domain combinations
-- This will help us understand what we're dealing with
SELECT nu_inscricao, domain, COUNT(*) as count
FROM theta_results
GROUP BY nu_inscricao, domain
HAVING COUNT(*) > 1
LIMIT 10;

-- Create a temporary table to consolidate results
CREATE TEMPORARY TABLE corrected_theta_results AS
SELECT 
    t.nu_inscricao,
    CASE 
        WHEN t.domain = 'CH' THEN a.CO_PROVA_CH
        WHEN t.domain = 'CN' THEN a.CO_PROVA_CN
        WHEN t.domain = 'MT' THEN a.CO_PROVA_MT
        WHEN t.domain = 'LC' THEN a.CO_PROVA_LC
    END as correct_co_prova,
    t.domain,
    -- Keep the best theta and se from duplicate entries (using most recent)
    t.theta,
    t.se,
    t.processed_at
FROM theta_results t
JOIN ALUNOS_PRESENTES a ON t.nu_inscricao = a.nu_inscricao
-- Use this to keep only the most recent entry if there are duplicates
WHERE (t.nu_inscricao, t.domain, t.processed_at) IN (
    SELECT nu_inscricao, domain, MAX(processed_at)
    FROM theta_results
    GROUP BY nu_inscricao, domain
);

-- Empty the original table
DELETE FROM theta_results;

-- Reinsert all data with correct co_prova values
INSERT INTO theta_results (nu_inscricao, co_prova, domain, theta, se, processed_at)
SELECT 
    nu_inscricao,
    correct_co_prova as co_prova,
    domain,
    theta,
    se,
    processed_at
FROM corrected_theta_results;

-- Check results of the update for a specific student (example from your query)
SELECT * 
FROM theta_results 
WHERE nu_inscricao = '210059052283';

-- Validate the fixes - this will show a sample of the corrections
SELECT 
  tr.nu_inscricao, 
  tr.domain, 
  tr.co_prova, 
  CASE 
    WHEN tr.domain = 'CH' THEN ap.CO_PROVA_CH 
    WHEN tr.domain = 'CN' THEN ap.CO_PROVA_CN 
    WHEN tr.domain = 'MT' THEN ap.CO_PROVA_MT 
    WHEN tr.domain = 'LC' THEN ap.CO_PROVA_LC 
  END as expected_co_prova,
  CASE 
    WHEN tr.domain = 'CH' AND tr.co_prova = ap.CO_PROVA_CH THEN 'correct' 
    WHEN tr.domain = 'CN' AND tr.co_prova = ap.CO_PROVA_CN THEN 'correct' 
    WHEN tr.domain = 'MT' AND tr.co_prova = ap.CO_PROVA_MT THEN 'correct' 
    WHEN tr.domain = 'LC' AND tr.co_prova = ap.CO_PROVA_LC THEN 'correct' 
    ELSE 'incorrect'
  END as status
FROM theta_results tr
JOIN ALUNOS_PRESENTES ap ON tr.nu_inscricao = ap.nu_inscricao
LIMIT 100;

-- Count any remaining incorrect records
SELECT COUNT(*) as incorrect_count 
FROM theta_results tr
JOIN ALUNOS_PRESENTES ap ON tr.nu_inscricao = ap.nu_inscricao
WHERE (tr.domain = 'CH' AND tr.co_prova != ap.CO_PROVA_CH) OR
      (tr.domain = 'CN' AND tr.co_prova != ap.CO_PROVA_CN) OR
      (tr.domain = 'MT' AND tr.co_prova != ap.CO_PROVA_MT) OR
      (tr.domain = 'LC' AND tr.co_prova != ap.CO_PROVA_LC);

-- Drop the temporary table
DROP TABLE corrected_theta_results;