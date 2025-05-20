-- ------------------------------------------------------------------------------
-- File: fix_theta_results_simple.sql
-- Description: Simple SQL script to fix incorrect co_prova values in theta_results table
-- ------------------------------------------------------------------------------

-- First, create a backup of the current theta_results table
CREATE TABLE IF NOT EXISTS theta_results_backup AS SELECT * FROM theta_results;

-- Create a simple mapping table for each student's domain-specific co_prova values
CREATE TEMPORARY TABLE student_domain_mappings AS
SELECT 
    nu_inscricao,
    CO_PROVA_CH,
    CO_PROVA_CN,
    CO_PROVA_MT,
    CO_PROVA_LC
FROM ALUNOS_PRESENTES;

-- Update each domain separately with simple UPDATE statements
-- Update CH domain
UPDATE theta_results
SET co_prova = m.CO_PROVA_CH
FROM student_domain_mappings m
WHERE theta_results.nu_inscricao = m.nu_inscricao
AND theta_results.domain = 'CH';

-- Update CN domain
UPDATE theta_results
SET co_prova = m.CO_PROVA_CN
FROM student_domain_mappings m
WHERE theta_results.nu_inscricao = m.nu_inscricao
AND theta_results.domain = 'CN';

-- Update MT domain
UPDATE theta_results
SET co_prova = m.CO_PROVA_MT
FROM student_domain_mappings m
WHERE theta_results.nu_inscricao = m.nu_inscricao
AND theta_results.domain = 'MT';

-- Update LC domain
UPDATE theta_results
SET co_prova = m.CO_PROVA_LC
FROM student_domain_mappings m
WHERE theta_results.nu_inscricao = m.nu_inscricao
AND theta_results.domain = 'LC';

-- Check results of the update for a specific student (example from your query)
SELECT * 
FROM theta_results 
WHERE nu_inscricao = '210059052283'
ORDER BY domain;

-- Validate the fixes - this will show a sample of the corrections
SELECT 
  tr.nu_inscricao, 
  tr.domain, 
  tr.co_prova, 
  CASE 
    WHEN tr.domain = 'CH' THEN m.CO_PROVA_CH 
    WHEN tr.domain = 'CN' THEN m.CO_PROVA_CN 
    WHEN tr.domain = 'MT' THEN m.CO_PROVA_MT 
    WHEN tr.domain = 'LC' THEN m.CO_PROVA_LC 
  END as expected_co_prova,
  CASE 
    WHEN tr.domain = 'CH' AND tr.co_prova = m.CO_PROVA_CH THEN 'correct' 
    WHEN tr.domain = 'CN' AND tr.co_prova = m.CO_PROVA_CN THEN 'correct' 
    WHEN tr.domain = 'MT' AND tr.co_prova = m.CO_PROVA_MT THEN 'correct' 
    WHEN tr.domain = 'LC' AND tr.co_prova = m.CO_PROVA_LC THEN 'correct' 
    ELSE 'incorrect'
  END as status
FROM theta_results tr
JOIN student_domain_mappings m ON tr.nu_inscricao = m.nu_inscricao
LIMIT 100;

-- Count any remaining incorrect records
SELECT COUNT(*) as incorrect_count 
FROM theta_results tr
JOIN student_domain_mappings m ON tr.nu_inscricao = m.nu_inscricao
WHERE (tr.domain = 'CH' AND tr.co_prova != m.CO_PROVA_CH) OR
      (tr.domain = 'CN' AND tr.co_prova != m.CO_PROVA_CN) OR
      (tr.domain = 'MT' AND tr.co_prova != m.CO_PROVA_MT) OR
      (tr.domain = 'LC' AND tr.co_prova != m.CO_PROVA_LC);

-- Drop the temporary table
DROP TABLE student_domain_mappings;
