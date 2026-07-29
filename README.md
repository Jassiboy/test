
====SLIDE 2=====================================================
Feature it brings:-
table_id is generated with combination of target_table and it's load process, it basically help to distinct process if it is loading up to same target table		
source_tables = all source table info					
incremental_cutoff_day:- so currently we are following 9 th of the month to do the truncate and load in incremental process, but it flexiable in nature and can be chnages for different table depend on the need.		
watermark column:- watercolumn is good to have for incremental process, for futture nedd			
DQ Rule:- A multiple DQ rule can be set [{"dq_rule": "dq_net_rev", "error_threshold_pct": 0.0}] so that whenever data I sloaded from source it can validateed for each
DQ-sttus:-  for tuning it on//off as per the need


====SLIDE 3=====================================================
A dedicated log table for audit and monitoring puprose, some of the important column featuers is hsown here:-

Feature:- 
1. FOr each table_process detail ata granular level.
2. Status - SUCCESS/PARTIAL_SUCCESS/FAILED (partial_success, if some month loaded some failed so that we do not need to load successful months again)
3.Dq _results for each proceess
4. Excutipn summary-- Each months log details


=========SLIDE4=============QA and perfomance:- 

Performance
============================
Loaded 3 months of data:-
acct to fac--- >202501 to 202503---- took 19 minutes ~~ 6-8 per month (Ground +smartpost)

Traditional MKVW_loadting from acc_to fac  ---takes 
FXG:  20 -30 mins per month
FXG (smartpost):  15 - 20  mins per month

From MKVW FACT_to AZURE fact(ADF/abinitio)
30 min per months

STRESS TESTING:- 
loaded 3 year of data ---
acct to fact -- 1hr 28min (Ground +smartpost)
From MKVW FACT_to AZURE fact(ADF/abinitio) == 40 hr

==============================
QA:- 

All thekpis for the tbale has been validated

however 

