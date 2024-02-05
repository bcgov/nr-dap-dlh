{{ config(materialized='table',
		  unique_key = ['src_sys_code','permit_id','app_file_id']
		) 
}}
with ats_data as (
  Select 
  'ATS'	as src_sys_code
  ,CASE
                    WHEN ats.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'EMLI'
                    WHEN ats.authorization_instrument_id = 27 THEN 'FOR'
                    WHEN ats.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'AGR'
                    WHEN ats.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342, 21, 107]) THEN 'ENV'
                    ELSE NULL
                END	AS ministry_code
  ,CASE
   WHEN ats.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'MIN'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[69, 542, 1481, 1121, 1124, 1125, 1122, 421, 68, 541, 70, 1, 71, 124]) THEN 'WAT'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[181, 73, 701, 702, 33, 704, 503, 82, 482, 710, 709, 1241, 481, 483, 502, 716, 708, 80, 39, 686, 685, 132, 84, 711, 684, 41, 110, 29, 714, 102, 105, 717, 521, 524, 103, 501, 718, 18, 720, 281, 89, 25, 24, 3, 4, 683, 713]) THEN 'LAN'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[1101, 1402, 1401, 57, 44, 1201, 45, 48, 851, 50, 941, 99, 51, 852, 58, 981, 52, 1081, 1083, 1082, 56, 1001, 401, 65, 881, 861, 846, 841, 843, 842, 847, 845, 441, 921, 11, 66, 100, 72]) THEN 'FAW'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'AGRI'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[761, 341, 901, 27, 762, 10, 1181, 113, 1022, 1021, 119, 126, 28, 30, 1023, 1261, 641, 16, 1041, 721]) THEN 'FOR'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342]) THEN 'PAR'
   WHEN ats.authorization_instrument_id = ANY (ARRAY[21, 107]) THEN 'RECS'
   ELSE NULL
   END as business_area_code
  ,ats.authorization_id 				as permit_id
  ,ats.file_number 						as app_file_id
  ,ats.authorization_status_code 		as permit_status_code
  ,ats.application_accepted_date 		as permit_status_date
  ,ats.adjudication_date 				as permit_issue_date
  ,null::date 							as permit_issue_expire_date
  ,ats.application_received_date 		as app_received_date
  ,coalesce(ats.fcbc_process_complete_date, ats.adjudication_date,ats.application_accepted_date,ats.application_accepted_date )			  as app_update_date
  ,ats.fcbc_process_complete_date 		as app_decision_date
  ,ats.adjudication_date 				as app_issuance_date
  ,ats.application_accepted_date 		as app_accepted_date
  ,null::date 							as app_rejected_date
  ,ats.adjudication_date 				as app_adjudication_date
  ,null 								as harvest_auth_status
  ,null::int 							as harvest_area_sq_m
  ,null 								as permit_org_unit_code
  ,floor(
          CASE
              WHEN ats.adjudication_date IS NULL THEN EXTRACT(epoch FROM CURRENT_DATE::timestamp without time zone - ats.application_received_date) / 86400
              ELSE EXTRACT(epoch FROM ats.adjudication_date - ats.application_received_date) / 86400
          END) 							AS application_age
  ,null 								as map_feature_id
  ,ats.authorization_instrument_id 		as permit_type_code
from fdw_ods_ats_replication.ats_authorizations ats
LEFT JOIN fdw_ods_ats_replication.ats_authorization_status_codes aasc
  ON(ats.authorization_status_code = aasc.authorization_status_code)
LEFT JOIN fdw_ods_ats_replication.ats_authorization_instruments aai
  ON(ats.authorization_instrument_id = aai.authorization_instrument_id)
where 1=1
AND aasc.authorization_status_code <> '2'
AND (ats.authorization_instrument_id = 
ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061, 69, 542, 1481, 1121, 1124, 
1125, 1122, 421, 68, 541, 70, 1, 71, 124, 181, 73, 701, 702, 33, 704, 503, 82, 482, 710, 709, 1241, 481, 483, 502, 716, 708, 80, 39, 686, 685, 132, 84, 711, 684, 41, 110, 29, 
714, 102, 105, 717, 521, 524, 103, 501, 718, 18, 720, 281, 89, 25, 24, 3, 4, 683, 713, 1101, 1402, 1401, 57, 44, 
1201, 45, 48, 851, 50, 941, 99, 51, 852, 58, 981, 52, 1081, 1083, 1082, 56, 1001, 401, 65, 881, 861, 846, 841, 843, 842, 847, 845, 441, 921, 11, 66, 100, 72, 2, 20, 682, 19, 1421, 1441, 761, 
341, 901, 27, 762, 10, 1181, 113, 1022, 1021, 119, 126, 28, 30, 1023, 1261, 641, 16, 1041, 721, 1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342, 21, 107]
))
)
--insert into pmt_dpl.fact_permits
select *
	from ats_data
;