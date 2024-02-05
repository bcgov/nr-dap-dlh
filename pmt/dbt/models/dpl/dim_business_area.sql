{{ config(materialized='table',
		  unique_key = ['src_sys_code','business_area_code']
		) 
}}
with ats_data as (
  select distinct 
  'ATS' as src_sys_code,
	CASE
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'MIN'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[69, 542, 1481, 1121, 1124, 1125, 1122, 421, 68, 541, 70, 1, 71, 124]) THEN 'WAT'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[181, 73, 701, 702, 33, 704, 503, 82, 482, 710, 709, 1241, 481, 483, 502, 716, 708, 80, 39, 686, 685, 132, 84, 711, 684, 41, 110, 29, 714, 102, 105, 717, 521, 524, 103, 501, 718, 18, 720, 281, 89, 25, 24, 3, 4, 683, 713]) THEN 'LAN'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[1101, 1402, 1401, 57, 44, 1201, 45, 48, 851, 50, 941, 99, 51, 852, 58, 981, 52, 1081, 1083, 1082, 56, 1001, 401, 65, 881, 861, 846, 841, 843, 842, 847, 845, 441, 921, 11, 66, 100, 72]) THEN 'FAW'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'AGRI'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[761, 341, 901, 27, 762, 10, 1181, 113, 1022, 1021, 119, 126, 28, 30, 1023, 1261, 641, 16, 1041, 721]) THEN 'FOR'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342]) THEN 'PAR'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[21, 107]) THEN 'RECS'
   ELSE NULL
   END as business_area_code,
  CASE
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'Mines'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[69, 542, 1481, 1121, 1124, 1125, 1122, 421, 68, 541, 70, 1, 71, 124]) THEN 'Water'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[181, 73, 701, 702, 33, 704, 503, 82, 482, 710, 709, 1241, 481, 483, 502, 716, 708, 80, 39, 686, 685, 132, 84, 711, 684, 41, 110, 29, 714, 102, 105, 717, 521, 524, 103, 501, 718, 18, 720, 281, 89, 25, 24, 3, 4, 683, 713]) THEN 'Lands'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[1101, 1402, 1401, 57, 44, 1201, 45, 48, 851, 50, 941, 99, 51, 852, 58, 981, 52, 1081, 1083, 1082, 56, 1001, 401, 65, 881, 861, 846, 841, 843, 842, 847, 845, 441, 921, 11, 66, 100, 72]) THEN 'Fish and Wildlife'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'Agriculture'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[761, 341, 901, 27, 762, 10, 1181, 113, 1022, 1021, 119, 126, 28, 30, 1023, 1261, 641, 16, 1041, 721]) THEN 'Forests'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342]) THEN 'Parks'
   WHEN authorizations.authorization_instrument_id = ANY (ARRAY[21, 107]) THEN 'Recreation Sites and Trails'
   ELSE NULL
   END AS business_area_description 
	from fdw_ods_ats_replication.ats_authorizations authorizations
	) 
--insert into pmt_dpl.dim_business_area
select *
	from ats_data
	where business_area_code is not null
;