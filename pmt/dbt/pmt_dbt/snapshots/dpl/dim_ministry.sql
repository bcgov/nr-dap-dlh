{% snapshot dim_ministry %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['ministry_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as (
  	select distinct 'CORP' as src_sys_code
    ,CASE
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'EMLI'
                      WHEN ats.authorization_instrument_id = 27 THEN 'FOR'
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'AGR'
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342, 21, 107]) THEN 'ENV'
                      ELSE NULL
                  END AS ministry_code
   ,CASE
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061]) THEN 'Ministry of Energy, Mines and Low Carbon Innovation'
                      WHEN ats.authorization_instrument_id = 27 THEN 'Ministry of Forests'
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[2, 20, 682, 19, 1421, 1441]) THEN 'Ministry of Agriculture'
                      WHEN ats.authorization_instrument_id = ANY (ARRAY[1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342, 21, 107]) THEN 'Minitry of Environment'
                      ELSE NULL
                  END AS ministry_description 
				  
  ,'CORP' || '|'||  coalesce(cast(ministry_code as varchar),'~') as unqid
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
--insert into pmt_dpl.dim_ministry
select * from ats_data
where ministry_code is not null
;

{% endsnapshot %}