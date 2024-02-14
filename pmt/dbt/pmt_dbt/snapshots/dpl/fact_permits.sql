{% snapshot fact_permits %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['ministry_code','business_area_code','permit_status_code','permit_status_date','permit_issue_date','app_received_date','app_update_dateapp_update_date','app_decision_date','app_decision_date','app_issuance_date','app_accepted_date','app_rejected_date','app_adjudication_date','harvest_area_sq_m','harvest_auth_status','map_feature_id','permit_type_code'],
		  invalidate_hard_deletes=True,
          bind=False,
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
                    ELSE NULL   --- ELSE NULL is causing null ministries
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
   ,ats.authorization_id::varchar 		as application_id
  ,null ::varchar 						as permit_id
  ,ats.file_number 						as app_file_id
  ,ats.authorization_status_code 		as authorization_status_code
  ,ats.project_id::varchar(20)			as project_id
  ,null 								as permit_status_code
  ,null::date  								as permit_status_date
  ,null::date  								as permit_issue_date
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
  ,null::bigint 								as map_feature_id
  ,ats.authorization_instrument_id::varchar(30) 		as permit_type_code
  ,'ATS' || '|'||  coalesce(cast(ats.authorization_id as varchar),'~')||'|'||coalesce(cast(ats.file_number as varchar),'~') as unqid
from fdw_ods_ats_replication.ats_authorizations ats
LEFT JOIN fdw_ods_ats_replication.ats_authorization_status_codes aasc
  ON(ats.authorization_status_code = aasc.authorization_status_code)
LEFT JOIN fdw_ods_ats_replication.ats_authorization_instruments aai
  ON(ats.authorization_instrument_id = aai.authorization_instrument_id)
where 1=1
AND aasc.authorization_status_code = '1'
AND (ats.authorization_instrument_id = 
ANY (ARRAY[961, 120, 1141, 76, 741, 742, 581, 742, 582, 744, 743, 583, 1061, 69, 542, 1481, 1121, 1124, 
1125, 1122, 421, 68, 541, 70, 1, 71, 124, 181, 73, 701, 702, 33, 704, 503, 82, 482, 710, 709, 1241, 481, 483, 502, 716, 708, 80, 39, 686, 685, 132, 84, 711, 684, 41, 110, 29, 
714, 102, 105, 717, 521, 524, 103, 501, 718, 18, 720, 281, 89, 25, 24, 3, 4, 683, 713, 1101, 1402, 1401, 57, 44, 
1201, 45, 48, 851, 50, 941, 99, 51, 852, 58, 981, 52, 1081, 1083, 1082, 56, 1001, 401, 65, 881, 861, 846, 841, 843, 842, 847, 845, 441, 921, 11, 66, 100, 72, 2, 20, 682, 19, 1421, 1441, 761, 
341, 901, 27, 762, 10, 1181, 113, 1022, 1021, 119, 126, 28, 30, 1023, 1261, 641, 16, 1041, 721, 1282, 1281, 1343, 1344, 1361, 1301, 1302, 1303, 1304, 1283, 60, 1341, 1305, 1306, 1342, 21, 107]
)) 
)
,fta_data as 
(
Select
'FTA' as src_sys_code
,'FOR' as ministry_code
,'FORESTS' as business_area_code
,tenure_app_id::varchar as application_id
,cutting_permit_id as permit_id
,forest_file_id as app_file_id
,tenure_application_state_code as authorization_status_code
,null as project_id
,permit_status_code as permit_status_code
,adjudication_date as permit_status_date
,issuance_date as permit_issue_date
,expiry_date as permit_issue_expire_date
,submission_date as app_received_date
,adjudication_date as app_update_date
,decision_date as app_decision_date
,issuance_date as app_issuance_date
,submission_date as app_accepted_date
,case when tenure_application_state_code = 'REJ' then adjudication_date end as app_rejected_date
,adjudication_date as app_adjudication_date
,permit_status_code as harvest_auth_status
,harvest_area::int  as harvest_area_sq_m
,org_unit_code as permit_org_unit_code
,extract(epoch from age)/86400 as application_age
,map_feature_id as map_feature_id
,permit_type_code as permit_type_code
,'FTA' || '|'||  coalesce(cast(forest_file_id as varchar),'~')||'|'||coalesce(cast( cutting_permit_id as varchar),'~') as unqid
from 
( SELECT 
			hva.forest_file_id ,
  			   ou.org_unit_code as org_unit_code,
            pfu.file_type_code AS permit_type_code,
            ftc.description AS permit_type,
            COALESCE(hva.cutting_permit_id, hva.forest_file_id) AS cutting_permit_id,
            htc.description AS havesting_auth_type,
            hasc.harvest_auth_status_code as permit_status_code,
            tatc.description AS tenure_application_type,
            tasc.description AS tenure_application_state_code,
            hva.tenure_term,
            hva.status_date,
            hva.issue_date,
            hva.expiry_date,
            hva.extend_date,
                CASE
                    WHEN hva.extend_date IS NOT NULL THEN hva.extend_date
                    WHEN hva.expiry_date IS NOT NULL THEN hva.expiry_date
                    ELSE GREATEST(hva.extend_date, hva.expiry_date)
                END AS current_expiry_date_calc,
            hva.location,
            hva.harvest_area,
            ta.tenure_app_id,
            ta.tenure_appname,
            ta.submission_id,
            ta.client_number,
            ta.adjudication_ind,
            ta.adjudication_date,
            ta.submission_date,
            ta.utm_zone,
            ta.decision_date,
            ta.issuance_date,
                CASE
                    WHEN ta.tenure_application_state_code = 'APP' THEN ta.decision_date - ta.submission_date
                    WHEN ta.tenure_application_state_code = ANY (ARRAY['ISS', 'REJ']) THEN ta.issuance_date - ta.submission_date
                    ELSE CURRENT_DATE::timestamp without time zone - ta.submission_date
                END  AS age,
            null as geometry,
            hag.map_feature_id,
            NULL AS project_name,
            NULL AS project_description,
            NULL AS property_subclass,
            NULL AS project_location,
            NULL AS utm_easting,
            NULL AS utm_northing,
            NULL AS utm_zone,
            hva.hva_skey
           FROM ( SELECT harvesting_authority.hva_skey,
                    harvesting_authority.forest_file_id,
                    harvesting_authority.cutting_permit_id,
                    harvesting_authority.harvesting_authority_id,
                    harvesting_authority.forest_district,
                    harvesting_authority.district_admn_zone,
                    harvesting_authority.geographic_district,
                    harvesting_authority.mgmt_unit_id,
                    harvesting_authority.mgmt_unit_type_code,
                    harvesting_authority.licence_to_cut_code,
                    harvesting_authority.harvest_type_code,
                    harvesting_authority.harvest_auth_status_code,
                    harvesting_authority.tenure_term,
                    harvesting_authority.status_date,
                    harvesting_authority.issue_date,
                    harvesting_authority.expiry_date,
                    harvesting_authority.extend_date,
                    harvesting_authority.extend_count,
                    harvesting_authority.harvest_auth_extend_reas_code,
                    harvesting_authority.quota_type_code,
                    harvesting_authority.crown_lands_region_code,
                    harvesting_authority.salvage_type_code,
                    harvesting_authority.cascade_split_code,
                    harvesting_authority.catastrophic_ind,
                    harvesting_authority.crown_granted_ind,
                    harvesting_authority.cruise_based_ind,
                    harvesting_authority.deciduous_ind,
                    harvesting_authority.bcaa_folio_number,
                    harvesting_authority.location,
                    harvesting_authority.higher_level_plan_reference,
                    harvesting_authority.harvest_area,
                    harvesting_authority.retirement_date,
                    harvesting_authority.revision_count,
                    harvesting_authority.entry_userid,
                    harvesting_authority.entry_timestamp,
                    harvesting_authority.update_userid,
                    harvesting_authority.update_timestamp,
                    harvesting_authority.is_waste_assessment_required,
                    harvesting_authority.is_cp_extensn_appl_fee_waived,
                    harvesting_authority.is_cp_extension_appl_fee_paid,
                    harvesting_authority.is_within_fibre_recovery_zone,
                    harvesting_authority.harvesting_authority_guid
                   FROM fdw_ods_fta_replication.harvesting_authority
                  WHERE (harvesting_authority.forest_file_id IN ( SELECT zz.forest_file_id
                           FROM ( SELECT a.forest_file_id,
                                    count(a.cutting_permit_id) AS count
                                   FROM fdw_ods_fta_replication.harvesting_authority a
                                     JOIN ( SELECT DISTINCT harvesting_authority_1.forest_file_id
   FROM fdw_ods_fta_replication.harvesting_authority harvesting_authority_1
  WHERE harvesting_authority_1.cutting_permit_id IS NULL) z ON a.forest_file_id = z.forest_file_id
                                  GROUP BY a.forest_file_id
                                 HAVING count(a.cutting_permit_id) = 0) zz))) hva
             JOIN fdw_ods_fta_replication.prov_forest_use pfu ON hva.forest_file_id = pfu.forest_file_id
             JOIN fdw_ods_fta_replication.file_type_code ftc ON ftc.file_type_code = pfu.file_type_code
             LEFT JOIN fdw_ods_fta_replication.harvest_authority_geom hag ON hva.hva_skey = hag.hva_skey
             JOIN fdw_ods_fta_replication.harvest_type_code htc ON hva.harvest_type_code = htc.harvest_type_code
             JOIN fdw_ods_fta_replication.harvest_auth_status_code hasc ON hasc.harvest_auth_status_code = hva.harvest_auth_status_code
           LEFT JOIN fdw_ods_fta_replication.tenure_application_map_feature tamf ON tamf.map_feature_id = hag.map_feature_id
             LEFT JOIN fdw_ods_fta_replication.tenure_application ta ON ta.forest_file_id = hva.forest_file_id and hva.cutting_permit_id = ta.cutting_permit_id
             left join fdw_ods_fta_replication.org_unit ou on (ta.org_unit_no = ou.org_unit_no)
             LEFT JOIN fdw_ods_fta_replication.tenure_application_state_code tasc ON tasc.tenure_application_state_code = ta.tenure_application_state_code
             LEFT JOIN fdw_ods_fta_replication.tenure_application_type_code tatc ON tatc.tenure_application_type_code = ta.tenure_application_type_code
          WHERE (ftc.file_type_code = ANY (ARRAY['A01', 'A02', 'A03', 'A04', 'A05', 'A11', 'A18', 'A28', 'A29', 'A30', 'A31', 'A41', 'A44', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B15', 'B16', 'E01', 'E02', 'H01', 'H02', 'S01', 'S02'])) AND (tasc.description = ANY (ARRAY['Inbox', 'Lobby']))

UNION ALL
         SELECT hva.forest_file_id ,
         ou.org_unit_code as org_unit_code,
            pfu.file_type_code AS permit_type_code,
            ftc.description AS permit_type,
            COALESCE(hva.cutting_permit_id, hva.forest_file_id) AS cutting_permit_id,
            htc.description AS havesting_auth_type,
            hasc.harvest_auth_status_code as permit_status_code,
            tatc.description AS tenure_application_type,
            tasc.description AS tenure_application_state_code,
            hva.tenure_term,
            hva.status_date,
            hva.issue_date,
            hva.expiry_date,
            hva.extend_date,
                CASE
                    WHEN hva.extend_date IS NOT NULL THEN hva.extend_date
                    WHEN hva.expiry_date IS NOT NULL THEN hva.expiry_date
                    ELSE GREATEST(hva.extend_date, hva.expiry_date)
                END AS current_expiry_date_calc,
            hva.location,
            hva.harvest_area,
            ta.tenure_app_id,
            ta.tenure_appname,
            ta.submission_id,
            ta.client_number,
            ta.adjudication_ind,
            ta.adjudication_date,
            ta.submission_date,
            ta.utm_zone,
            ta.decision_date,
            ta.issuance_date,
                CASE
                    WHEN ta.tenure_application_state_code = 'APP' THEN ta.decision_date - ta.submission_date
                    WHEN ta.tenure_application_state_code = ANY (ARRAY['ISS', 'REJ']) THEN ta.issuance_date - ta.submission_date
                    ELSE CURRENT_DATE::timestamp without time zone - ta.submission_date
                END AS age,
            null as geometry,
            hag.map_feature_id,
            NULL AS project_name,
            NULL AS project_description,
            NULL AS property_subclass,
            NULL AS project_location,
            NULL AS utm_easting,
            NULL AS utm_northing,
            NULL AS utm_zone,
            hva.hva_skey
           FROM ( SELECT harvesting_authority.hva_skey,
                    harvesting_authority.forest_file_id,
                    harvesting_authority.cutting_permit_id,
                    harvesting_authority.harvesting_authority_id,
                    harvesting_authority.forest_district,
                    harvesting_authority.district_admn_zone,
                    harvesting_authority.geographic_district,
                    harvesting_authority.mgmt_unit_id,
                    harvesting_authority.mgmt_unit_type_code,
                    harvesting_authority.licence_to_cut_code,
                    harvesting_authority.harvest_type_code,
                    harvesting_authority.harvest_auth_status_code,
                    harvesting_authority.tenure_term,
                    harvesting_authority.status_date,
                    harvesting_authority.issue_date,
                    harvesting_authority.expiry_date,
                    harvesting_authority.extend_date,
                    harvesting_authority.extend_count,
                    harvesting_authority.harvest_auth_extend_reas_code,
                    harvesting_authority.quota_type_code,
                    harvesting_authority.crown_lands_region_code,
                    harvesting_authority.salvage_type_code,
                    harvesting_authority.cascade_split_code,
                    harvesting_authority.catastrophic_ind,
                    harvesting_authority.crown_granted_ind,
                    harvesting_authority.cruise_based_ind,
                    harvesting_authority.deciduous_ind,
                    harvesting_authority.bcaa_folio_number,
                    harvesting_authority.location,
                    harvesting_authority.higher_level_plan_reference,
                    harvesting_authority.harvest_area,
                    harvesting_authority.retirement_date,
                    harvesting_authority.revision_count,
                    harvesting_authority.entry_userid,
                    harvesting_authority.entry_timestamp,
                    harvesting_authority.update_userid,
                    harvesting_authority.update_timestamp,
                    harvesting_authority.is_waste_assessment_required,
                    harvesting_authority.is_cp_extensn_appl_fee_waived,
                    harvesting_authority.is_cp_extension_appl_fee_paid,
                    harvesting_authority.is_within_fibre_recovery_zone,
                    harvesting_authority.harvesting_authority_guid
                   FROM fdw_ods_fta_replication.harvesting_authority
                  WHERE (harvesting_authority.forest_file_id IN ( SELECT zz.forest_file_id
                           FROM ( SELECT a.forest_file_id,
                                    count(a.cutting_permit_id) AS count
                                   FROM fdw_ods_fta_replication.harvesting_authority a
                                     JOIN ( SELECT DISTINCT harvesting_authority_1.forest_file_id
   FROM fdw_ods_fta_replication.harvesting_authority harvesting_authority_1
  WHERE harvesting_authority_1.cutting_permit_id IS NOT NULL) z ON a.forest_file_id = z.forest_file_id
                                  GROUP BY a.forest_file_id
                                 HAVING count(a.cutting_permit_id) > 0) zz))) hva
             JOIN fdw_ods_fta_replication.prov_forest_use pfu ON hva.forest_file_id = pfu.forest_file_id
             JOIN fdw_ods_fta_replication.file_type_code ftc ON ftc.file_type_code = pfu.file_type_code
             LEFT JOIN fdw_ods_fta_replication.harvest_authority_geom hag ON hva.hva_skey = hag.hva_skey
             JOIN fdw_ods_fta_replication.harvest_type_code htc ON hva.harvest_type_code = htc.harvest_type_code
             JOIN fdw_ods_fta_replication.harvest_auth_status_code hasc ON hasc.harvest_auth_status_code = hva.harvest_auth_status_code
          LEFT JOIN fdw_ods_fta_replication.tenure_application_map_feature tamf ON tamf.map_feature_id = hag.map_feature_id
             LEFT JOIN (
				select row_number()over(partition by forest_file_id, cutting_permit_id order by tenure_app_id desc) rn, * 
				from fdw_ods_fta_replication.tenure_application
				) ta ON 
				ta.forest_file_id = hva.forest_file_id 
				and hva.cutting_permit_id = ta.cutting_permit_id 
				and ta.rn =1
             left join fdw_ods_fta_replication.org_unit ou on (ta.org_unit_no = ou.org_unit_no)
             LEFT JOIN fdw_ods_fta_replication.tenure_application_state_code tasc ON tasc.tenure_application_state_code = ta.tenure_application_state_code
             LEFT JOIN fdw_ods_fta_replication.tenure_application_type_code tatc ON tatc.tenure_application_type_code = ta.tenure_application_type_code
          WHERE (ftc.file_type_code = ANY (ARRAY['A01', 'A02', 'A03', 'A04', 'A05', 'A11', 'A18', 'A28', 'A29', 'A30', 'A31', 'A41', 'A44', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B15', 'B16', 'E01', 'E02', 'H01', 'H02', 'S01', 'S02'])) AND (tasc.description = ANY (ARRAY['Inbox', 'Lobby']))
  )fta limit 100
)
, rrs_rup_data as (
	Select
	'RRS_RUP' as src_sys_code
	,'FOR' as ministry_code
	,'ROADS' as business_area_code
	,permit_id as application_id
	,permit_id as permit_id
	,road_submission_id::varchar(32)  as app_file_id
	,submission_status_code as authorization_status_code
	,null as project_id
	,null as permit_status_code
	,update_date::date as permit_status_date
	,null::date as permit_issue_date
	,null::date as permit_issue_expire_date
	,received_date::date as app_received_date
	,update_date::date as app_update_date
	,null::date as app_decision_date
	,null::date as app_issuance_date
	,received_date::date as app_accepted_date
	,null::date as app_rejected_date
	,update_date::date as app_adjudication_date
	,null as harvest_auth_status
	,null::int  as harvest_area_sq_m
	,null as permit_org_unit_code
	,age as application_age
	,null ::bigint as map_feature_id
	,authorization_type as permit_type_code
	,'RRS_RUP' || '|'||  coalesce(cast(road_submission_id as varchar),'~')||'|'||coalesce(cast( permit_id as varchar),'~') as unqid
	from 
	(
	SELECT 
	  rup.permit_id::character varying AS permit_id,
		rup.permit_type AS authorization_type,
		rup.submission_status_code,
		rup.road_submission_id,
		rup.permit_id AS authorization_id,
		NULL::timestamp(0) without time zone AS status_date,
		NULL::timestamp(0) without time zone AS issue_date,
		NULL::timestamp(0) without time zone AS current_expiry_date_calc,
		NULL AS location,
		NULL::integer AS harvest_area,
		rup.received_date,
		NULL::timestamp(0) without time zone AS app_decision_date,
		NULL::timestamp(0) without time zone AS app_issuance_date,
		NULL AS project_name,
		NULL AS project_description,
		NULL AS property_subclass,
		NULL AS project_location,
		NULL AS project_id,
		NULL::timestamp without time zone AS accepted_date,
		NULL::timestamp without time zone AS rejected_date,
		NULL::timestamp without time zone AS adjudication_date,
		NULL AS permit_status,
		NULL::bigint AS map_feature_id,
		NULL AS road_section_id,
		NULL AS road_section_status,
		NULL AS organization_unit_name,
		NULL AS nrsos_smart_form_id,
		rup.update_date,
		NULL::character varying AS ats_region_name,
		NULL::bigint AS hva_skey,
		NULL::bytea AS road_section_guid,
		floor(EXTRACT(epoch FROM CURRENT_DATE::timestamp without time zone - rup.received_date) / 86400::numeric) AS age
	   FROM ( SELECT 
				rs.nrsos_smart_form_id AS permit_id,
				rs.road_submission_id,
				'RUP' AS permit_type,
				rs.nrsos_smart_form_id,
				rs.submission_date::date AS received_date,
				rs.submission_status_code,
				rs.update_date,
				NULL AS project_name,
				NULL AS project_description,
				NULL AS property_subclass,
				NULL AS project_location,
				NULL AS utm_easting,
				NULL AS utm_northing,
				NULL AS utm_zone
			   FROM fdw_ods_rrs_replication.road_submission rs
			  WHERE 
			  rs.road_submission_type_code = 'RUP' AND 
			  rs.nrsos_smart_form_id IS NOT NULL AND 
			  rs.submission_status_code = ANY (ARRAY['PRO'::character varying, 'SUB'::character varying])
			 ) rup
	) rup
)
,rrs_rp_data as (
	Select
	'RRS_RP' as src_sys_code
	,'FOR' as ministry_code
	,'ROADS' as business_area_code
	,permit_id as application_id
	,permit_id as permit_id
	,application_id::varchar(32) as app_file_id
	,application_status as authorization_status_code
	,null as project_id
	,road_tenure_status_code as permit_status_code
	,adjudication_date::date as permit_status_date
	,app_issuance_date::date  as permit_issue_date
	,null::date as permit_issue_expire_date
	,received_date::date as app_received_date
	,adjudication_date as app_update_date
	,app_decision_date::date as app_decision_date
	,app_issuance_date::date as app_issuance_date
	,received_date::date as app_accepted_date
	,case when application_status = 'REJ' then adjudication_date end as app_rejected_date
	,adjudication_date::date as app_adjudication_date
	,road_tenure_status_code as harvest_auth_status
	,null::int  as harvest_area_sq_m
	,org_unit_code as permit_org_unit_code
	,age as application_age
	,map_feature_id as map_feature_id
	,permit_type as permit_type_code
	,'RRS_RP' || '|'||  coalesce(cast(application_id as varchar),'~')||'|'||coalesce(cast( permit_id as varchar),'~') as unqid
	from 
	(
	 SELECT
    rp.permit_id::character varying AS permit_id,
    rp.permit_type ,
    rp.application_id ,
    rp.application_status,
	rp.road_tenure_status_code,
    rp.application_submission_date AS received_date,
    rp.application_decision_date::date AS app_decision_date,
    rp.application_issuance_date::date AS app_issuance_date,
    rp.application_adjudication_date::date AS adjudication_date,
    rp.map_feature_id,
    rp.road_section_id,
    rp.road_section_status,
    rp.org_unit_code,
    rp.application_update_date AS update_date,
    floor(
        CASE
            WHEN (rp.application_status = 'Approved' OR rp.application_status = 'Issued') AND rp.application_issuance_date IS NOT NULL THEN EXTRACT(epoch FROM rp.application_issuance_date - rp.application_submission_date) / 86400
            WHEN (rp.application_status = 'Approved' OR rp.application_status = 'Issued') AND rp.application_decision_date IS NOT NULL THEN EXTRACT(epoch FROM rp.application_decision_date - rp.application_submission_date) / 86400
            WHEN rp.application_status = 'Rejected' THEN EXTRACT(epoch FROM rp.application_update_date - rp.application_submission_date) / 86400
            ELSE EXTRACT(epoch FROM CURRENT_DATE::timestamp without time zone - rp.application_submission_date) / 86400
        END::numeric) AS age

   FROM ( SELECT
            z.permit_id,
            z.road_tenure_type_code AS permit_type,
            z.road_tenure_status AS permit_status,
            z.road_application_id AS application_id,
            z.road_application_status AS application_status,
			z.road_tenure_status_code,
            z.map_feature_id,
            z.road_section_id,
            z.road_section_status,
            z.org_unit_code,
            z.submission_date AS application_submission_date,
            z.adjudication_date AS application_adjudication_date,
            z.decision_date AS application_decision_date,
            z.issuance_date AS application_issuance_date,
            z.create_date AS application_create_date,
            z.update_date AS application_update_date,
            z.geometry_road_section_guid
           FROM ( SELECT t.resource_road_tenure_id AS permit_id,
                    t.road_tenure_type_code,
                    rtsc.description AS road_tenure_status,
                    ra.road_application_id,
                    rasc.description AS road_application_status,
          			t.road_tenure_status_code,
                    mf.map_feature_id,
                    rs.road_section_id,
                    rssc.description AS road_section_status,
                    rs.current_ind,
                    mf.feature_record_type_code,
                    mf.feature_business_identifier,
                    ra.submission_date,
                    ra.adjudication_date,
                    ra.decision_date,
                    ra.issuance_date,
                    ra.create_date,
                    ra.update_date,
                    fou.org_unit_code,
                    rs.geometry_road_section_guid
                   FROM fdw_ods_rrs_replication.resource_road_tenure t
                     JOIN fdw_ods_rrs_replication.road_application ra ON encode(ra.resource_road_tenure_guid, 'hex') = encode(t.resource_road_tenure_guid, 'hex')
                     JOIN fdw_ods_rrs_replication.road_appl_map_feature mf ON encode(mf.road_application_guid, 'hex') = encode(ra.road_application_guid, 'hex')
                     JOIN fdw_ods_rrs_replication.road_section rs ON encode(mf.feature_record_guid, 'hex') = encode(rs.road_section_guid, 'hex')
                     JOIN fdw_ods_rrs_replication.road_feature_class_sdw fc ON encode(mf.road_feature_class_sdw_guid, 'hex') = encode(fc.road_feature_class_sdw_guid, 'hex')
                     JOIN fdw_ods_rrs_replication.road_org_unit_sdw ou ON encode(ou.road_org_unit_sdw_guid, 'hex') = encode(ra.metadata_org_unit_sdw_guid, 'hex')
					 left join  fdw_ods_fta_replication.org_unit fou on (fou.org_unit_name = ou.organization_unit_name)  
                     LEFT JOIN fdw_ods_rrs_replication.road_application_status_code rasc ON ra.road_application_status_code = rasc.road_application_status_code
                     LEFT JOIN fdw_ods_rrs_replication.road_tenure_type_code rttc ON t.road_tenure_type_code = rttc.road_tenure_type_code
                     LEFT JOIN fdw_ods_rrs_replication.road_tenure_status_code rtsc ON t.road_tenure_status_code = rtsc.road_tenure_status_code
                     LEFT JOIN fdw_ods_rrs_replication.road_section_status_code rssc ON rssc.road_section_status_code = rs.road_section_status_code
                     LEFT JOIN fdw_ods_rrs_replication.road_submission rsu ON encode(rsu.road_submission_guid, 'hex') = encode(ra.road_submission_guid, 'hex')
                  WHERE t.road_tenure_type_code <> 'B40' AND (rsu.road_submission_type_code = ANY (ARRAY['RP'::character varying, 'SRP'::character varying, 'SRL'::character varying]))) z
          WHERE z.road_application_status = ANY (ARRAY['Inbox'::character varying, 'Lobby'::character varying])) rp
	)rp
)
--insert into pmt_dpl.fact_permits
select * from ats_data
union ALL
select * from fta_data	
union ALL
select * from rrs_rup_data	
union ALL
select * from rrs_rp_data		
;

{% endsnapshot %}