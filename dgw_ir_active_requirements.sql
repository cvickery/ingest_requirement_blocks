-- SQL to pull number of distinct students per RA, per term, across CUNY for matching to show
-- program requirements in T-Rex

select institution, dap_req_id, dap_active_term, 
count (distinct dap_stu_id)as totalstudents
from fnd_dw.dap_result_dtl
where dap_req_id not in ('FALLTHRU','STUINFO','OTL','INSUFF')
-- this removes RA numbers that don't contain scribe
and dap_active_term is not null
-- there were lots of null values showing up, not sure why, but this gets rid of them. 
and dap_active_term not like '%G%'
-- this removes graduate students
and dap_active_term like '1%'
-- this removes the junk terms from conversion
group by institution, dap_req_id, dap_active_term
order by institution, dap_req_id, dap_active_term desc;