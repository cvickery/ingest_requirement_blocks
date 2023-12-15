-- view core information about requirement_blocks
drop view if exists view_requirement_blocks;
create view view_requirement_blocks as
  select institution, requirement_id, block_type, block_value, major1, dgw_parse_date, title
    from requirement_blocks
   where period_stop ~* '^9'
     and block_value !~* '^mhc'
     and major1 !~* '^mhc'
order by institution, block_type, block_value, major1
;
