-- The requirement_blocks table
drop table if exists requirement_blocks cascade;

create table requirement_blocks (
 institution       text   not null,
 requirement_id    text   not null,
 block_type        text,
 block_value       text,
 title             text,
 period_start      text,
 period_stop       text,
 school            text,
 degree            text,
 college           text,
 major1            text,
 major2            text,
 concentration     text,
 minor             text,
 liberal_learning  text,
 specialization    text,
 program           text,
 parse_status      text,
 parse_date        date,
 parse_who         text,
 parse_what        text,
 lock_version      text,
 requirement_text  text,
 requirement_html  text,
 parse_tree        jsonb default '{}'::jsonb,
 irdw_load_date    date,
 dgw_seconds       real,
 dgw_timestamp     text,
 terminfo          jsonb,
 PRIMARY KEY (institution, requirement_id));

drop view if exists view_blocks;
create view view_blocks as
  select institution,
         requirement_id,
         block_type,
         block_value,
         title,
         period_stop,
         parse_date,
         to_char(length(parse_tree::text), '999G999G999') as tree_size,
         dgw_seconds,
         irdw_load_date
  from requirement_blocks;
