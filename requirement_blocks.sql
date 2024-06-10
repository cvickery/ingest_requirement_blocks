-- The requirement_blocks table
DROP TABLE IF EXISTS requirement_blocks CASCADE;

CREATE TABLE requirement_blocks (
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
 dgw_parse_tree    json default null,
 dgw_seconds       real default null,
 irdw_load_date    date,
 dgw_parse_date    date default null,
 terminfo          json,
 PRIMARY KEY (institution, requirement_id));

drop view if exists view_blocks;
create view view_blocks as
   SELECT institution,
    requirement_id,
    block_type,
    block_value,
    title,
    major1,
    period_stop,
    term_info IS NOT NULL AS is_active
   FROM requirement_blocks;
