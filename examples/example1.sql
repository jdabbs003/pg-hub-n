--
-- Example
--
-- PostgreSQL triggers and common function to generate
-- pg-hub compatible events in response to table inserts,
-- deletes, and updates, at a statement level.
--
-- Copyright (c) 2024 James M Dabbs III
-- ISC License
--

create or replace function fun_notify_tab_stm() returns trigger
as $$
 declare key_count integer := 0;
 declare key_list text := '';
 declare payload text;
 declare channel text := TG_TABLE_NAME || '_stm';
 declare key_max integer = 300;
 begin
  if (TG_OP = 'DELETE') then
   select count(id) from old_table into key_count;
   if (key_count < key_max) then
    select string_agg(id::text, ',') from old_table into key_list;
   end if;
  end if;

  if (TG_OP = 'UPDATE') then
   select distinct count(id) from ((select id from new_table) union (select id from old_table)) into key_count;
   if (key_count < key_max) then
    select distinct string_agg(id::text, ',') from ((select id from new_table) union (select id from old_table)) into key_list;
   end if;
  end if;

  if (TG_OP = 'INSERT') then
   select count(id) from new_table into key_count;
   if (key_count < key_max) then
    select string_agg(id::text, ',') from new_table into key_list;
   end if;
  end if;
			
  payload :=
   'e' ||
   key_list ||
   ':{"op":"' ||
   lower(substring(TG_OP,1,3)) ||
   '","count":' ||
   key_count ||
  '}';
			
  perform pg_notify(channel, payload);
  return NULL;
 end;
$$ language plpgsql;

create trigger accounts_notify_stm_ins after insert on accounts
 referencing new table as new_table
 for each statement execute function fun_notify_tab_stm();

create trigger accounts_notify_stm_upd after update on accounts
 referencing old table as old_table new table as new_table
 for each statement execute function fun_notify_tab_stm();

create trigger accounts_notify_stm_del after delete on accounts
 referencing old table as old_table
 for each statement execute function fun_notify_tab_stm();
