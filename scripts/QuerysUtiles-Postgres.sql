-- Ver las sesiones conectadas a una bd
SELECT * FROM pg_stat_activity WHERE datname = 'Mg_Operaciones_TST'

--Matar sesiones conectadas a una base de datos 
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'Mg_Operaciones_TST'


SELECT * FROM pg_stat_activity WHERE datname = 'Mg_Operaciones_TST'

---Matar un spid en específico
SELECT pg_terminate_backend(8148) FROM pg_stat_activity WHERE datname = 'Mg_Operaciones_TST'

SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'Mg_Operaciones_TST'

--- Estadisticos..
vacuum verbose analyze CUS_ADRESSES
vacuum verbose analyze OP_TICKETS

---- Estadisticos en bd mg_operaciones
VACUUM VERBOSE ANALYZE DA_OP_CREW_LOCATIONS --OK 24 MIN
VACUUM VERBOSE ANALYZE OP_CREW_LOCATIONS ---OK  7 SEG
VACUUM VERBOSE ANALYZE OP_TICKETS     ---OK  27 MIN
VACUUM VERBOSE ANALYZE CUS_CUSTOMER_SERVICES --OK 1 MIN
VACUUM VERBOSE ANALYZE CUS_ADDRESSES   -- OK 24 MIN
VACUUM VERBOSE ANALYZE CUS_CUSTOMERS  --OK 1 MIN


??? autovacuum: VACUUM public.op_tickets

---- SPID  HACIENDO BACKUP
SELECT * FROM pg_stat_activity WHERE datname = 'mg_operaciones' and application_name='pg_dump'



---LISTADO DE TABLAS DE LA BD
SELECT * FROM information_schema.tables  WHERE table_catalog = 'Mg_Operaciones_TST' AND table_type = 'BASE TABLE' AND table_schema = 'public'
order by table_name


-- SIMILAR A TOP 10 DE SQL
select *  from comprobante_fiscal_digital limit 10

select *  from cus_places limit 10

-- CREACIÓN DE INDICES
--create index idx_folio_fiscal on comprobante_fiscal_digital(folio_fiscal);

select * from comprobante_fiscal_digital  where folio_fiscal='5d42aad7-cce6-4a8b-b70d-515b38a30d08'


CREATE INDEX "IX_mi_op_ticket_history_date_asi"
    ON public.op_ticket_history USING btree
    (date_asi ASC NULLS LAST)
    TABLESPACE pg_default;


-- UBICACION DE LAS TABLAS
select pg_relation_filepath('bitacora_comandos')  --- Ej:  'pg_tblspc/16500/PG_9.2_201204301/16510/16511'

--Revisar logs
data/pg_log (desde linea de comando)



---Querys larga duración
SELECT
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query,
  state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
and state != 'idle'


--Querys ordenados en base a la fecha de inicio
-- Para ver los que duran más
SELECT datname,
       pid,
       usename,
       client_addr,
       client_port,
       xact_start,
       backend_start,
       query_start,
       state,
       query
FROM pg_stat_activity 
WHERE state != 'idle'
ORDER BY query_start ASC


---- BLOQUEOS 1 -- QUÉ CONSULTA ESTÁ BLOQUEADA POR QUÉ DECLARACIÓN

SELECT
  COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
  now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
  blockeda.query as blocked_query, blockedl.mode as blocked_mode,
  blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
  blockingl.mode as blocking_mode
FROM pg_catalog.pg_locks blockedl
JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
JOIN pg_catalog.pg_locks blockingl ON(
  ( (blockingl.transactionid=blockedl.transactionid) OR
  (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
  ) AND blockedl.pid != blockingl.pid)
JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
  AND blockinga.datid = blockeda.datid
WHERE NOT blockedl.granted
AND blockinga.datname = current_database()


-- BLOQUEOS 2 OTRO QUERY: BLOQUEO DE CONSULTAS Y LAS SESIONES OBSTRUIDAS--

SELECT
    activity.pid,
    activity.usename,
    activity.query,
    blocking.pid AS blocking_id,
    blocking.query AS blocking_query
FROM pg_stat_activity AS activity
JOIN pg_stat_activity AS blocking ON blocking.pid = ANY(pg_blocking_pids(activity.pid));


--- O HACERLO VISTA Y CONSULTA LA VISTA---- BLOQUEOS 1
CREATE VIEW lock_monitor AS(
SELECT
  COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
  now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
  blockeda.query as blocked_query, blockedl.mode as blocked_mode,
  blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
  blockingl.mode as blocking_mode
FROM pg_catalog.pg_locks blockedl
JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
JOIN pg_catalog.pg_locks blockingl ON(
  ( (blockingl.transactionid=blockedl.transactionid) OR
  (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
  ) AND blockedl.pid != blockingl.pid)
JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
  AND blockinga.datid = blockeda.datid
WHERE NOT blockedl.granted
AND blockinga.datname = current_database()
);

SELECT * from lock_monitor;




-------------------------------


