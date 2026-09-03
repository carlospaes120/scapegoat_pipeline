-- =====================================================================
-- Perfis de acesso e privilégios — v3.1-mvp
-- Implementa o § 9 da Política de Dados (menor privilégio).
-- Idempotente: pode rodar mais de uma vez.
--
-- Princípios que este arquivo torna técnicos, não apenas escritos:
--   1. O pipeline NÃO roda como superusuário  -> papel `etl`
--   2. O pipeline NÃO apaga postagens         -> etl sem DELETE em core.postagem
--      (remoção é tombstone: UPDATE em `situacao`)
--   3. O site NÃO enxerga texto nem handles   -> app_site só lê o esquema pub
--
-- No Supabase: rode como `postgres`. O papel `app_site` é o que deve ser
-- concedido a `anon`/`authenticated` se um dia a API REST for ligada.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 1. Papéis (grupos sem login; usuários reais herdam deles)
-- ---------------------------------------------------------------------
DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['curador','consultor','moderador','app_site','etl'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;
END $$;

COMMENT ON ROLE curador   IS 'Carlos. Data owner e steward: tudo.';
COMMENT ON ROLE consultor IS 'Consultoria de modelagem. Somente leitura.';
COMMENT ON ROLE moderador IS 'Triagem de contribuições: lê o acervo, muda status de CASO, registra auditoria.';
COMMENT ON ROLE app_site  IS 'O site. SELECT exclusivamente no esquema pub.';
COMMENT ON ROLE etl       IS 'O pipeline. Escreve em stg e core, nunca apaga postagem, nunca é superusuário.';

-- ---------------------------------------------------------------------
-- 2. Higiene: ninguém cria coisa solta no schema public
-- ---------------------------------------------------------------------
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- ---------------------------------------------------------------------
-- 3. Curador — acesso total
-- ---------------------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA core, stg, mart, pub TO curador;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA core, stg, mart, pub TO curador;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA core, stg, mart, pub TO curador;

-- ---------------------------------------------------------------------
-- 4. ETL — o pipeline
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA core, stg, mart TO etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg  TO etl;
GRANT SELECT, INSERT, UPDATE         ON ALL TABLES IN SCHEMA core TO etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA core, stg TO etl;
-- Apagar conteúdo é decisão curatorial, não efeito colateral de carga:
REVOKE DELETE ON core.postagem, core.caso, core.ator, core.conta FROM etl;

-- ---------------------------------------------------------------------
-- 5. Consultor — leitura
-- ---------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core, mart, pub TO consultor;
GRANT SELECT ON ALL TABLES IN SCHEMA core, mart, pub TO consultor;

-- ---------------------------------------------------------------------
-- 6. Moderador — triagem
-- ---------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core, pub TO moderador;
GRANT SELECT ON ALL TABLES IN SCHEMA core, pub TO moderador;
GRANT UPDATE (status) ON core.caso      TO moderador;
GRANT INSERT          ON core.auditoria TO moderador;
GRANT USAGE, SELECT ON SEQUENCE core.auditoria_auditoria_id_seq TO moderador;

-- ---------------------------------------------------------------------
-- 7. app_site — o site, e SÓ o esquema público
-- ---------------------------------------------------------------------
GRANT USAGE  ON SCHEMA pub TO app_site;
GRANT SELECT ON ALL TABLES IN SCHEMA pub TO app_site;
REVOKE ALL ON SCHEMA core, stg, mart FROM app_site;

-- ---------------------------------------------------------------------
-- 8. Privilégios padrão — o que for criado depois nasce com a regra certa
-- ---------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES IN SCHEMA core
  GRANT SELECT, INSERT, UPDATE ON TABLES TO etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA core, stg
  GRANT USAGE, SELECT ON SEQUENCES TO etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA core, mart, pub
  GRANT SELECT ON TABLES TO consultor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pub
  GRANT SELECT ON TABLES TO app_site;
ALTER DEFAULT PRIVILEGES IN SCHEMA core, stg, mart, pub
  GRANT ALL ON TABLES TO curador;

COMMIT;
