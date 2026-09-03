-- =====================================================================
-- Teste do DDL v3.1-mvp — prova que cada regra do modelo é aplicada
-- pelo banco, e não apenas prometida na documentação.
--
--   psql -d scapegoat -v ON_ERROR_STOP=1 -f tests/test_ddl_v3_1_mvp.sql
--
-- Cada bloco tenta uma operação que DEVE falhar. Se ela passar, o teste
-- levanta exceção e o script inteiro aborta.
-- =====================================================================

\set ON_ERROR_STOP on
SET client_min_messages TO notice;

-- ---------------------------------------------------------------------
-- Fixture: um caso mínimo, ponta a ponta
-- ---------------------------------------------------------------------
BEGIN;

INSERT INTO core.plataforma (nome, dominio) VALUES ('x', 'x.com');
INSERT INTO core.papel_tipo (nome, descricao, criterio) VALUES
  ('vitima_principal', 'Alvo central da convergência acusatória.',
   'Conta/ator que recebe a maior parte das menções acusadoras na janela do clímax.'),
  ('lider_acusacao', 'Conta que concentra atenção antes do pico.',
   'Postagens acusadoras entre as de maior centralidade de entrada, excluído o alvo.');

INSERT INTO core.contribuinte (pseudonimo, email, papel_sistema, consentimento_lgpd, consentido_em)
VALUES ('carlos', 'carlos@example.org', 'admin', true, now());

INSERT INTO core.caso (slug, titulo, idioma_principal, pais, ambito, data_inicio, data_fim, status, criado_por)
VALUES ('monark', 'Monark / Flow Podcast (fev/2022)', 'pt', 'BR', 'online',
        '2022-02-07', '2022-02-14', 'rascunho', 1);

INSERT INTO core.ator (nome_publico, tipo, anonimizado) VALUES
  ('Alvo do episódio', 'pessoa', true),
  (NULL,               'pessoa', true);

INSERT INTO core.conta (plataforma_id, handle, id_nativo, ator_id, vinculado_por, vinculado_em, vinculo_criterio)
VALUES (1, 'alvo_handle', '111111111111111111', 1, 1, now(),
        'Handle citado nominalmente na cobertura de imprensa do episódio.');
INSERT INTO core.conta (plataforma_id, handle, id_nativo) VALUES
  (1, 'participante_1', '222222222222222222'),
  (1, 'participante_2', '333333333333333333');

INSERT INTO core.papel (caso_id, ator_id, papel_id, metodo, confianca, observacao)
VALUES (1, 1, 1, 'manual', 1.000, 'Alvo declarado do episódio.');

INSERT INTO core.coleta (caso_id, plataforma_id, tipo, consulta, ferramenta,
                         periodo_inicio, periodo_fim, responsavel)
VALUES (1, 1, 'scraping',
        '(Monark OR Flow) (exagero OR histeria OR "caça às bruxas") lang:pt',
        'playwright_dom v4', '2022-02-07', '2022-02-15', 1);

INSERT INTO core.arquivo_raw (coleta_id, uri, formato, hash_sha256, bytes)
VALUES (1, 'raw/plataforma=x/caso=monark/coleta=2022-02-15/tweets.jsonl', 'jsonl',
        repeat('a', 64), 9123456);

INSERT INTO core.postagem (plataforma_id, id_nativo, autor_conta_id, created_at, texto,
                           idioma, tipo_ref)
VALUES (1, '1490000000000000001', 2, '2022-02-08 21:10:00+00', 'texto original', 'pt', 'original');

INSERT INTO core.postagem (plataforma_id, id_nativo, autor_conta_id, created_at, texto,
                           idioma, tipo_ref, ref_postagem_id, ref_conta_id)
VALUES (1, '1490000000000000002', 3, '2022-02-08 21:40:00+00', 'resposta', 'pt', 'reply', 1, 2);

-- reply cujo tweet-pai está fora do corpus: aresta preservada mesmo assim
INSERT INTO core.postagem (plataforma_id, id_nativo, autor_conta_id, created_at, texto,
                           idioma, tipo_ref, ref_conta_id)
VALUES (1, '1490000000000000003', 3, '2022-02-09 01:00:00+00', 'resposta órfã', 'pt', 'reply', 1);

INSERT INTO core.captura (coleta_id, postagem_id, likes, retweets, quotes, replies)
VALUES (1, 1, 10, 5, 1, 2), (1, 2, 0, 0, 0, 0), (1, 3, 3, 0, 0, 0);

INSERT INTO core.mencao (postagem_id, conta_id) VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO core.postagem_hashtag (postagem_id, hashtag) VALUES (1, 'monark'), (1, 'flow');

INSERT INTO core.classificacao (postagem_id, esquema, rotulo, modelo, versao, confianca)
VALUES (1, 'stance', 'acusador', 'bertimbau-stance', 'v2b', 0.873),
       (2, 'stance', 'defensor', 'bertimbau-stance', 'v2b', 0.612);

INSERT INTO core.metrica (caso_id, nome, valor, versao_pipeline) VALUES
  (1, 'qc_linhas_raw', 5143, 'v0.1.0'),
  (1, 'qc_duplicatas_removidas', 340, 'v0.1.0');
INSERT INTO core.metrica (caso_id, nome, valor, janela_inicio, janela_fim, versao_pipeline)
VALUES (1, 'gini_mencoes', 0.812, '2022-02-08 21:00+00', '2022-02-08 23:59+00', 'v0.1.0');

INSERT INTO core.artefato (caso_id, tipo, uri, parametros, versao_pipeline)
VALUES (1, 'grafo', 'curated/caso=monark/monark_3h_2022-02-08_21-00.gexf',
        '{"janela":"3h","dirigido":true}'::jsonb, 'v0.1.0');

INSERT INTO core.auditoria (perfil, acao, entidade, entidade_id, motivo)
VALUES ('curador', 'publicou_caso', 'core.caso', '1', 'Revisão concluída.');

COMMIT;

DO $$
DECLARE v_eng integer;
BEGIN
  SELECT engajamento INTO v_eng FROM core.captura WHERE postagem_id = 1;
  IF v_eng <> 18 THEN
    RAISE EXCEPTION 'FALHOU: engajamento derivado deu % (esperado 18)', v_eng;
  END IF;
  RAISE NOTICE 'ok  01  engajamento derivado (10+5+1+2=18) calculado pelo banco';
END $$;

-- ---------------------------------------------------------------------
-- Testes negativos: cada regra tem de barrar o dado errado
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_temp.deve_falhar(rotulo text, sql text) RETURNS void AS $$
BEGIN
  BEGIN
    EXECUTE sql;
    RAISE EXCEPTION 'FALHOU: % — o banco aceitou dado que deveria recusar', rotulo;
  EXCEPTION
    WHEN check_violation OR unique_violation OR not_null_violation
      OR foreign_key_violation OR insufficient_privilege OR string_data_right_truncation THEN
      RAISE NOTICE 'ok  %', rotulo;
  END;
END $$ LANGUAGE plpgsql;

BEGIN;
SELECT pg_temp.deve_falhar('02  idioma fora do ISO 639-1 (maiúsculo)',
  $q$INSERT INTO core.caso (slug,titulo,idioma_principal,ambito) VALUES ('t1','T1','PT','online')$q$);

SELECT pg_temp.deve_falhar('03  país fora do ISO 3166-1 alpha-2 (minúsculo)',
  $q$INSERT INTO core.caso (slug,titulo,idioma_principal,pais,ambito) VALUES ('t2','T2','pt','br','online')$q$);

SELECT pg_temp.deve_falhar('04  caso com data_fim anterior a data_inicio',
  $q$INSERT INTO core.caso (slug,titulo,idioma_principal,ambito,data_inicio,data_fim)
     VALUES ('t3','T3','pt','online','2022-02-14','2022-02-07')$q$);

SELECT pg_temp.deve_falhar('05  vínculo CONTA->ATOR sem critério anotado (MDM)',
  $q$INSERT INTO core.conta (plataforma_id,handle,ator_id) VALUES (1,'sem_criterio',2)$q$);

SELECT pg_temp.deve_falhar('06  handle repetido só mudando maiúsculas',
  $q$INSERT INTO core.conta (plataforma_id,handle) VALUES (1,'PARTICIPANTE_1')$q$);

SELECT pg_temp.deve_falhar('07  id_nativo repetido na mesma plataforma',
  $q$INSERT INTO core.conta (plataforma_id,handle,id_nativo) VALUES (1,'outro','222222222222222222')$q$);

SELECT pg_temp.deve_falhar('08  reply sem ref_conta_id (perderia a aresta da conversa)',
  $q$INSERT INTO core.postagem (plataforma_id,id_nativo,autor_conta_id,created_at,tipo_ref)
     VALUES (1,'1490000000000000009',2,'2022-02-08 22:00+00','reply')$q$);

SELECT pg_temp.deve_falhar('09  postagem original carregando referência',
  $q$INSERT INTO core.postagem (plataforma_id,id_nativo,autor_conta_id,created_at,tipo_ref,ref_conta_id)
     VALUES (1,'1490000000000000010',2,'2022-02-08 22:00+00','original',1)$q$);

SELECT pg_temp.deve_falhar('10  postagem duplicada (plataforma, id_nativo)',
  $q$INSERT INTO core.postagem (plataforma_id,id_nativo,autor_conta_id,created_at,tipo_ref)
     VALUES (1,'1490000000000000001',2,'2022-02-08 22:00+00','original')$q$);

SELECT pg_temp.deve_falhar('11  tombstone sem data do evento',
  $q$UPDATE core.postagem SET situacao='removida_a_pedido' WHERE postagem_id=1$q$);

SELECT pg_temp.deve_falhar('12  contador de engajamento negativo',
  $q$UPDATE core.captura SET likes=-1 WHERE postagem_id=1$q$);

SELECT pg_temp.deve_falhar('13  stance fora do domínio {acusador,defensor,neutro}',
  $q$INSERT INTO core.classificacao (postagem_id,rotulo,modelo,versao)
     VALUES (3,'atacante','bertimbau-stance','v2b')$q$);

SELECT pg_temp.deve_falhar('14  confiança fora de [0,1]',
  $q$INSERT INTO core.classificacao (postagem_id,rotulo,modelo,versao,confianca)
     VALUES (3,'neutro','bertimbau-stance','v2b',1.5)$q$);

SELECT pg_temp.deve_falhar('15  reclassificação duplicada (mesma postagem/modelo/versão)',
  $q$INSERT INTO core.classificacao (postagem_id,rotulo,modelo,versao)
     VALUES (1,'neutro','bertimbau-stance','v2b')$q$);

SELECT pg_temp.deve_falhar('16  métrica duplicada com janela nula (NULLS NOT DISTINCT)',
  $q$INSERT INTO core.metrica (caso_id,nome,valor,versao_pipeline)
     VALUES (1,'qc_linhas_raw',9999,'v0.1.0')$q$);

SELECT pg_temp.deve_falhar('17  hashtag não normalizada (maiúscula)',
  $q$INSERT INTO core.postagem_hashtag (postagem_id,hashtag) VALUES (2,'Monark')$q$);

SELECT pg_temp.deve_falhar('18  hash SHA-256 malformado',
  $q$INSERT INTO core.arquivo_raw (coleta_id,uri,formato,hash_sha256,bytes)
     VALUES (1,'raw/x.jsonl','jsonl','naoehumhash',10)$q$);

SELECT pg_temp.deve_falhar('19  consentimento LGPD marcado sem data',
  $q$INSERT INTO core.contribuinte (pseudonimo,papel_sistema,consentimento_lgpd)
     VALUES ('sem_data','colaborador',true)$q$);

SELECT pg_temp.deve_falhar('20  ação de auditoria fora do vocabulário',
  $q$INSERT INTO core.auditoria (acao,entidade) VALUES ('fez_algo','core.caso')$q$);
ROLLBACK;

-- ---------------------------------------------------------------------
-- Testes de privilégio: o § 9 da Política, verificado
-- ---------------------------------------------------------------------
BEGIN;
SET LOCAL ROLE etl;
SELECT pg_temp.deve_falhar('21  etl NÃO pode apagar postagem (remoção é tombstone)',
  $q$DELETE FROM core.postagem WHERE postagem_id=3$q$);
RESET ROLE;
ROLLBACK;

BEGIN;
SET LOCAL ROLE etl;
UPDATE core.postagem SET situacao='apagada_na_fonte', situacao_em=now() WHERE postagem_id=3;
RESET ROLE;
DO $$ BEGIN RAISE NOTICE 'ok  22  etl PODE marcar tombstone (o caminho legítimo)'; END $$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_site;
SELECT pg_temp.deve_falhar('23  app_site NÃO enxerga core.postagem (texto e handles)',
  $q$SELECT count(*) FROM core.postagem$q$);
SELECT pg_temp.deve_falhar('24  app_site NÃO enxerga core.conta',
  $q$SELECT count(*) FROM core.conta$q$);
RESET ROLE;
ROLLBACK;

BEGIN;
SET LOCAL ROLE consultor;
SELECT pg_temp.deve_falhar('25  consultor é somente leitura',
  $q$INSERT INTO core.metrica (caso_id,nome,valor,versao_pipeline) VALUES (1,'x',1,'v')$q$);
RESET ROLE;
ROLLBACK;

DO $$ BEGIN RAISE NOTICE '--- 25 verificações concluídas ---'; END $$;
