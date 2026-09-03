-- =====================================================================
-- Banco de casos de scapegoating — DDL v3.1-mvp (esquema congelado)
-- Projeto: scapegoat-pipeline · Autor: Carlos A. Paes da Silva
-- Alvo: PostgreSQL 16 (testado localmente) · Destino: Supabase
-- Congelado em 03/09/2026. Mudanças posteriores entram como MIGRAÇÃO.
--
-- Incorpora, além do v3.1 de 25/07:
--   · tombstone de conteúdo em POSTAGEM            (GGD aula 1, 12/08)
--   · tabela AUDITORIA                             (GGD aula 3, 14/08)
--   · ISO 639-1 (idioma) e ISO 3166-1 alpha-2 (país) (GGD aula 2, 13/08)
--   · colunas de curadoria do vínculo CONTA→ATOR   (GGD aula 2, 13/08)
--   · perfis de acesso e papel `etl`  -> ver roles_grants.sql
--
-- Os COMMENT ON deste arquivo são a fonte do Catálogo de Dados.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 0. Esquemas (zonas lógicas)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core;  -- ambiente operacional (3FN) — camadas 1-2
CREATE SCHEMA IF NOT EXISTS stg;   -- staging schema-on-read (JSONB), sem constraints
CREATE SCHEMA IF NOT EXISTS mart;  -- camada analítica (estrela) — views materializadas
CREATE SCHEMA IF NOT EXISTS pub;   -- views públicas: o que o site pode consumir

COMMENT ON SCHEMA core IS 'Ambiente operacional normalizado (v3.1). Contém texto e identificadores — NUNCA exposto ao site.';
COMMENT ON SCHEMA stg  IS 'Zona de aterrissagem JSONB. Sem constraints; conteúdo descartável após promoção.';
COMMENT ON SCHEMA mart IS 'Camada analítica dimensional (estrela) derivada de core.';
COMMENT ON SCHEMA pub  IS 'Views públicas: metadados, agregados, métricas e grafos pseudonimizados. Único esquema legível pelo site.';

-- ---------------------------------------------------------------------
-- CAMADA 1 — Identidade e casos
-- ---------------------------------------------------------------------

CREATE TABLE core.plataforma (
    plataforma_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome          text NOT NULL UNIQUE,
    dominio       text
);
COMMENT ON TABLE  core.plataforma        IS 'Lookup extensível de plataformas (X, Instagram, TikTok, imprensa...). Grão: uma plataforma.';
COMMENT ON COLUMN core.plataforma.nome   IS 'Nome curto e estável, em minúsculas (ex.: x, instagram).';

CREATE TABLE core.papel_tipo (
    papel_id  smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome      text NOT NULL UNIQUE,
    descricao text NOT NULL,
    criterio  text,
    versao    text NOT NULL DEFAULT 'v1'
);
COMMENT ON TABLE  core.papel_tipo          IS 'Taxonomia autoral de papéis (dado de referência da pesquisa). Grão: um tipo de papel. Versionada: mudar a definição exige nova versão.';
COMMENT ON COLUMN core.papel_tipo.criterio IS 'Critério operacional de atribuição — o que faz um ator receber este papel.';

CREATE TABLE core.contribuinte (
    contribuinte_id   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pseudonimo        text NOT NULL UNIQUE,
    email             text,
    papel_sistema     text NOT NULL
        CHECK (papel_sistema IN ('admin','pesquisador','colaborador')),
    consentimento_lgpd boolean NOT NULL DEFAULT false,
    consentido_em     timestamptz,
    criado_em         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT contribuinte_consentimento_datado
        CHECK (consentimento_lgpd = false OR consentido_em IS NOT NULL)
);
COMMENT ON TABLE  core.contribuinte IS 'Pessoas que operam ou alimentam o acervo. Grão: um contribuinte. Consentimento registrado com data (LGPD).';

CREATE TABLE core.caso (
    caso_id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug             text NOT NULL UNIQUE
        CHECK (slug ~ '^[a-z0-9_]{2,60}$'),
    titulo           text NOT NULL UNIQUE,
    descricao        text,
    idioma_principal char(2) NOT NULL
        CHECK (idioma_principal ~ '^[a-z]{2}$'),          -- ISO 639-1
    pais             char(2)
        CHECK (pais ~ '^[A-Z]{2}$'),                      -- ISO 3166-1 alpha-2
    ambito           text NOT NULL
        CHECK (ambito IN ('online','offline','misto')),
    data_inicio      date,
    data_fim         date,
    status           text NOT NULL DEFAULT 'rascunho'
        CHECK (status IN ('rascunho','em_moderacao','publicado','arquivado')),
    criado_por       integer REFERENCES core.contribuinte,
    criado_em        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT caso_periodo_coerente
        CHECK (data_fim IS NULL OR data_inicio IS NULL OR data_fim >= data_inicio)
);
COMMENT ON TABLE  core.caso        IS 'Episódio de scapegoating. Entidade de primeira classe do acervo. Grão: um caso.';
COMMENT ON COLUMN core.caso.slug   IS 'Identificador curto usado em URIs do lake e do site (ex.: monark, karol_conka).';
COMMENT ON COLUMN core.caso.status IS 'Ciclo de moderação: rascunho -> em_moderacao -> publicado -> arquivado. Só `publicado` aparece nas views de pub.';
COMMENT ON COLUMN core.caso.ambito IS 'online | offline | misto — o acervo admite casos sem rastro em plataforma.';

CREATE TABLE core.ator (
    ator_id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome_publico text,
    tipo         text NOT NULL
        CHECK (tipo IN ('pessoa','instituicao','coletivo','veiculo_midia')),
    anonimizado  boolean NOT NULL DEFAULT true,
    criado_em    timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE  core.ator             IS 'Pessoa, instituição, coletivo ou veículo — independente de plataforma. Grão: um ator. "Instituição" é TIPO de ator, nunca papel.';
COMMENT ON COLUMN core.ator.anonimizado IS 'true = nome nunca sai em nada público. Padrão true: anonimizar é a regra, nomear é a exceção justificada.';

CREATE TABLE core.conta (
    conta_id        integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plataforma_id   smallint NOT NULL REFERENCES core.plataforma,
    handle          text NOT NULL,
    id_nativo       text
        CHECK (id_nativo IS NULL OR id_nativo ~ '^[0-9]{1,25}$'),
    ator_id         integer REFERENCES core.ator,
    vinculado_por   integer REFERENCES core.contribuinte,
    vinculado_em    timestamptz,
    vinculo_criterio text,
    criado_em       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT conta_vinculo_justificado
        CHECK (ator_id IS NULL OR vinculo_criterio IS NOT NULL)
);
CREATE UNIQUE INDEX conta_plataforma_handle_uk
    ON core.conta (plataforma_id, lower(handle));
CREATE UNIQUE INDEX conta_plataforma_id_nativo_uk
    ON core.conta (plataforma_id, id_nativo) WHERE id_nativo IS NOT NULL;
CREATE INDEX conta_ator_ix ON core.conta (ator_id) WHERE ator_id IS NOT NULL;
COMMENT ON TABLE  core.conta                  IS 'Conta em uma plataforma. Grão: uma conta. Separada de ATOR porque a mesma pessoa tem várias contas e nem todo ator tem conta.';
COMMENT ON COLUMN core.conta.id_nativo        IS 'ID numérico imutável da plataforma. Handles mudam; este não. É a chave de resolução preferida.';
COMMENT ON COLUMN core.conta.vinculo_criterio IS 'MDM: por que esta conta foi vinculada a este ator. Obrigatório quando há vínculo — na dúvida, não vincular.';

CREATE TABLE core.papel (
    caso_id    integer  NOT NULL REFERENCES core.caso ON DELETE CASCADE,
    ator_id    integer  NOT NULL REFERENCES core.ator,
    papel_id   smallint NOT NULL REFERENCES core.papel_tipo,
    metodo     text NOT NULL
        CHECK (metodo IN ('manual','modelo','contribuicao')),
    confianca  numeric(4,3)
        CHECK (confianca IS NULL OR confianca BETWEEN 0 AND 1),
    observacao text,
    atribuido_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (caso_id, ator_id, papel_id)
);
COMMENT ON TABLE core.papel IS 'Papel de um ator EM UM CASO (vítima, acusador, líder da acusação, defensor...). Grão: caso x ator x papel. Ator sem papel = ausência de linha, nunca NULL. Interpretação curatorial contestável, não fato sobre a pessoa.';

-- ---------------------------------------------------------------------
-- CAMADA 2 — Coleta e conteúdo
-- ---------------------------------------------------------------------

CREATE TABLE core.coleta (
    coleta_id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    caso_id        integer NOT NULL REFERENCES core.caso ON DELETE CASCADE,
    plataforma_id  smallint REFERENCES core.plataforma,
    tipo           text NOT NULL
        CHECK (tipo IN ('api','scraping','manual','importacao')),
    consulta       text,
    ferramenta     text,
    periodo_inicio timestamptz,
    periodo_fim    timestamptz,
    executada_em   timestamptz NOT NULL DEFAULT now(),
    responsavel    integer REFERENCES core.contribuinte,
    CONSTRAINT coleta_periodo_coerente
        CHECK (periodo_fim IS NULL OR periodo_inicio IS NULL OR periodo_fim >= periodo_inicio)
);
CREATE INDEX coleta_caso_ix ON core.coleta (caso_id);
COMMENT ON TABLE  core.coleta          IS 'Um ato de coleta. Grão: uma execução. É a unidade de proveniência (PROV:Activity) de tudo que entra no acervo.';
COMMENT ON COLUMN core.coleta.consulta IS 'A consulta literal usada. Registrá-la é o que permite declarar o viés de coleta — os termos escolhidos enviesam stance e concentração.';
COMMENT ON COLUMN core.coleta.tipo     IS 'api | scraping | manual | importacao — a fonte é plugável: nada a jusante depende de qual foi.';

CREATE TABLE core.arquivo_raw (
    arquivo_id   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    coleta_id    integer NOT NULL REFERENCES core.coleta ON DELETE CASCADE,
    uri          text NOT NULL UNIQUE,
    formato      text NOT NULL
        CHECK (formato IN ('jsonl','json','csv','html','gexf','outro')),
    hash_sha256  char(64) NOT NULL
        CHECK (hash_sha256 ~ '^[0-9a-f]{64}$'),
    bytes        bigint NOT NULL CHECK (bytes > 0),
    registrado_em timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE  core.arquivo_raw     IS 'Catálogo da zona raw do lake. Grão: um objeto. Regra anti-swamp: nenhum objeto entra em raw/ sem linha aqui.';
COMMENT ON COLUMN core.arquivo_raw.uri IS 'URI do objeto no Storage (Supabase). Write-once: o bruto nunca é reescrito.';
COMMENT ON COLUMN core.arquivo_raw.hash_sha256 IS 'SHA-256 do arquivo como coletado. É o que torna a linhagem verificável, não apenas declarada.';

CREATE TABLE core.postagem (
    postagem_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plataforma_id   smallint NOT NULL REFERENCES core.plataforma,
    id_nativo       text NOT NULL,
    autor_conta_id  integer NOT NULL REFERENCES core.conta,
    created_at      timestamptz NOT NULL,
    texto           text,
    idioma          char(2) CHECK (idioma IS NULL OR idioma ~ '^[a-z]{2}$'),
    tipo_ref        text NOT NULL DEFAULT 'original'
        CHECK (tipo_ref IN ('original','retweet','quote','reply')),
    ref_postagem_id bigint REFERENCES core.postagem,
    ref_conta_id    integer REFERENCES core.conta,
    situacao        text NOT NULL DEFAULT 'ativa'
        CHECK (situacao IN ('ativa','apagada_na_fonte','removida_a_pedido')),
    situacao_em     timestamptz,
    carregada_em    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT postagem_nativa_uk UNIQUE (plataforma_id, id_nativo),
    CONSTRAINT postagem_original_sem_referencia
        CHECK (tipo_ref <> 'original'
               OR (ref_postagem_id IS NULL AND ref_conta_id IS NULL)),
    CONSTRAINT postagem_reply_preserva_aresta
        CHECK (tipo_ref <> 'reply' OR ref_conta_id IS NOT NULL),
    CONSTRAINT postagem_situacao_datada
        CHECK (situacao = 'ativa' OR situacao_em IS NOT NULL)
);
CREATE INDEX postagem_created_ix   ON core.postagem (created_at);
CREATE INDEX postagem_autor_ix     ON core.postagem (autor_conta_id);
CREATE INDEX postagem_ref_post_ix  ON core.postagem (ref_postagem_id) WHERE ref_postagem_id IS NOT NULL;
CREATE INDEX postagem_ref_conta_ix ON core.postagem (ref_conta_id)    WHERE ref_conta_id IS NOT NULL;
COMMENT ON TABLE  core.postagem                 IS 'Postagem em qualquer plataforma. Grão: uma postagem. Chave natural = (plataforma, id_nativo); a PK é surrogate porque IDs nativos são textuais e de 19 dígitos.';
COMMENT ON COLUMN core.postagem.tipo_ref        IS 'original | retweet | quote | reply. Regra de carga: para reply a FONTE DA VERDADE é in_reply_to_status_id, nunca o campo tweet_type do corpus (2.478 replies vinham como original).';
COMMENT ON COLUMN core.postagem.ref_conta_id    IS 'Conta respondida/citada. Preserva a aresta da conversa mesmo quando a postagem-pai está fora do corpus.';
COMMENT ON COLUMN core.postagem.situacao        IS 'Tombstone: ativa | apagada_na_fonte | removida_a_pedido. O acervo privado retém e marca; as views de pub removem. Atende LGPD (eliminação) e os termos da plataforma.';
COMMENT ON COLUMN core.postagem.idioma          IS 'ISO 639-1, preenchido pelo pipeline — não vem no corpus.';

CREATE TABLE core.captura (
    coleta_id    integer NOT NULL REFERENCES core.coleta ON DELETE CASCADE,
    postagem_id  bigint  NOT NULL REFERENCES core.postagem ON DELETE CASCADE,
    likes        integer NOT NULL DEFAULT 0 CHECK (likes    >= 0),
    retweets     integer NOT NULL DEFAULT 0 CHECK (retweets >= 0),
    quotes       integer NOT NULL DEFAULT 0 CHECK (quotes   >= 0),
    replies      integer NOT NULL DEFAULT 0 CHECK (replies  >= 0),
    engajamento  integer GENERATED ALWAYS AS (likes + retweets + quotes + replies) STORED,
    capturado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (coleta_id, postagem_id)
);
COMMENT ON TABLE  core.captura             IS 'Fotografia datada do engajamento de uma postagem numa coleta. Grão: coleta x postagem. Engajamento é do momento da coleta, não atributo eterno da postagem — por isso mora aqui, não em POSTAGEM.';
COMMENT ON COLUMN core.captura.engajamento IS 'Derivada (likes+retweets+quotes+replies). Nunca preencher à mão.';

CREATE TABLE core.mencao (
    postagem_id bigint  NOT NULL REFERENCES core.postagem ON DELETE CASCADE,
    conta_id    integer NOT NULL REFERENCES core.conta,
    PRIMARY KEY (postagem_id, conta_id)
);
CREATE INDEX mencao_conta_ix ON core.mencao (conta_id);
COMMENT ON TABLE core.mencao IS 'Aresta da rede de menções: uma postagem menciona uma conta. Grão: postagem x conta. É a matéria-prima de todas as métricas de rede.';

CREATE TABLE core.postagem_hashtag (
    postagem_id bigint NOT NULL REFERENCES core.postagem ON DELETE CASCADE,
    hashtag     text   NOT NULL CHECK (hashtag = lower(hashtag) AND hashtag <> ''),
    PRIMARY KEY (postagem_id, hashtag)
);
CREATE INDEX postagem_hashtag_tag_ix ON core.postagem_hashtag (hashtag);
COMMENT ON TABLE core.postagem_hashtag IS 'Hashtags extraídas do texto, normalizadas em minúsculas e sem o "#". Grão: postagem x hashtag.';

-- ---------------------------------------------------------------------
-- CAMADA 3 — Análise
-- ---------------------------------------------------------------------

CREATE TABLE core.classificacao (
    classificacao_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    postagem_id   bigint NOT NULL REFERENCES core.postagem ON DELETE CASCADE,
    esquema       text NOT NULL DEFAULT 'stance',
    rotulo        text NOT NULL,
    modelo        text NOT NULL,
    versao        text NOT NULL,
    confianca     numeric(4,3) CHECK (confianca IS NULL OR confianca BETWEEN 0 AND 1),
    classificado_em timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT classificacao_idempotente UNIQUE (postagem_id, esquema, modelo, versao),
    CONSTRAINT classificacao_stance_dominio
        CHECK (esquema <> 'stance' OR rotulo IN ('acusador','defensor','neutro'))
);
CREATE INDEX classificacao_postagem_ix ON core.classificacao (postagem_id);
COMMENT ON TABLE  core.classificacao        IS 'Rótulo atribuído a uma postagem por um modelo ou por anotação. Grão: postagem x esquema x modelo x versão. Reclassificar NÃO sobrescreve: gera nova linha, e a concordância entre versões vira métrica de qualidade.';
COMMENT ON COLUMN core.classificacao.rotulo IS 'Para esquema=stance: acusador | defensor | neutro. ATENÇÃO à dupla semântica — "acusador" aqui é postura DE UMA POSTAGEM; papel de um ATOR no caso é core.papel.';
COMMENT ON COLUMN core.classificacao.confianca IS 'Confiança do modelo. Sem ela não é possível declarar margem de erro nas respostas que dependem de stance.';

CREATE TABLE core.metrica (
    metrica_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    caso_id         integer NOT NULL REFERENCES core.caso ON DELETE CASCADE,
    nome            text NOT NULL,
    valor           double precision,
    janela_inicio   timestamptz,
    janela_fim      timestamptz,
    versao_pipeline text NOT NULL,
    calculada_em    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT metrica_janela_coerente
        CHECK (janela_fim IS NULL OR janela_inicio IS NULL OR janela_fim >= janela_inicio)
);
CREATE UNIQUE INDEX metrica_uk
    ON core.metrica (caso_id, nome, versao_pipeline, janela_inicio, janela_fim)
    NULLS NOT DISTINCT;
CREATE INDEX metrica_caso_nome_ix ON core.metrica (caso_id, nome);
COMMENT ON TABLE  core.metrica                 IS 'Métrica persistida com grão, janela e versão de pipeline. Grão: caso x nome x janela x versão. Métricas de qualidade da carga usam o prefixo qc_ (qc_linhas_raw, qc_duplicatas_removidas, ...).';
COMMENT ON COLUMN core.metrica.versao_pipeline IS 'Convenção do projeto: tag ou commit git do pipeline que produziu o número. É o que torna a linhagem auditável ponta a ponta.';

CREATE TABLE core.artefato (
    artefato_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    caso_id         integer NOT NULL REFERENCES core.caso ON DELETE CASCADE,
    tipo            text NOT NULL
        CHECK (tipo IN ('grafo','figura','tabela','relatorio')),
    uri             text NOT NULL UNIQUE,
    parametros      jsonb NOT NULL DEFAULT '{}'::jsonb,
    versao_pipeline text NOT NULL,
    gerado_em       timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE core.artefato IS 'Saída binária do pipeline (GEXF, PNG, CSV) guardada no Storage. Grão: um artefato. Os parâmetros que o geraram ficam em JSONB para reprodução.';

CREATE TABLE core.auditoria (
    auditoria_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ocorrido_em  timestamptz NOT NULL DEFAULT now(),
    usuario      text NOT NULL DEFAULT current_user,
    perfil       text,
    acao         text NOT NULL CHECK (acao IN (
                    'publicou_caso','arquivou_caso',
                    'aprovou_contribuicao','rejeitou_contribuicao',
                    'marcou_tombstone','atendeu_takedown','alterou_papel',
                    'exportou_dataset','concedeu_acesso','revogou_acesso')),
    entidade     text NOT NULL,
    entidade_id  text,
    motivo       text,
    detalhe      jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX auditoria_ocorrido_ix ON core.auditoria (ocorrido_em DESC);
COMMENT ON TABLE core.auditoria IS 'Registro de AÇÕES ADMINISTRATIVAS (o "A" de auditoria do DMBOK). Grão: uma ação. A Política de Dados §6 promete que o registro de um pedido de remoção é mantido — esta tabela é o que cumpre a promessa; um pedido de takedown é evento jurídico e a linha é a prova.';

-- ---------------------------------------------------------------------
-- STAGING — schema-on-read, sem constraints por desenho
-- ---------------------------------------------------------------------

CREATE TABLE stg.staging_raw (
    staging_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    arquivo_id  integer REFERENCES core.arquivo_raw,
    linha       integer,
    raw         jsonb NOT NULL,
    carregado_em timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX staging_raw_arquivo_ix ON stg.staging_raw (arquivo_id);
COMMENT ON TABLE stg.staging_raw IS 'Aterrissagem do JSONL bruto, uma linha por registro, sem tipagem. Deliberadamente sem constraints: a validação acontece no QC, entre esta tabela e core. Conteúdo descartável após a promoção.';

COMMIT;
