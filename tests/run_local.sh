#!/usr/bin/env bash
# Recria um banco limpo, aplica o DDL e os GRANTs e roda a bateria de testes.
# Uso:  bash tests/run_local.sh  [nome_do_banco]
# Requer: PostgreSQL local em execução e psql no PATH.
set -euo pipefail

DB="${1:-scapegoat_test}"
PSQL="psql -h ${PGHOST:-/tmp} -U ${PGUSER:-postgres} -v ON_ERROR_STOP=1 -q"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "-> recriando banco $DB"
$PSQL -d postgres -c "DROP DATABASE IF EXISTS $DB;" -c "CREATE DATABASE $DB;"

echo "-> aplicando sql/ddl_v3_1_mvp.sql"
$PSQL -d "$DB" -f "$ROOT/sql/ddl_v3_1_mvp.sql"

echo "-> aplicando sql/roles_grants.sql"
$PSQL -d "$DB" -f "$ROOT/sql/roles_grants.sql"

echo "-> rodando tests/test_ddl_v3_1_mvp.sql"
$PSQL -d "$DB" -f "$ROOT/tests/test_ddl_v3_1_mvp.sql" 2>&1 \
  | grep -E 'NOTICE|ERROR' | sed 's/.*NOTICE:  //'

echo "-> tudo verde"
