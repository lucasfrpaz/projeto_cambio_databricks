from datetime import datetime, timezone
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


TABELA_AUDITORIA = (
    "databricks_cata_managed.audit.execucao_task"
)


def iniciar_auditoria(
    spark: SparkSession,
    run_id: str,
    job_name: str,
    task_name: str,
    camada: str,
    tabela_origem: str,
    tabela_destino: str,
    batch_id: str,
) -> datetime:
    inicio_execucao = datetime.now(timezone.utc)

    registro = {
        "run_id": str(run_id),
        "job_name": job_name,
        "task_name": task_name,
        "camada": camada,
        "tabela_origem": tabela_origem,
        "tabela_destino": tabela_destino,
        "batch_id": batch_id,
        "status": "RUNNING",
        "linhas_lidas": None,
        "linhas_inseridas": None,
        "linhas_atualizadas": None,
        "linhas_rejeitadas": None,
        "inicio_execucao": inicio_execucao,
        "fim_execucao": None,
        "duracao_segundos": None,
        "mensagem_erro": None,
        "data_auditoria": inicio_execucao,
    }

    schema_auditoria = spark.table(
        TABELA_AUDITORIA
    ).schema

    df_registro = spark.createDataFrame(
        [registro],
        schema=schema_auditoria,
    )

    delta_auditoria = DeltaTable.forName(
        spark,
        TABELA_AUDITORIA,
    )

    (
        delta_auditoria.alias("t")
        .merge(
            df_registro.alias("s"),
            """
            t.run_id = s.run_id
            AND t.task_name = s.task_name
            AND t.tabela_destino = s.tabela_destino
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return inicio_execucao


def obter_metricas_delta(
    spark: SparkSession,
    tabela_destino: str,
) -> dict:
    historico = (
        spark.sql(
            f"DESCRIBE HISTORY {tabela_destino} LIMIT 1"
        )
        .select("operation", "operationMetrics")
        .first()
    )

    if historico is None:
        return {
            "linhas_lidas": 0,
            "linhas_inseridas": 0,
            "linhas_atualizadas": 0,
        }

    operacao = historico["operation"]
    metricas = historico["operationMetrics"] or {}

    if operacao == "MERGE":
        return {
            "linhas_lidas": int(
                metricas.get("numSourceRows", 0)
            ),
            "linhas_inseridas": int(
                metricas.get(
                    "numTargetRowsInserted",
                    0,
                )
            ),
            "linhas_atualizadas": int(
                metricas.get(
                    "numTargetRowsUpdated",
                    0,
                )
            ),
        }

    linhas_escritas = int(
        metricas.get("numOutputRows", 0)
    )

    return {
        "linhas_lidas": linhas_escritas,
        "linhas_inseridas": linhas_escritas,
        "linhas_atualizadas": 0,
    }


def finalizar_auditoria(
    spark: SparkSession,
    run_id: str,
    task_name: str,
    tabela_destino: str,
    inicio_execucao: datetime,
    status: str,
    linhas_lidas: int = 0,
    linhas_inseridas: int = 0,
    linhas_atualizadas: int = 0,
    linhas_rejeitadas: int = 0,
    mensagem_erro: Optional[str] = None,
) -> None:
    fim_execucao = datetime.now(timezone.utc)

    duracao_segundos = int(
        (fim_execucao - inicio_execucao).total_seconds()
    )

    erro_resumido = (
        mensagem_erro[:4000]
        if mensagem_erro
        else None
    )

    delta_auditoria = DeltaTable.forName(
        spark,
        TABELA_AUDITORIA,
    )

    condicao = (
        (F.col("run_id") == str(run_id))
        & (F.col("task_name") == task_name)
        & (F.col("tabela_destino") == tabela_destino)
    )

    delta_auditoria.update(
        condition=condicao,
        set={
            "status": F.lit(status),
            "linhas_lidas": F.lit(
                int(linhas_lidas)
            ),
            "linhas_inseridas": F.lit(
                int(linhas_inseridas)
            ),
            "linhas_atualizadas": F.lit(
                int(linhas_atualizadas)
            ),
            "linhas_rejeitadas": F.lit(
                int(linhas_rejeitadas)
            ),
            "fim_execucao": F.lit(
                fim_execucao
            ),
            "duracao_segundos": F.lit(
                duracao_segundos
            ),
            "mensagem_erro": F.lit(
                erro_resumido
            ).cast("string"),
            "data_auditoria": F.lit(
                fim_execucao
            ),
        },
    )