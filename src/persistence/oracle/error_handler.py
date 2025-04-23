#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia tratamento de erros específicos do Oracle para o contexto agrícola.

Este módulo fornece mecanismos para classificar, registrar e tratar os
diferentes tipos de erros que podem ocorrer durante operações no Oracle,
especialmente no contexto de monitoramento de perdas na colheita.
"""

import logging
import cx_Oracle
from typing import Dict, Any, Optional, Callable, Union

# Configuração de logging
logger = logging.getLogger(__name__)


class OracleError:
    """
    Representa um erro Oracle classificado para tratamento apropriado.

    Encapsula informações de um erro Oracle com metadados adicionais
    para facilitar o tratamento contextualizado em operações agrícolas.
    """

    # Códigos de erro por categoria
    CONNECTION_ERRORS = [3113, 3114, 12541, 12545, 12170, 12547]
    RESOURCE_ERRORS = [30, 1033, 1034, 1089, 4031]
    CONSTRAINT_ERRORS = [1, 2290, 2291, 2292, 2293]
    UNIQUE_ERRORS = [1, 1400]
    PERMISSION_ERRORS = [1031, 1017, 942]
    TIMEOUT_ERRORS = [1013, 12535, 12609]

    # Erros que podem ser recuperados com retry
    RECOVERABLE_ERRORS = CONNECTION_ERRORS + RESOURCE_ERRORS + TIMEOUT_ERRORS

    def __init__(self, exception: cx_Oracle.Error, context: Optional[Dict] = None):
        """
        Inicializa objeto de erro com classificação e contexto.

        Args:
            exception: Exceção Oracle original
            context: Informações adicionais sobre o contexto do erro
        """
        self.original_exception = exception
        self.context = context or {}

        # Extrai detalhes do erro
        error_obj, = exception.args
        self.code = error_obj.code
        self.message = error_obj.message
        self.offset = getattr(error_obj, 'offset', None)

        # Classifica o erro
        self._classify()

    def _classify(self) -> None:
        """
        Classifica o erro por tipo para tratamento adequado.
        """
        self.is_connection_error = self.code in self.CONNECTION_ERRORS
        self.is_resource_error = self.code in self.RESOURCE_ERRORS
        self.is_constraint_error = self.code in self.CONSTRAINT_ERRORS
        self.is_unique_error = self.code in self.UNIQUE_ERRORS
        self.is_permission_error = self.code in self.PERMISSION_ERRORS
        self.is_timeout_error = self.code in self.TIMEOUT_ERRORS

        # Determina se o erro é recuperável
        self.is_recoverable = self.code in self.RECOVERABLE_ERRORS

        # Categorização para contexto agrícola
        self.is_data_integrity_issue = self.is_constraint_error
        self.is_infrastructure_issue = (self.is_connection_error or
                                      self.is_resource_error or
                                      self.is_timeout_error)
        self.is_security_issue = self.is_permission_error

    def to_dict(self) -> Dict[str, Any]:
        """
        Converte o erro para formato de dicionário.

        Returns:
            Dict: Representação do erro como dicionário
        """
        return {
            "code": self.code,
            "message": self.message,
            "is_recoverable": self.is_recoverable,
            "category": self._get_category(),
            "context": self.context,
            "suggestion": self._get_suggestion()
        }

    def _get_category(self) -> str:
        """
        Retorna a categoria principal do erro.

        Returns:
            str: Categoria do erro
        """
        if self.is_connection_error:
            return "CONNECTION"
        elif self.is_resource_error:
            return "RESOURCE"
        elif self.is_constraint_error:
            return "CONSTRAINT"
        elif self.is_permission_error:
            return "PERMISSION"
        elif self.is_timeout_error:
            return "TIMEOUT"
        else:
            return "UNKNOWN"

    def _get_suggestion(self) -> str:
        """
        Retorna sugestão de ação baseada no tipo de erro.

        Returns:
            str: Sugestão para resolver o erro
        """
        if self.is_connection_error:
            return "Verifique a conectividade com o banco de dados"
        elif self.is_resource_error:
            return "Recursos insuficientes no banco de dados"
        elif self.is_constraint_error:
            return "Dados violam regras de integridade"
        elif self.is_unique_error:
            return "Registro duplicado"
        elif self.is_permission_error:
            return "Permissão insuficiente para a operação"
        elif self.is_timeout_error:
            return "Operação excedeu o tempo limite"
        else:
            return "Analise o código e mensagem para mais detalhes"

    def log(self, level: int = logging.ERROR) -> None:
        """
        Registra o erro no log com nível especificado.

        Args:
            level: Nível de log (default: ERROR)
        """
        context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())

        log_message = (
            f"Erro Oracle {self.code}: {self.message} "
            f"[Categoria: {self._get_category()}] "
            f"[Recuperável: {self.is_recoverable}] "
        )

        if context_str:
            log_message += f"[Contexto: {context_str}]"

        logger.log(level, log_message)

        if self.is_recoverable:
            logger.info(f"Sugestão: {self._get_suggestion()}")


class ErrorHandler:
    """
    Gerencia tratamento de erros para operações Oracle.

    Fornece estratégias para lidar com diferentes tipos de erros
    Oracle no contexto de aplicações agrícolas.
    """

    def __init__(self):
        """
        Inicializa o gerenciador de erros.
        """
        # Handlers registrados para tipos específicos de erro
        self.handlers = {
            "CONNECTION": self._handle_connection_error,
            "RESOURCE": self._handle_resource_error,
            "CONSTRAINT": self._handle_constraint_error,
            "PERMISSION": self._handle_permission_error,
            "TIMEOUT": self._handle_timeout_error,
            "UNKNOWN": self._handle_unknown_error
        }

    def process_error(self, exception: Union[cx_Oracle.Error, Exception],
                     context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Processa um erro, classificando e aplicando estratégia apropriada.

        Args:
            exception: Exceção a ser processada
            context: Informações contextuais do erro

        Returns:
            Dict: Resultado do processamento do erro
        """
        # Converte para OracleError se for cx_Oracle.Error
        if isinstance(exception, cx_Oracle.Error):
            oracle_error = OracleError(exception, context)
            oracle_error.log()

            # Aplica handler específico
            category = oracle_error._get_category()
            handler = self.handlers.get(category, self._handle_unknown_error)
            return handler(oracle_error)
        else:
            # Erros não-Oracle
            logger.error(f"Erro não-Oracle: {str(exception)}", exc_info=True)
            return {
                "status": "error",
                "message": str(exception),
                "category": "NON_ORACLE",
                "is_recoverable": False,
                "action_taken": "logged"
            }

    def _handle_connection_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro de conexão.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        return {
            "status": "error",
            "message": f"Erro de conexão: {error.message}",
            "category": "CONNECTION",
            "is_recoverable": True,
            "action_taken": "connection_retry_suggested",
            "suggestion": "Verificar conectividade de rede e disponibilidade do servidor"
        }

    def _handle_resource_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro de recursos.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        return {
            "status": "error",
            "message": f"Erro de recursos: {error.message}",
            "category": "RESOURCE",
            "is_recoverable": True,
            "action_taken": "resource_wait_suggested",
            "suggestion": "Aguardar liberação de recursos ou otimizar consulta"
        }

    def _handle_constraint_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro de violação de constraint.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        # Identifica qual constraint foi violada
        constraint_info = self._extract_constraint_info(error)

        return {
            "status": "error",
            "message": f"Violação de integridade: {error.message}",
            "category": "CONSTRAINT",
            "is_recoverable": False,
            "constraint_type": constraint_info.get("type", "unknown"),
            "constraint_name": constraint_info.get("name", "unknown"),
            "action_taken": "data_validation_suggested",
            "suggestion": "Revisar dados para garantir integridade"
        }

    def _handle_permission_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro de permissão.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        return {
            "status": "error",
            "message": f"Erro de permissão: {error.message}",
            "category": "PERMISSION",
            "is_recoverable": False,
            "action_taken": "permission_escalation_needed",
            "suggestion": "Verificar privilégios do usuário do banco de dados"
        }

    def _handle_timeout_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro de timeout.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        return {
            "status": "error",
            "message": f"Operação excedeu tempo limite: {error.message}",
            "category": "TIMEOUT",
            "is_recoverable": True,
            "action_taken": "query_optimization_suggested",
            "suggestion": "Otimizar consulta ou aumentar timeout"
        }

    def _handle_unknown_error(self, error: OracleError) -> Dict[str, Any]:
        """
        Trata erro desconhecido.

        Args:
            error: Erro a ser tratado

        Returns:
            Dict: Resultado do tratamento
        """
        return {
            "status": "error",
            "message": f"Erro desconhecido: {error.message}",
            "code": error.code,
            "category": "UNKNOWN",
            "is_recoverable": False,
            "action_taken": "logged",
            "suggestion": "Analisar logs para identificar causa"
        }

    def _extract_constraint_info(self, error: OracleError) -> Dict[str, str]:
        """
        Extrai informações sobre a constraint violada.

        Args:
            error: Erro de constraint

        Returns:
            Dict: Informações da constraint
        """
        # Inicializa com valores default
        info = {
            "type": "unknown",
            "name": "unknown"
        }

        # ORA-00001: unique constraint violated
        if error.code == 1:
            info["type"] = "unique"
            # Tenta extrair nome da constraint da mensagem
            parts = error.message.split(".")
            if len(parts) > 1:
                info["name"] = parts[1].strip().split("(")[0].strip()

        # ORA-02290: check constraint violated
        elif error.code == 2290:
            info["type"] = "check"
            parts = error.message.split(".")
            if len(parts) > 1:
                info["name"] = parts[1].strip()

        # ORA-02291: integrity constraint violated - parent key not found
        elif error.code == 2291:
            info["type"] = "foreign_key"
            parts = error.message.split(".")
            if len(parts) > 1:
                info["name"] = parts[1].strip().split("(")[0].strip()

        # ORA-02292: integrity constraint violated - child record found
        elif error.code == 2292:
            info["type"] = "parent_key"
            parts = error.message.split(".")
            if len(parts) > 1:
                info["name"] = parts[1].strip()

        return info


class RetryPolicy:
    """
    Define política de retry para operações recuperáveis.

    Implementa diferentes estratégias de retry com backoff
    para operações que podem ser recuperadas após falhas.
    """

    def __init__(self, max_attempts: int = 3,
                initial_delay: float = 1.0,
                backoff_factor: float = 2.0,
                max_delay: float = 30.0):
        """
        Inicializa política de retry.

        Args:
            max_attempts: Número máximo de tentativas
            initial_delay: Atraso inicial entre tentativas (segundos)
            backoff_factor: Fator multiplicativo para backoff exponencial
            max_delay: Atraso máximo entre tentativas (segundos)
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay

    def should_retry(self, error: OracleError, attempt: int) -> bool:
        """
        Determina se operação deve ser tentada novamente.

        Args:
            error: Erro ocorrido
            attempt: Número da tentativa atual

        Returns:
            bool: True se deve tentar novamente, False caso contrário
        """
        # Não tenta novamente se atingiu máximo de tentativas
        if attempt >= self.max_attempts:
            return False

        # Só tenta novamente se erro for recuperável
        return error.is_recoverable

    def get_delay(self, attempt: int) -> float:
        """
        Calcula tempo de espera para próxima tentativa.

        Implementa backoff exponencial com jitter.

        Args:
            attempt: Número da tentativa atual

        Returns:
            float: Tempo de espera em segundos
        """
        import random

        # Backoff exponencial
        delay = self.initial_delay * (self.backoff_factor ** attempt)

        # Limita ao máximo configurado
        delay = min(delay, self.max_delay)

        # Adiciona jitter (variação aleatória) para evitar thundering herd
        jitter = random.uniform(0.8, 1.2)
        delay *= jitter

        return delay


def with_error_handling(func: Callable) -> Callable:
    """
    Decorador para adicionar tratamento de erro a funções.

    Encapsula função com tratamento padrão de erros Oracle.

    Args:
        func: Função a ser decorada

    Returns:
        Callable: Função decorada com tratamento de erro
    """
    def wrapper(*args, **kwargs):
        handler = ErrorHandler()
        try:
            return func(*args, **kwargs)
        except cx_Oracle.Error as e:
            context = {
                "function": func.__name__,
                "args": str(args),
                "kwargs": str(kwargs)
            }
            error_info = handler.process_error(e, context)

            # Re-lança exceção com informações adicionais
            raise RuntimeError(f"Erro Oracle: {error_info['message']}") from e
        except Exception as e:
            error_info = handler.process_error(e)
            raise RuntimeError(f"Erro não esperado: {str(e)}") from e

    return wrapper


def with_retry(retry_policy: Optional[RetryPolicy] = None) -> Callable:
    """
    Decorador para adicionar retry a funções.

    Tenta executar função múltiplas vezes em caso de erros recuperáveis.

    Args:
        retry_policy: Política de retry (usa padrão se None)

    Returns:
        Callable: Decorador configurado
    """
    retry_policy = retry_policy or RetryPolicy()

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            import time

            attempt = 0
            last_error = None

            while attempt < retry_policy.max_attempts:
                try:
                    return func(*args, **kwargs)
                except cx_Oracle.Error as e:
                    error = OracleError(e, {"function": func.__name__})
                    last_error = error

                    attempt += 1
                    error.log()

                    if retry_policy.should_retry(error, attempt):
                        delay = retry_policy.get_delay(attempt)
                        logger.warning(
                            f"Tentativa {attempt} falhou, tentando novamente "
                            f"em {delay:.2f}s: {error.message}"
                        )
                        time.sleep(delay)
                    else:
                        break
                except Exception as e:
                    # Não faz retry para erros não-Oracle
                    logger.error(f"Erro não-Oracle: {str(e)}", exc_info=True)
                    raise

            # Se chegou aqui, todas as tentativas falharam
            if last_error:
                error_handler = ErrorHandler()
                error_info = error_handler.process_error(last_error.original_exception)

                raise RuntimeError(
                    f"Todas as {attempt} tentativas falharam. "
                    f"Último erro: {error_info['message']}"
                ) from last_error.original_exception
            else:
                raise RuntimeError(f"Todas as {attempt} tentativas falharam.")

        return wrapper

    return decorator
