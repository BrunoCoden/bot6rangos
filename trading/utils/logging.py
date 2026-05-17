import logging
import os


def get_logger(name: str) -> logging.Logger:
    """
    Devuelve un logger configurado con nivel INFO por defecto.
    Se respeta la variable de entorno TRADING_LOG_LEVEL si está definida.
    """
    level_name = os.getenv("TRADING_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s] %(message)s")
        )
        logger.addHandler(handler)
    else:
        logger.setLevel(level)
    return logger
