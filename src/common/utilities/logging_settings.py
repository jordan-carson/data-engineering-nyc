# import sys
# import logging
# from logging.config import dictConfig
#
# dummy = print({'dev': 1})
#
# print(type(dummy))
# def initialize_logger(location, log_name):
#
#     location = r'\\Users\jordancarson\Projects'
#
#     logging_config = dict(
#         version=1,
#         formatters={
#             'verbose': {
#                 'format': ("[%(asctime)s] %(levelname)s "
#                            "[%(name)s:%(lineno)s] %(message)s"),
#                 'datefmt': "%d/%b/%Y %H:%M:%S",
#             },
#             'simple': {
#                 'format': '%(levelname)s %(message)s',
#             },
#         },
#         handlers={
#             # 'api-logger': {'class': 'logging.handlers.RotatingFileHandler',
#             #                'formatter': 'verbose',
#             #                'level': logging.DEBUG,
#             #                'filename': 'logs/api.log',
#             #                'maxBytes': 52428800,
#             #                'backupCount': 7},
#             # 'batch-process-logger': {'class': 'logging.handlers.RotatingFileHandler',
#             #                          'formatter': 'verbose',
#             #                          'level': logging.DEBUG,
#             #                          'filename': 'logs/batch.log',
#             #                          'maxBytes': 52428800,
#             #                          'backupCount': 7},
#             # 'development-logger': {'class': 'logging.handlers.RotatingFileHandler',
#             #                        'formatter': 'verbose',
#             #                        'level': logging.DEBUG,
#             #                        'filename': 'logs/development.log',
#             #                        'maxBytes': 52428800,
#             #                        'backupCount': 7},
#             f'{log_name}-logger': {'class': 'logging.handlers.RotatingFileHandler',
#                                    'formatter': 'verbose',
#                                    'level': logging.DEBUG,
#                                    'filename': f'logs/{log_name}.log', #{location}/
#                                    'maxBytes': 52428800,
#                                    'backupCount': 7},
#             'console': {
#                 'class': 'logging.StreamHandler',
#                 'level': 'DEBUG',
#                 'formatter': 'simple',
#                 'stream': sys.stdout,
#             },
#         },
#         loggers={
#             # 'api_logger': {
#             #     'handlers': ['api-logger', 'console'],
#             #     'level': logging.DEBUG
#             # },
#             # 'batch_process_logger': {
#             #     'handlers': ['batch-process-logger', 'console'],
#             #     'level': logging.DEBUG
#             # },
#             # 'development_logger': {
#             #     'handlers': ['development-logger', 'console'],
#             #     'level': logging.DEBUG
#             # },
#             f'{log_name}_logger': {
#                 'handlers': [f'{log_name}-logger', 'console'],
#                 'level': logging.DEBUG
#             },
#
#         }
#     )
#     dictConfig(logging_config)
#
#     logger = logging.getLogger(f'{log_name}_logger')
#     return logger
    # api_logger = logging.getLogger('api_logger')
    # batch_process_logger = logging.getLogger('batch_process_logger')
    # development_logger = logging.getLogger('development_logger')
import logging
import sys

logformat = "%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s"
datefmt = "%m-%d %H:%M"

logging.basicConfig(filename="app.log", level=logging.INFO, filemode="w",
                    format=logformat, datefmt=datefmt)

stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(logging.Formatter(fmt=logformat, datefmt=datefmt))

logger = logging.getLogger("app")
logger.addHandler(stream_handler)

logger.info("information")
logger.warning("warning")


def fun():
    logger.info(" fun inf")
    logger.warning("fun warn")

if __name__ == "__main__":
    fun()
# initialize_logger('', 'examplelog2')
#
# logging.info('Log One')
# logging.info('Log Second')
# logging.info('Log One')
# logging.info('Log Second')
# logging.info('Log One')
# logging.info('Log Second')
# logging.info('Log One')
# logging.info('Log Second')
# logging.info('Log One')
# logging.info('Log Second')
# logging.info('Log One')
# logging.info('Log Second')