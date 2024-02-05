import psycopg2
from getpass import getpass


class BaseDeDatos:
    # Credenciales de la base de datos como atributos estáticos
    HOST = xxxxxx
    DATABASE = xxxx
    USER = xxxxx
    PASSWORD = xxxx
    PORT = xxxxxx

    @staticmethod
    def conectar():
        return psycopg2.connect(
            host=BaseDeDatos.HOST,
            database=BaseDeDatos.DATABASE,
            user=BaseDeDatos.USER,
            password=BaseDeDatos.PASSWORD
        )

    @staticmethod
    def consultar(consulta_sql, parametros=None):
        with BaseDeDatos.conectar() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(consulta_sql, parametros)
                return cursor.fetchall()

    @staticmethod
    def ejecutar(consulta_sql, parametros=None):
        with BaseDeDatos.conectar() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(consulta_sql, parametros)
                conexion.commit()
