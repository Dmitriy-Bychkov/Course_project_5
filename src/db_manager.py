from typing import Any
import psycopg2


class DBManager:
    """Класс для подключения к БД Postgres и работы с ней"""

    def __init__(self, database_name=None):
        self.database_name = database_name

    def __repr__(self):
        return f'{self.__class__.__name__}()'

    @staticmethod
    def get_companies_and_vacancies_count(database_name: str, params: dict):
        """
        Получает список всех компаний и
        количество вакансий у каждой компании
        """

        conn = psycopg2.connect(dbname=database_name, **params)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM companies
            """)
        rows = cur.fetchall()

        for data in rows:
            print(f'id: {data[0]}, company: {data[1]}, vac_count: {data[2]}')

        cur.close()
        conn.close()

    @staticmethod
    def get_all_vacancies(database_name: str, params: dict):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию
        """

        conn = psycopg2.connect(dbname=database_name, **params)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url 
            FROM vacancies
            """)
        rows = cur.fetchall()

        for data in rows:
            print(
                f'company: {data[0]}, vacancy: {data[1]}, salary_from: {data[2]}, '
                f'salary_to: {data[3]}, url: {data[4]}')

        cur.close()
        conn.close()

    @staticmethod
    def get_avg_salary(database_name: str, params: dict):
        """Получает среднюю зарплату по вакансиям"""

        conn = psycopg2.connect(dbname=database_name, **params)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT company_name, vacancy_name, salary_from, salary_to, CASE
            WHEN salary_from IS NOT NULL AND salary_to IS NULL THEN salary_from
            WHEN salary_from IS NULL AND salary_to IS NOT NULL THEN salary_to
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2
            ELSE NULL
            END as avg_salary, vacancy_url
            FROM vacancies
            """)
        rows = cur.fetchall()

        for data in rows:
            print(
                f'company: {data[0]}, vacancy: {data[1]}, salary_from: {data[2]}, '
                f'salary_to: {data[3]}, avg_salary: {data[4]},url: {data[5]}')

        cur.close()
        conn.close()

    @staticmethod
    def get_vacancies_with_higher_salary(database_name: str, params: dict):
        """
        Получает список всех вакансий, у которых
        зарплата выше средней по всем вакансиям
        """

        conn = psycopg2.connect(dbname=database_name, **params)
        cur = conn.cursor()
        cur.execute(
            """
            WITH salaries AS (
            SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url,
            CASE
            WHEN salary_from IS NOT NULL AND salary_to IS NULL THEN salary_from
            WHEN salary_from IS NULL AND salary_to IS NOT NULL THEN salary_to
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2
            ELSE NULL
            END as avg_salary
            FROM vacancies
            )
            SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url
            FROM salaries
            WHERE avg_salary > (SELECT AVG(avg_salary) FROM salaries)
            """)
        rows = cur.fetchall()

        for data in rows:
            print(
                f'company: {data[0]}, vacancy: {data[1]}, salary_from: {data[2]}, '
                f'salary_to: {data[3]}, url: {data[4]}')

        cur.close()
        conn.close()

    @staticmethod
    def get_vacancies_with_keyword(database_name: str, params: dict, keyword: str):
        """
        Получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например “python”
        """

        conn = psycopg2.connect(dbname=database_name, **params)
        cur = conn.cursor()
        query = """
                SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url
                FROM vacancies 
                WHERE lower(vacancy_name) LIKE lower(%s)
                """
        search_keyword = f"%{keyword}%"
        cur.execute(query, (search_keyword,))
        rows = cur.fetchall()

        if len(rows) == 0:
            print("Нет результатов по вашему запросу")
        else:
            for data in rows:
                print(
                    f'company: {data[0]}, vacancy: {data[1]}, salary_from: {data[2]}, '
                    f'salary_to: {data[3]}, url: {data[4]}')

        cur.close()
        conn.close()

    @staticmethod
    def create_database(database_name: str, params: dict):
        """
        Создание базы данных и таблиц для сохранения
        данных о вакансиях и компаниях.
        """

        conn = psycopg2.connect(dbname='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
        cur.execute(f"CREATE DATABASE {database_name}")

        conn.close()

        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE companies (
                            company_id INTEGER PRIMARY KEY,
                            company_name VARCHAR NOT NULL,
                            vacancies_count INTEGER
                        )
                    """)

        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE vacancies (
                            vacancy_id SERIAL PRIMARY KEY,
                            company_id INT REFERENCES companies(company_id),
                            company_name VARCHAR NOT NULL,
                            vacancy_name VARCHAR NOT NULL,
                            salary_from INTEGER,
                            salary_to INTEGER,
                            currency VARCHAR(10),
                            vacancy_url TEXT
                        )
                    """)

        conn.commit()
        conn.close()

    @staticmethod
    def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
        """Сохранение данных о компаниях и вакансиях в базу данных."""

        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            for company in data:
                company_id = company['items'][0]['employer']['id']
                vacancies_count = company['found']
                company_name = company['items'][0]['employer']['name']

                cur.execute(
                    """
                    INSERT INTO companies (company_id, company_name, vacancies_count)
                    VALUES (%s, %s, %s)
                    RETURNING company_id
                    """,
                    (company_id, company_name, vacancies_count)
                )
                # channel_id = cur.fetchone()[0]
                for vacancy in company['items']:
                    vacancy_name = vacancy['name']

                    # Блок проверки на наличие указания зарплаты и валюты
                    if vacancy['salary'] is None:
                        salary_from = None
                        salary_to = None
                        currency = None
                    elif not vacancy['salary']['from']:
                        salary_from = None
                        salary_to = vacancy['salary']['to']
                        currency = vacancy['salary']['currency']
                    elif not vacancy['salary']['to']:
                        salary_from = vacancy['salary']['from']
                        salary_to = None
                        currency = vacancy['salary']['currency']
                    else:
                        salary_from = vacancy['salary']['from']
                        salary_to = vacancy['salary']['to']
                        currency = vacancy['salary']['currency']

                    vacancy_url = vacancy['alternate_url']

                    cur.execute(
                        """
                        INSERT INTO vacancies (company_id, company_name, vacancy_name, 
                        salary_from, salary_to, currency, vacancy_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (company_id, company_name, vacancy_name, salary_from, salary_to, currency, vacancy_url)
                    )

        conn.commit()
        conn.close()
