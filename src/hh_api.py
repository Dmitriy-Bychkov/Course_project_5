from typing import Any
from src.companies_list import companies_ids
from tqdm import tqdm
import requests


class HHApi:
    """Класс для подключения к API hh.ru и работы с ним"""

    def __init__(self, per_page=100):
        self.api_url = 'https://api.hh.ru/vacancies'
        self.per_page = per_page

    def __repr__(self):
        return f'{self.__class__.__name__}()'

    def get_hh_data(self) -> list[dict[str, Any]]:
        """Подключается к API hh.ru и получает данные о компаниях и их вакансиях"""

        company_data = []

        # Используем модуль tqdm для отображения прогресса выполнения метода
        for company_id in tqdm(companies_ids,
                               desc='Загружаю список компаний и их вакансий'):
            params = {
                "employer_id": company_id,
                "per_page": self.per_page,
            }

            response = requests.get(self.api_url, params)

            if response.status_code == 200:
                data = response.json()
                company_data.append(data)
            else:
                print(f'Ошибка подключения к серверу - {response.status_code}')

        return company_data
