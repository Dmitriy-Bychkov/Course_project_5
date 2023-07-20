from src.db_manager import DBManager
from src.hh_api import HHApi
from utils.config import config


def users_interaction_logic():
    """
    Функция основной логики взаимодействия с пользователем.
    Исподьзуется для вызова в главном файле запуска - main
    """

    # Создаем рабочие переменные и экземпляры классов
    hh_data = HHApi()
    params = config()

    # Блок приветсвия
    print('Добро пожаловать в парсер вакансий с сайта hh.ru!\n'
          'Я соберу вакансии определенных компаний и сохраню полученные результаты в базу данных,\n'
          'а также помогу отобрать вакансии по ключевому поисковому запросу и зарплате')
    print('.' * 50)
    input("Нажмите клавишу 'Enter' для начала парсинга!\n")

    # Блок загрузки данных по АПИ
    # print('Загружаю список компаний и их вакансий...')
    companies_data = hh_data.get_hh_data()
    print('Список успешно загружен!')
    print(f"Всего было загружено: {len(companies_data) * len(companies_data[0]['items'])} вакансий.\n")

    # Блок создания базы данных и заполнения ее таблиц
    while True:
        database_name = input('Введите название базы данных для сохранения результатов:\n').lower()
        if database_name[0] in '0123456789':
            print('Название базы не должно начинаться с цифры, попробуйте повторить ввод!')

        else:
            DBManager.create_database(database_name, params)
            print('Создаю базу данных и сохраняю вакансии...')
            print(f'База данных "{database_name}" создана успешно!')
            DBManager.save_data_to_database(companies_data, database_name, params)
            print('Все вакансии сохранены.')
            break

    # Предлагаем пользователю поработать с базой данных
    print('.' * 50)
    print('Теперь вы можете вывести на экран данные из базы\n'
          '(или просто введите "exit" для выхода)\n')

    # создаем экземпляр класса DBManager для работы с его методами
    db_manager = DBManager(database_name, params)
    while True:
        user_select = input('Введите соответствующую цифру для дальнейшей работы:\n'
                            '1 - Вывести список всех компаний и количество вакансий у каждой компании\n'
                            '2 - Вывести полный список всех компаний и их вакансий\n'
                            '3 - Вывести вакансии с рассчитанной средней зарплатой\n'
                            '4 - Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
                            '5 - Вывести список всех вакансий по ключевому слову, например “python”.\n'
                            ).lower()
        if user_select == 'exit':
            quit()

        if user_select[0] not in '12345':
            print('Вы ввели некорректный номер!\n')

        else:
            if user_select == '1':
                db_manager.get_companies_and_vacancies_count()
                break

            elif user_select == '2':
                db_manager.get_all_vacancies()
                break

            elif user_select == '3':
                db_manager.get_avg_salary()
                break

            elif user_select == '4':
                db_manager.get_vacancies_with_higher_salary()
                break

            elif user_select == '5':
                user_keyword = input('Введите слово для поиска:\n')
                db_manager.get_vacancies_with_keyword(user_keyword)
                break
