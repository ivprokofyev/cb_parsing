#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
actual_dir = os.getcwd()
pathMyModules = actual_dir+'/modules'
sys.path.append(pathMyModules)

import datetime
from sql_main import SqlShell

__all__ = ('conv_db_auto', )  # указать все функции, которые имеются в модули


def conv_db_auto():
    # log запуска
    log = open(actual_dir + '/log.txt', 'a', encoding='utf-8')
    log.write('\n' + str(datetime.datetime.now()) + ': Запуск конвертера')

    sqlshell = SqlShell()

    try:
        # все записи из input_parsers
        input_parsers = sqlshell.getConvDb()

        len_before = len(input_parsers)
        data_none = []

        # проверка значений на наличие id
        for data in input_parsers:
            # проверка аэропортов - check_airports
            for airport in data[:11]:
                if airport is not None:
                    check_airport = sqlshell.checkAirportsConvDb(airport=airport)
                    if check_airport is None:
                        # print('Не найден код аэропорта')
                        data_none.append(data)
            # проверка авиакомпаний - check_airlines
            for airline in data[12:15]:
                if airline is not None:
                    check_airline = sqlshell.checkAirlinesConvDb(airline=airline)
                    if check_airline is None:
                        # print('Не найден код авиакомпании')
                        data_none.append(data)
            # проверка типа груза - check_type
            if data[-2] is not None:
                check_type = sqlshell.checkTypeConvDb(type=data[-2])
                if check_type is None:
                    # print('Не найден код типа груза')
                    data_none.append(data)

        input_parsers = list(set(input_parsers) - set(data_none))
        len_after = len(input_parsers)
        if len_before - len_after > 0:
            log.write('\n' + str(datetime.datetime.now()) + ': В авто режиме не обработано {} записи'.format(len_before - len_after))

        # количество записей до обработки
        count_row_before = sqlshell.countRoutesConvDB()

        for data in input_parsers:
            id_record_parsers = data[-1]
            id_carrier = data[-3]
            for x in data[:4]:
                if x is not None:
                    if x == 'Москва':
                        id_airport_from = 'Шереметьево'
                    else:
                        id_airport_from = x
                    break
            for x in data[4:8]:
                if x is not None:
                    if x == 'Москва':
                        id_airport_to = 'Домодедово'
                    else:
                        id_airport_to = x
                    break
            id_airport_transit = 'Отсутствует'
            for x in data[8:12]:
                if x is not None:
                    id_airport_transit = x
                    break
            id_airline = 'Отсутствует'
            for x in data[12:15]:
                if x is not None:
                    id_airline = x
                    break
            id_cargo_type = data[16]

            id_route = sqlshell.routeConvDb(id_record_parsers=id_record_parsers,
                                            id_carrier=id_carrier,
                                            id_airport_from=id_airport_from,
                                            id_airport_to=id_airport_to,
                                            id_airport_transit=id_airport_transit,
                                            id_airline=id_airline,
                                            id_cargo_type=id_cargo_type)
            if len(id_route) == 0:
                # print('Найден дубль, проверка следующей записи')
                # print(data)
                pass
            else:
                id_parsers = id_route[0][1]
                # проверяем на наличие весовых категорий в парсинге
                for x in range(1, 11):
                    a = sqlshell.rateRowConvDb(number=x, id_parsers=id_parsers)
                    if a is None:
                        break
                    else:
                        sqlshell.rateConvDb(id_route=id_route[0][0], number=x, id_parsers=id_parsers)

        # количество записей после обработки
            count_row_after = sqlshell.countRoutesConvDB()

        log.write('\n' + str(datetime.datetime.now()) + ': Успешно обработано {} записей'.format(len(input_parsers)))
        log.write('\n' + str(datetime.datetime.now()) + ': Добавлено {} записей'.format(count_row_after - count_row_before))
    except Exception as err:
        log.write('\n' + str(datetime.datetime.now()) + ': Конвертер завершил работу с ошибкой - {}'.format(err))
    finally:
        # закрываем log
        log.close()
        # закрываем соединение к БД
        sqlshell.closeConnection()
