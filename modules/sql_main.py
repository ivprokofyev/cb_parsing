#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2

class SqlShell:
    """
    Middle-end для доступа к БД
    Методы:
    Для парсеров
    countInputParsers - посчет количества записей спарсенных тарифов в БД
    getAllIdInputParsers - получение всех записей по ID агента в БД
    checkInputParsers - проверка маршрутов на уникальность
    insertInputParsers - запись новых маршрутов
    updateInputParsers - обновление тарифов для существующих маршрутов
    deleteInputParsers - удаление неактуальных маршрутов

    Для обработки "сырых" данных
    getConvDb - получаем список всех записей для обработки
    checkAirportsConvDb - проверка наличия id для наименования аэропорта
    checkAirlinesConvDb - проверка наличия id для наименования авиакомпании
    checkTypeConvDb - проверка наличия id для типа груза
    countRoutesConvDB - количество маршрутов в итоговой БД
    routeConvDb - конвертация временных записей парсеров в итоговую таблицу Маршрутов
    rateRowConvDb - проверка зависимости от весовых диапозонов
    rateConvDb - конвертация временных записей парсеров в итоговую таблицу Тарифов
    addAirportConvDb - добавление наименования аэропорта в альтернативный справочник
    addAirlineConvDb - добавление наименования авиакомпании в альтернативный справочник
    addTypeConvDb - добавление наименования типа груза в альтернативный справочник

    Общие
    closeConnection - закрытие курсора и соединения к БД для экземпляра класса
    """
    DATABASE = {
        'local': "host='localhost' dbname='****' user='****' password='****'"
    }

    def __init__(self,
                 connection=DATABASE['local']
                 ):
        self.connection = psycopg2.connect(connection)
        self.cursor = self.connection.cursor()

    def countInputParsers(self):
        self.cursor.execute('''select count(*) 
                                 from input_parsers''')
        count_row = self.cursor.fetchall()[0][0]

        return count_row

    def getAllIdInputParsers(self, id_carrier):
        self.cursor.execute('''select id_record_parsers
                                 from input_parsers
                                where id_carrier = %s''', (id_carrier,))
        return self.cursor.fetchall()

    def checkInputParsers(self, id_carrier=None,
                          airport_from_ikao=None,
                          city_to=None,
                          airline_ikao=None,
                          city_transit=None,
                          airport_transit_ikao=None,
                          airport_from_name=None,
                          airport_from_iata=None,
                          airport_transit_iata=None,
                          city_from=None):
        self.cursor.execute('''select id_record_parsers
                                 from input_parsers
                                where id_carrier = %s
                                  and (airport_from_ikao = %s or airport_from_ikao is null)
                                  and (city_to = %s or city_to is null)
                                  and (airline_ikao = %s or airline_ikao is null)
                                  and (city_transit = %s or city_transit is null)
                                  and (airport_transit_ikao = %s or airport_transit_ikao is null)
                                  and (airport_from_name = %s or airport_from_name is null)
                                  and (airport_from_iata = %s or airport_from_iata is null)
                                  and (airport_transit_iata = %s or airport_transit_iata is null)
                                  and (city_from = %s or city_from is null)
                            ''', (id_carrier,
                                  airport_from_ikao,
                                  city_to,
                                  airline_ikao,
                                  city_transit,
                                  airport_transit_ikao,
                                  airport_from_name,
                                  airport_from_iata,
                                  airport_transit_iata,
                                  city_from))
        return self.cursor.fetchall()

    def insertInputParsers(self, id_carrier=None,
                           airport_from_ikao=None,
                           city_to=None,
                           airline_ikao=None,
                           city_transit=None,
                           weight_min_kg=None,
                           rate_1_rub=None,
                           airport_transit_ikao=None,
                           airport_from_name=None,
                           cost_min_rub=None,
                           airport_from_iata=None,
                           airport_transit_iata=None,
                           city_from=None):
        self.cursor.execute('''
                      insert into input_parsers(
                             id_carrier,
                             airport_from_ikao,
                             city_to,
                             airline_ikao,
                             weight_min_kg,
                             weight_1_from_kg,
                             weight_1_to_kg,
                             rate_1_rub,
                             cargo_type,
                             city_transit,
                             airport_transit_ikao,
                             airport_from_name,
                             cost_min_rub,
                             airport_from_iata,
                             airport_transit_iata,
                             city_from
                             )
                             values (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    0,
                                    9999,
                                    %s,
                                    'GENERAL',
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                    )
                      RETURNING id_record_parsers
                    ''', (id_carrier,
                          airport_from_ikao,
                          city_to,
                          airline_ikao,
                          weight_min_kg,
                          rate_1_rub,
                          city_transit,
                          airport_transit_ikao,
                          airport_from_name,
                          cost_min_rub,
                          airport_from_iata,
                          airport_transit_iata,
                          city_from))
        self.connection.commit()
        return self.cursor.fetchall()

    def updateInputParsers(self, id_carrier=None,
                           airport_from_ikao=None,
                           city_to=None,
                           airline_ikao=None,
                           city_transit=None,
                           weight_min_kg=None,
                           rate_1_rub=None,
                           airport_transit_ikao=None,
                           airport_from_name=None,
                           cost_min_rub=None,
                           airport_from_iata=None,
                           airport_transit_iata=None,
                           city_from=None):
        self.cursor.execute('''
                   update input_parsers
                      set weight_min_kg = %s,
                          rate_1_rub = %s,
                          cost_min_rub = %s
                    where id_carrier = %s
                      and (airport_from_ikao = %s or airport_from_ikao is null)
                      and (city_to = %s or city_to is null)
                      and (airline_ikao = %s or airline_ikao is null)
                      and (city_transit = %s or city_transit is null)
                      and (airport_transit_ikao = %s or airport_transit_ikao is null)
                      and (airport_from_name = %s or airport_from_name is null)
                      and (airport_from_iata = %s or airport_from_iata is null)
                      and (airport_transit_iata = %s or airport_transit_iata is null)
                      and (city_from = %s or city_from is null)
                    RETURNING id_record_parsers
                    ''', (weight_min_kg,
                          rate_1_rub,
                          cost_min_rub,
                          id_carrier,
                          airport_from_ikao,
                          city_to,
                          airline_ikao,
                          city_transit,
                          airport_transit_ikao,
                          airport_from_name,
                          airport_from_iata,
                          airport_transit_iata,
                          city_from))
        self.connection.commit()
        return self.cursor.fetchall()

    def deleteInputParsers(self, id_record_parsers=None):
        self.cursor.execute('''delete
                                 from input_parsers
                                where id_record_parsers = %s
                            ''', (id_record_parsers,))
        self.connection.commit()

    def getConvDb(self):
        self.cursor.execute('''
             select city_from,
                    airport_from_iata,
                    airport_from_ikao,
                    airport_from_name,
                    city_to,
                    airport_to_iata,
                    airport_to_ikao,
                    airport_to_name,
                    city_transit,
                    airport_transit_iata,
                    airport_transit_ikao,
                    airport_transit_name,
                    airline_name,
                    airline_iata,
                    airline_ikao,
                    id_carrier,
                    cargo_type,
                    id_record_parsers
               from input_parsers;
            ''')
        return self.cursor.fetchall()

    def checkAirportsConvDb(self, airport=None):
        self.cursor.execute('''
            select id_airport
              from airports
             where lower(%s) in (
                   lower(airport_name),
                   lower(airport_code_iata),
                   lower(airport_code_ikao),
                   lower(city_name)
                   )
    
             union
    
            select id_airport
              from airports_alt
             where lower(airport_name_alt) = lower(%s);
            ''', (airport, airport))
        return self.cursor.fetchone()

    def checkAirlinesConvDb(self, airline=None):
        self.cursor.execute('''
           select id_airlines
             from airlines
            where lower(%s) in (
                  lower(airline_name),
                  lower(airline_iata),
                  lower(airline_ikao)
                  )

            union

           select id_airlines
             from airlines_alt
            where lower(airlines_name_alt) = lower(%s);
           ''', (airline, airline))
        return self.cursor.fetchone()

    def checkTypeConvDb(self, type=None):
        self.cursor.execute('''
            select id_cargo_type
              from cargo_type
             where lower(cargo_type_name) = lower(%s)
            ''', (type, ))
        return self.cursor.fetchone()

    def countRoutesConvDB(self):
        self.cursor.execute('''select count(*) from carrier_routes''')
        return self.cursor.fetchall()[0][0]

    def routeConvDb(self,
                    id_record_parsers=None,
                    id_carrier=None,
                    id_airport_from=None,
                    id_airport_to=None,
                    id_airport_transit=None,
                    id_airline=None,
                    id_cargo_type=None
                    ):
        self.cursor.execute('''
             insert into carrier_routes 
                    (
                        id_record_parsers,
                        id_carrier,
                        id_airport_from,
                        id_airport_to,
                        id_airport_transit,
                        id_airlines,
                        id_cargo_type
                    )
             values 
                    (
                        %s,
                        %s,
                        (
                          select id_airport
                            from airports
                           where lower(%s) in 
                                 (
                                    lower(airport_name),
                                    lower(airport_code_iata),
                                    lower(airport_code_ikao),
                                    lower(city_name)
                                 )
        
                           union
        
                          select id_airport
                            from airports_alt
                           where lower(airport_name_alt) = lower(%s)
                        ),
                        (
                          select id_airport
                            from airports
                           where lower(%s) in 
                                 (
                                    lower(airport_name),
                                    lower(airport_code_iata),
                                    lower(airport_code_ikao),
                                    lower(city_name)
                                 )
        
                           union
        
                          select id_airport
                            from airports_alt
                           where lower(airport_name_alt) = lower(%s)
                        ),
                        (
                          select id_airport
                            from airports
                           where lower(%s) in 
                                 (
                                    lower(airport_name),
                                    lower(airport_code_iata),
                                    lower(airport_code_ikao),
                                    lower(city_name)
                                 )
        
                           union
        
                          select id_airport
                            from airports_alt
                           where lower(airport_name_alt) = lower(%s)
                        ),
                        (
                          select id_airlines
                            from airlines
                           where lower(%s) in 
                                 (
                                    lower(airline_name),
                                    lower(airline_iata),
                                    lower(airline_ikao)
                                 )
        
                           union
        
                          select id_airlines
                            from airlines_alt
                           where lower(airlines_name_alt) = lower(%s)
                        ),
                        (
                          select id_cargo_type
                            from cargo_type
                           where lower(cargo_type_name) = lower(%s)
                        )
                    )
        
             ON CONFLICT 
                    (
                        id_carrier, 
                        id_airport_from, 
                        id_airport_to, 
                        id_airlines,
                        id_cargo_type,
                        id_airport_transit
                    ) 
                    DO NOTHING
             RETURNING id_route, id_record_parsers
            ''', (id_record_parsers,
                  id_carrier,
                  id_airport_from, id_airport_from,
                  id_airport_to, id_airport_to,
                  id_airport_transit, id_airport_transit,
                  id_airline, id_airline,
                  id_cargo_type))
        self.connection.commit()
        return self.cursor.fetchall()

    def rateRowConvDb(self, number=1, id_parsers=None):
        self.cursor.execute('''select rate_%s_rub
                                     from input_parsers
                                    where id_record_parsers = %s
                                ''', (number, id_parsers))
        return self.cursor.fetchone()[0]

    def rateConvDb(self, id_route=None, number=1, id_parsers=None):
        self.cursor.execute(''' 
            insert into carrier_rates 
                   (
                   id_route,
                   weight_min_kg,
                   weight_from_kg,
                   weight_to_kg,
                   cost_min_rub,
                   rate_rub
                   )
            values (
                    %s,
                    (
                    select weight_min_kg
                      from input_parsers
                     where id_record_parsers = %s
                    ),
                    (
                    select weight_%s_from_kg
                      from input_parsers
                     where id_record_parsers = %s
                    ),
                    (
                    select weight_%s_to_kg
                      from input_parsers
                     where id_record_parsers = %s
                    ),
                    (
                    select cost_min_rub
                      from input_parsers
                     where id_record_parsers = %s
                    ),
                    (
                    select rate_%s_rub
                      from input_parsers
                     where id_record_parsers = %s
                    )
                    )
            ''', ((id_route,
                   id_parsers,
                   number,
                   id_parsers,
                   number,
                   id_parsers,
                   id_parsers,
                   number,
                   id_parsers,)))
        self.connection.commit()

    def addAirportConvDb(self, keyboard=None, airport=None):
        self.cursor.execute('''
            select setval('airports_alt_id_airports_alt_seq', 
                   (select max(id_airports_alt) from airports_alt));
            insert into airports_alt
                   (id_airport, 
                    airport_name_alt)
                    values (%s, %s)
                    ''', (keyboard, airport))
        self.connection.commit()

    def addAirlineConvDb(self, keyboard=None, airline=None):
        self.cursor.execute('''
            select setval('airlines_alt_id_airlines_alt_seq', 
                   (select max(id_airlines_alt) from airlines_alt));
            insert into airlines_alt
                   (id_airlines, 
                   airlines_name_alt)
                   values (%s, %s)
                   ''', (keyboard, airline))
        self.connection.commit()

    def addTypeConvDb(self, keyboard=None, type=None):
        self.cursor.execute('''
            select setval('cargo_type_alt_id_cargo_type_alt_seq', 
                   (select max(id_cargo_type_alt) from cargo_type_alt));
            insert into cargo_type_alt
                   (id_cargo_type, 
                   cargo_type_name_alt)
                   values (%s, %s)
                   ''', (keyboard, type))
        self.connection.commit()

    def closeConnection(self):
        self.cursor.close()
        self.connection.close()
