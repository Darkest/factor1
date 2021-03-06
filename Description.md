Для реализации данного приложения были выбраны следующие библиоткеи\фреймворки:

- finagle/finch - за его удобство и простоту описания веб сервисов, а также отличную заявленную производительность
- H2 - за простоту развертывания и возможность работы в режиме in-memory, что упрощает написание тестов

Для ускорения поиска в таблицах БД были созданы следующие индексы БД: 
- USERS_LAT_INDEX и USERS_LON_INDEX на таблице USERS для ускорения поиска пользователь попадающих в зону поиска
- Поля таблицы TILES - LAT и LON так же являются проиндексированными, так как являются первыичным ключом

Для ускорения подсчета количества пользователей находящихся в определнной геолокации 
применен алгоритм первоначального отсева пользователей, не попадающих в "квадрат" 
с границами в максимально возможных отдалаениях от заданной геолокации. 
Далее происходит точная проверка расстояния среди отобранных пользователей. 
 
### Сложность выполнения запросов
- Проверка нахождения пользователя в указанной точке(общая сложность - O(log N)) 
   - Поиск пользователя в БД(при настроенном индексе на поле id) - O(log N)
   - Поиск допустимого расстояния в БД(при настроенном индексе на полях LAT и LON - O(log N))
   - Вычисление растояния - O(1)
- Обновление\добавление местоположения пользователя - O(log N)
- Подсчет количества пользователей в заданной ячейке - O(N logN)
  - Поиск допустимого расстояния в БД(при настроенном индексе на полях LAT и LON таблицы геолокаций)- O(log N))
  - Вычисление допустимых диапазонов долготы и широты - O(1)
  - Получение списка пользователей находящихся в указанном диапазоне из БД(при настроенном индексе на полях LAT и LON таблицы пользователей) - O(log N)
  - Точная проверка расстояния - O(n), где n - количество предварительно отобранных пользователей

### Возможные улучшения:
- Добавить дополнительное логирование для более простого поиска проблем в продакшене
- Добавить обработку ошибок таким образом, чтобы клиенту отдавались понятные сообщения об ошибках
- Провести нагрузочное тестирование, и поправить возможные узкие места
- Перейти на использование более точного алгоритма рассчета расстояния, учитывающего эллиптичность Земли
- Добавить возможность ведения статистики по запросам(возможно подключить twitter statistic server)
- Добавить юнит тесты для всех классов
- Создать файл настройки логирования logback
- При появлении новых требований к сервису, возможно, стоит перейти на использование ORM
- Местами возможно вынести дублирующийся код

