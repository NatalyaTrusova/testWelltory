import pandas as pd
import datetime

def find_walkings(data: pd.DataFrame) -> dict:
    """
    Function input is a dataframe with a record of time intervals and the number of steps.
    The function processes the original df and looks for walk sections.
    At the output, a dictionary is generated with walks time and steps in each day.
    """
    data.drop_duplicates(inplace=True)
    data = data.sort_values(by='time_start_local')
    data = data.reset_index(drop=True)

    data[['time_start_local', 'time_end_local']] = data[['time_start_local', 'time_end_local']].apply(pd.to_datetime)

    # Приведем все даты к одинаковому формату UTC и удалим больше ненужную колонку offset
    data['time_start_local'] = data['time_start_local'] - data['time_offset'].apply(lambda x: datetime.timedelta(seconds=x))
    data['time_end_local'] =data['time_end_local'] - data['time_offset'].apply(lambda x: datetime.timedelta(seconds=x))
    data = data.drop(columns=['time_offset'], axis=1)

    # Найдем длину интервала между началом и концом промежутка
    data['segment'] = data['time_end_local'] - data['time_start_local']
    data['segment_min']=data[data.segment>datetime.timedelta(seconds=0)]['segment'].dt.seconds/60
    data.drop(columns=['segment'], axis=1, inplace=True)

    # Также рассчитаем скорость (количество шагов в минуту) для поиска аномальных данных
    data['speed_steps_in_min'] = data['steps'] / data['segment_min']

    # Посмотрим промежутки между записями
    data['record_gap'] = data['time_start_local'].shift(-1) - data['time_end_local']

    # Посмотрим насколько пересекаются времена начала и конца
    data['start_delta'] = (data['time_start_local'].shift(-1) - data['time_start_local']).dt.seconds
    data['end_delta'] = (data['time_end_local'].shift(-1) - data['time_end_local']).dt.seconds

    # Удаляем дубль строки у которых начало и конец периода отличаются на 1 с
    data = data[(data.start_delta > 1) & (data.end_delta > 1)]
    data = data.reset_index(drop=True)
    data['record_gap'] = data['time_start_local'].shift(-1) - data['time_end_local']
    data['start_delta'] = (data['time_start_local'].shift(-1) - data['time_start_local']).dt.seconds
    data['end_delta'] = (data['time_end_local'].shift(-1) - data['time_end_local']).dt.seconds

    # В цикле решим проблему с оставшимися пересекающимися промежутками и в принципе близко записанныи данным
    # Введем процент отклонения скорости, по которому мы считаем скорость все той же (например 5%)
    appropiate_difference = 15
    # Тогда отберем все индексы этих соседних записей
    nearest_actions = data[data.record_gap < datetime.timedelta(seconds=60)].index
    next_nearest_actions = nearest_actions + 1
    # Теперь учтем, что записи должны быть не только близки по времени, но и по скорости (в таком случае считаем действие тем же)
    appropiate_difference_index = data[(data.index.isin(nearest_actions)) &
                                     (abs(100 - data.speed_steps_in_min.shift(
                                         -1) * 100 / data.speed_steps_in_min) < appropiate_difference)].index
    next_appropiate_difference_index = appropiate_difference_index + 1

    new_df = data.copy()

    # В цикле проходимся по всему df до тех пор пока близких участков со схожей скоростью не останется
    while not appropiate_difference_index.empty:
        #     Отберем df для аггрегации из соседних схожих строк
        df_for_aggregation = new_df[new_df.index.isin(appropiate_difference_index | next_appropiate_difference_index)] \
            [['time_start_local', 'time_end_local', 'speed_steps_in_min', 'steps', 'segment_min']]

        #     Создадим результирующий df, не содержащий строки для агрегации
        result_df = new_df[~new_df.index.isin(appropiate_difference_index | next_appropiate_difference_index)] \
            [['time_start_local', 'time_end_local', 'speed_steps_in_min', 'steps', 'segment_min']]

        #     Рассчитаем среднее между строками для агрегации
        df_for_aggregation['mean_speed'] = (df_for_aggregation['speed_steps_in_min'] + \
                                            df_for_aggregation['speed_steps_in_min'].shift(-1)) / 2

        #    Оценим количество шагов по сумме (пусть у них и есть небольшое пересечение)
        df_for_aggregation['sum_steps'] = df_for_aggregation['steps'] + df_for_aggregation['steps'].shift(-1)

        #     Создадим новый df куда занесем все объединенные участки
        new_df = pd.DataFrame()
        new_df['time_start_local'] = df_for_aggregation[df_for_aggregation.index.isin(appropiate_difference_index)][
            'time_start_local'].values
        new_df['time_end_local'] = df_for_aggregation[df_for_aggregation.index.isin(next_appropiate_difference_index)][
            'time_end_local'].values
        new_df['speed_steps_in_min'] = df_for_aggregation[df_for_aggregation.index.isin(appropiate_difference_index)][
            'mean_speed'].values
        new_df['steps'] = df_for_aggregation[df_for_aggregation.index.isin(appropiate_difference_index)][
            'sum_steps'].values

        #     Объединим результирующий и полученный df, заново отсортируем и приведем его в порядок
        new_df = pd.concat([result_df, new_df])
        new_df.drop_duplicates(inplace=True)
        new_df = new_df.sort_values(by='time_start_local')
        new_df = new_df.reset_index(drop=True)
        #     Заново рассчитаем временные промежутки между записями
        new_df['record_gap'] = new_df['time_start_local'].shift(-1) - new_df['time_end_local']
        new_df['segment_min'] = (new_df['time_end_local'] - new_df['time_start_local']).dt.seconds / 60

        #     Перерасчет индексов
        nearest_actions = new_df[new_df.record_gap < datetime.timedelta(seconds=60)].index
        next_nearest_actions = nearest_actions + 1
        appropiate_difference_index = new_df[(new_df.index.isin(nearest_actions)) &
                                             (abs(100 - new_df.speed_steps_in_min.shift(
                                                 -1) * 100 / new_df.speed_steps_in_min) < appropiate_difference)].index
        next_appropiate_difference_index = appropiate_difference_index + 1

    # Получаем df у которого записи со схожими скоростями и временем меньше 60 с между ними объединены в одну
    # Заново рассчитаем временной отрезок между началом и концом записи
    result_df['segment_min'] = (result_df['time_end_local'] - result_df['time_start_local']).dt.seconds / 60

    # Возьмем информацию из интернета, что средняя скорость ходьбы человека - 75 шагов в минуту
    # Тогда окончательно будем считать прогулками временые промежутки со скоростью больше 75 шагов в минуту
    # И длительностью дольше 5 минут
    walks = result_df[(result_df['segment_min'] >= 5) & (result_df['speed_steps_in_min'] >= 75)]

    # Правильнее будет рассчитать количество шагов заново исходя из скорости и временного промежутка, т.к. могла накопиться ошибка
    walks['steps'] = round(walks['speed_steps_in_min'] * walks['segment_min'])

    walks['date'] = walks['time_start_local'].dt.strftime("%Y-%m-%d")
    walks['start_time'] = walks['time_start_local'].dt.strftime("%Y-%m-%d %H:%M:%S")
    walks['end_time'] = walks['time_end_local'].dt.strftime("%Y-%m-%d %H:%M:%S")

    def split_transactions(df: pd.DataFrame):
        for row, df_transactions in df.groupby(['date']):
            yield {row: list(split_line_items(df_transactions)),
                   }

    def split_line_items(df_transactions: pd.DataFrame):
        for element in df_transactions.itertuples():
            yield {
                'start': element.start_time,
                'end': element.end_time,
                'steps': element.steps
            }

    result_json = split_transactions(walks)

    return list(result_json)

data=pd.read_csv('data_for_test.csv')
walks=find_walkings(data)
print(walks)
