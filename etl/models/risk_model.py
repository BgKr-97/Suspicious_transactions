import pandas as pd
import json


class RiskScoringModel:
    """
    Класс для скоринговой модели оценки риска транзакций.
    Загружает весовые коэффициенты из JSON-файла и рассчитывает для каждого клиента
    и каждой транзакции:

    - risk_score: суммарный скоринговый балл;
    - risk_status: статус транзакции;
    - reason_flags: список причин.
    """
    def __init__(self, json_path: str):
        """
        Инициализация модели.

        Параметры:
        - json_path: путь к JSON-файлу с весами признаков
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.map = self.extract_feature_scores()

    def extract_feature_scores(self) -> dict:
        """
        Возвращает словарь признаков в формате:
        { 'column': {'score': value, 'reason': feature_name}, ... }
        """
        result = {}
        data = self.config

        for group in data.values():
            for name, info in group.items():
                column = info.get("column")
                score = info.get("score")

                result[column] = {"score": score, "reason_flags": name}
        return result

    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет в DataFrame столбцы:
          - risk_score: суммарный балл по булевым столбцам;
          - risk_status: один из ['Обычная', 'Требует проверки', 'Подозрительная'], выбираемый по критериям.
        """
        df = df.copy()
        df['risk_score'] = 0

        # Формирование массива причин каждой транзакции
        def reasons(row: pd.Series):
            k = []
            score_total = 0

            for col_name, d_info in self.map.items():
                reason = d_info["reason_flags"]
                score = d_info['score']

                if not col_name == 'client_age':
                    if row[col_name]:
                        score_total += score
                        k.append(reason)
                else:
                    if row['client_age'] >= 60:
                        k.append(reason)
            k = ', '.join(k)

            if not k:
                return k, 0
            return k, score_total

        df[['reason_flags', 'risk_score']] = df.apply(reasons, axis=1, result_type='expand')

        # Считаем усиливающие факторы при их наличии
        def high_age(row: pd.Series):
            current_score = row['risk_score']
            if row['client_age'] >= 60:
                score = self.map['client_age']['score']
                cols_to_check = list(self.map.keys())
                cols_to_check.remove('client_age')

                if row[cols_to_check].any():
                    current_score *= score[0]
                else:
                    current_score += score[1]
            return int(current_score)
        df['risk_score'] = df.apply(high_age, axis=1)

        # Определяем статус транзакции
        def assign_status(row: pd.Series):
            if row['risk_score'] >= 80:
                return 'Подозрительная'
            elif 50 < row['risk_score'] < 80:
                return 'Требует проверки'
            else:
                return 'Обычная'

        df['risk_status'] = df.apply(assign_status, axis=1)
        df['is_suspicious'] = df.apply(lambda x: x['risk_status'] != 'Обычная', axis=1)

        return df