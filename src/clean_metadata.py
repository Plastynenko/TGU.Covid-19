import os
import pandas as pd
import numpy as np


PROJECT_ROOT = r"C:\COVIDPRO"
RAW_METADATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "metadata.csv")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
CLEANED_CSV_PATH = os.path.join(PROCESSED_DIR, "metadata_cleaned.csv")
CLEANED_PARQUET_PATH = os.path.join(PROCESSED_DIR, "metadata_cleaned.parquet")


def normalize_sex(sex):
    if pd.isna(sex):
        return "Unknown"
    sex = str(sex).strip().upper()
    if sex in ["M", "MALE"]:
        return "M"
    if sex in ["F", "FEMALE"]:
        return "F"
    return "Unknown"


def normalize_finding(f):
    if pd.isna(f):
        return "Unknown"
    f_str = str(f).strip()

    upper = f_str.upper()

    # COVID-19
    if "COVID" in upper:
        return "COVID-19"

    # SARS (но не COVID)
    if "SARS" in upper:
        return "SARS"

    # Пневмония
    if upper.startswith("PNEUMONIA"):
        return "Pneumonia"

    # No Finding
    if upper == "NO FINDING":
        return "No finding"

    # Unknown / todo
    if upper in ["UNKNOWN", "TODO"]:
        return "Unknown"

    # Tuberculosis
    if upper == "TUBERCULOSIS":
        return "Tuberculosis"

    return "Other"


def make_age_group(age):
    # простая группировка: дети / взрослые / пожилые
    if pd.isna(age):
        return "unknown"
    try:
        a = float(age)
    except ValueError:
        return "unknown"

    if a < 18:
        return "child"
    elif a < 60:
        return "adult"
    else:
        return "senior"


def make_age_category(age):
    # группировка под задание: young / middle / old
    if pd.isna(age):
        return "unknown"
    try:
        a = float(age)
    except ValueError:
        return "unknown"

    if a < 30:
        return "young"
    elif a < 60:
        return "middle"
    else:
        return "old"


def main():
    print(f"Читаю исходный файл: {RAW_METADATA_PATH}")
    df = pd.read_csv(RAW_METADATA_PATH)

    print(f"Исходное количество строк: {len(df)}")

    # Удаляем колонку Unnamed: 29, если есть
    if "Unnamed: 29" in df.columns:
        df = df.drop(columns=["Unnamed: 29"])
        print("Удалена колонка 'Unnamed: 29'")

    # Приводим возраст к числу
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # Убираем явные аномалии по возрасту
    before_age_clean = df["age"].isna().sum()
    df.loc[(df["age"] < 0) | (df["age"] > 120), "age"] = np.nan
    after_age_clean = df["age"].isna().sum()
    print(f"Пропуски в age до очистки аномалий: {before_age_clean}, после: {after_age_clean}")

    # Заполняем пропуски в поле sex
    df["sex"] = df["sex"].apply(normalize_sex)

    # Вычисляем медиану возраста (по не-пустым)
    age_median = df["age"].median()
    print(f"Медианный возраст: {age_median}")

    # Заполняем пропуски возраста медианой
    df["age"] = df["age"].fillna(age_median)

    # Нормализуем диагноз
    df["finding_clean"] = df["finding"].apply(normalize_finding)

    # Конвертируем дату в datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Создаём возрастные группы
    df["age_group"] = df["age"].apply(make_age_group)
    df["age_category"] = df["age"].apply(make_age_category)

    # Удаляем дубликаты (например, по patientid + filename)
    before_dups = len(df)
    df = df.drop_duplicates(subset=["patientid", "filename"])
    after_dups = len(df)
    print(f"Удалено дубликатов: {before_dups - after_dups}")

    # Убеждаемся, что директория для сохранения существует
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Сохраняем результат
    print(f"Сохраняю очищенные данные в {CLEANED_CSV_PATH} и {CLEANED_PARQUET_PATH}")
    df.to_csv(CLEANED_CSV_PATH, index=False)
    df.to_parquet(CLEANED_PARQUET_PATH, index=False)

    print("Готово.")


if __name__ == "__main__":
    main()