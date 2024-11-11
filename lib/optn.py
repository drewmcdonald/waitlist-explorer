import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from tenacity import retry, wait_exponential, stop_after_attempt, RetryCallState

ages_map = {
    '< 1 Year': '< 1',
    '1-5 Years': '1-5',
    '6-10 Years': '6-10',
    '11-17 Years': '11-17',
    '18-34 Years': '18-34',
    '35-49 Years': '35-49',
    '50-64 Years': '50-64',
    '65 +': '65+',
}
ages = list(ages_map.values())


def recode_age(age: pd.Series) -> pd.Categorical:
    return pd.Categorical(age.map(ages_map), categories=ages, ordered=True)


waiting_times_map = {
    '< 30 Days': 'less than 30 days',
    '30 to < 90 Days': '30 to 90 days',
    '90 Days to < 6 Months': '90 days to 6 months',
    '6 Months to < 1 Year': '6 months to 1 year',
    '1 Year to < 2 Years': '1 to 2 years',
    '2 Years to < 3 Years': '2 to 3 years',
    '3 Years to < 5 Years': '3 to 5 years',
    '5 or More Years': '5+ years',
}
waiting_times = list(waiting_times_map.values())


def recode_waiting_time(waiting_time: pd.Series) -> pd.Categorical:
    return pd.Categorical(
        waiting_time.map(waiting_times_map), categories=waiting_times, ordered=True
    )


status_map = {
    'Liver Status 1A': 'Status 1A',
    'Liver Status 1B': 'Status 1B',
    'Liver MELD / PELD 35+': 'MELD/PELD 35+',
    'Liver MELD / PELD 30-34': 'MELD/PELD 30-34',
    'Liver MELD / PELD 25-29': 'MELD/PELD 25-29',
    'Liver MELD / PELD 20-24': 'MELD/PELD 20-24',
    'Liver MELD / PELD 15-19': 'MELD/PELD 15-19',
    'Liver MELD / PELD <15': 'MELD/PELD <15',
    'Liver Status 7 (Inactive)': 'Temporarily Inactive',
}

statuses = list(status_map.values())


def recode_status(status: pd.Series) -> pd.Categorical:
    return pd.Categorical(status.map(status_map), categories=statuses, ordered=True)


def clean_center_code(center_code: pd.Series) -> pd.Series:
    return center_code.str.split("-").str[0]


expected_transplant_filename = "Transplant___Waiting_List_Status_at_Transplant_by_Transplant_Center,_Recipient_Age.csv"
expected_waitlist_filename = (
    "Waitlist___Waiting_List_Status_by_Transplant_Center,_Age,_Waiting_Time.csv"
)


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=15),
    stop=stop_after_attempt(5),
)
def download_transplant_report(download_dir: Path):
    chrome_options = Options()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_argument("--headless=new")
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(
        "https://optn.transplant.hrsa.gov/data/view-data-reports/build-advanced/"
    )
    driver.set_window_size(1370, 1039)
    category = Select(driver.find_element(By.ID, "category"))
    category.select_by_visible_text("Transplant")

    select_col1 = Select(driver.find_element(By.ID, "col1"))
    select_col1.select_by_visible_text("Waiting List Status at Transplant (59 items)")

    select_row1 = Select(driver.find_element(By.ID, "row1"))
    select_row1.select_by_visible_text("Transplant Center (356 items)")

    select_row2 = Select(driver.find_element(By.ID, "row2"))
    select_row2.select_by_visible_text("Recipient Age (9 items)")

    select_organ = Select(driver.find_element(By.ID, "slice0"))
    select_organ.select_by_visible_text("Liver")

    select_year = Select(driver.find_element(By.ID, "slice5"))
    select_year.select_by_visible_text("2024")

    select_type = Select(driver.find_element(By.ID, "slice6"))
    select_type.select_by_visible_text("Deceased Donor")

    driver.find_element(By.ID, "DataAdvancedSubmit").click()
    time.sleep(2)

    driver.find_element(By.ID, "tool_export").click()
    time.sleep(1)

    driver.quit()

    path = download_dir / expected_transplant_filename
    if not path.exists():
        raise FileNotFoundError(f"Expected transplant report not found at {path}")

    return path


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=15),
    stop=stop_after_attempt(5),
)
def download_waitlist_report(download_dir: Path):
    chrome_options = Options()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_argument("--headless=new")
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(
        "https://optn.transplant.hrsa.gov/data/view-data-reports/build-advanced/"
    )
    driver.set_window_size(1370, 1039)

    category = Select(driver.find_element(By.ID, "category"))
    category.select_by_visible_text("Waiting List")

    select_col1 = Select(driver.find_element(By.ID, "col1"))
    select_col1.select_by_visible_text("Waiting List Status (59 items)")

    select_row1 = Select(driver.find_element(By.ID, "row1"))
    select_row1.select_by_visible_text("Transplant Center (356 items)")

    select_row2 = Select(driver.find_element(By.ID, "row2"))
    select_row2.select_by_visible_text("Age (9 items)")

    select_row3 = Select(driver.find_element(By.ID, "row3"))
    select_row3.select_by_visible_text("Waiting Time (8 items)")

    select_organ = Select(driver.find_element(By.ID, "slice0"))
    select_organ.select_by_visible_text("Liver")

    driver.find_element(By.ID, "DataAdvancedSubmit").click()
    time.sleep(2)

    driver.find_element(By.ID, "tool_export").click()
    time.sleep(1)

    driver.quit()

    path = download_dir / expected_waitlist_filename
    if not path.exists():
        raise FileNotFoundError(f"Expected waitlist report not found at {path}")

    return path


def process_waitlist_report(filename: Path, retrieved_dt: datetime) -> pd.DataFrame:
    rename_map = {
        "Unnamed: 0": "center_code",
        "Unnamed: 1": "age",
        "Unnamed: 2": "waiting_time",
    }

    df = (
        pd.read_csv(filename)
        .ffill()
        .rename(columns=rename_map)
        .drop(columns=["Unnamed: 3"])
    )

    df['center_code'] = clean_center_code(df['center_code'])
    df['age'] = recode_age(df['age'])
    df['waiting_time'] = recode_waiting_time(df['waiting_time'])

    df = df.loc[df['center_code'] != 'All Centers']

    melted = df.melt(
        id_vars=["center_code", "age", "waiting_time"],
        var_name="status",
        value_name="count",
    )

    melted['count'] = melted['count'].replace(',', '', regex=True).astype(int)
    melted['status'] = recode_status(melted['status'])
    melted['retrieved_dt'] = retrieved_dt

    melted.dropna(inplace=True, axis=0)

    return melted[melted['count'] > 0]


def process_transplant_report(filename: Path, retrieved_dt: datetime) -> pd.DataFrame:
    df = (
        pd.read_csv(filename)
        .ffill()
        .rename(columns={"Unnamed: 0": "center_code", "Unnamed: 1": "age"})
        .drop(columns=["Unnamed: 2"])
    )

    df['center_code'] = clean_center_code(df['center_code'])
    df['age'] = recode_age(df['age'])

    melted = df.melt(
        id_vars=["center_code", "age"], var_name="status", value_name="count"
    )

    melted['count'] = melted['count'].replace(',', '', regex=True).astype(int)
    melted['status'] = recode_status(melted['status'])
    melted['retrieved_dt'] = retrieved_dt

    return melted[melted['count'] > 0]
