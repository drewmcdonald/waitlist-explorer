from functools import reduce
from typing import Dict, Literal, Sequence, TypeVar

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud.storage import Client as GcsClient
from google.oauth2.service_account import Credentials
from numerize import numerize

from lib.Center import Center
from lib.optn import ages, statuses, waiting_times
from lib.Report import ReportCollection
from lib.util import config

client = GcsClient(
    credentials=Credentials.from_service_account_info(st.secrets['google'])
)
collection = ReportCollection(client, client.bucket(config.gcs_bucket))

st.set_page_config(layout="wide", page_title='Wait')


@st.cache_data
def read_centers():
    return [
        Center.from_json(l) for l in open('data/centers_geocoded.jsonl').readlines()
    ]


@st.cache_data
def read_distances():
    return pd.read_csv("data/centers_distance.txt", delimiter="\t")


T = TypeVar('T')


def first_and_last(lst: Sequence[T]) -> tuple[T, T]:
    return lst[0], lst[-1]


@st.cache_data
def load_data():
    centers = read_centers()
    name_by_code = {c.code: str(c) for c in centers}
    waitlist_report = collection.get_processed_waitlist()
    waitlist_report['center'] = waitlist_report['center_code'].map(name_by_code)

    waitlist_report.columns = [
        c.replace('_', ' ').title() for c in waitlist_report.columns
    ]

    return [
        c for c in centers if c.code in waitlist_report['Center Code'].unique()
    ], waitlist_report


centers, waitlist_report = load_data()
distances = read_distances()


x_order: Dict[str, Sequence[str]] = {
    'Status': list(reversed(statuses)),
    'Age': ages,
    'Waiting Time': waiting_times,
    'Center': [str(c) for c in centers],
}

color_order: Dict[str, Sequence[str]] = {
    'Status': list(reversed(statuses)),
    'Age': list(reversed(ages)),
    'Waiting Time': waiting_times,
    'Center': [str(c) for c in centers],
}

col_type: Dict[str, Literal['ordinal', 'nominal']] = {
    'Status': 'ordinal',
    'Age': 'ordinal',
    'Waiting Time': 'ordinal',
    'Center': 'nominal',
}


def summary_chart(frame: pd.DataFrame, group_by: str, color_by: str):

    return (
        alt.Chart(
            frame.groupby([group_by, color_by], observed=True)['Count']
            .sum()
            .reset_index()
        )
        .mark_bar()
        .encode(
            alt.X(group_by, type=col_type[group_by], sort=x_order[group_by]),
            alt.Y('Count', title='Number of waitlist patients'),
            alt.Color(
                color_by,
                type=col_type[color_by],
            ),
            alt.Order(field=group_by),
        )
        .properties(height=600)
        .interactive()
    )


def centers_in_radius(center_code: str, radius_nm: float) -> list[str]:
    others = (
        distances.loc[
            (distances['source'] == center_code)
            & (distances['distance_nm'] <= radius_nm),
            'target',
        ]
        .unique()
        .tolist()
    )
    return [center_code] + others


def filter_data(
    report: pd.DataFrame,
) -> pd.DataFrame:
    st.sidebar.header("Filter waitlist")
    center_input = st.sidebar.selectbox(
        "Center",
        centers,
        index=None,
        placeholder="Select a center",
        format_func=lambda c: str(c),
    )

    max_distance = None
    if center_input:
        include_radius_toggle = st.sidebar.toggle(
            "Include other centers in radius",
            value=False,
        )
        if include_radius_toggle:
            max_distance = st.sidebar.slider(
                "Radius (nautical miles)",
                min_value=0,
                max_value=1000,
                value=150,
                step=50,
            )

    status_input = st.sidebar.select_slider(
        "Status", statuses, value=('Status 1A', 'MELD/PELD <15')
    )
    age_input = st.sidebar.select_slider("Age", ages, value=first_and_last(ages))
    waiting_time_input = st.sidebar.select_slider(
        "Waiting Time", waiting_times, value=first_and_last(waiting_times)
    )

    conditions = [
        report['Age'].between(*age_input),
        report['Waiting Time'].between(*waiting_time_input),
        report['Status'].between(*status_input),
    ]

    if center_input:
        code = center_input.code
        if max_distance is not None:
            conditions.append(
                report['Center Code'].isin(centers_in_radius(code, max_distance))
            )
        else:
            conditions.append(report['Center Code'] == code)

    return report.loc[
        reduce(lambda x, y: x & y, conditions),
        ['Center', 'Age', 'Waiting Time', 'Status', 'Count'],
    ]


df = filter_data(waitlist_report)
cols = ['Age', 'Waiting Time', 'Status', 'Center']

st.markdown(
    """
    # Waitlist explorer
    """
)

col1, col2 = st.columns(2)


with col1:
    st.metric(
        label="Waitlist patients",
        value=numerize.numerize(df['Count'].astype(float).sum()),
        help='Number of patients on the waitlist',
    )

    group_by = col1.selectbox("Group by", cols, index=0)

with col2:
    st.metric(
        label="Transplant centers",
        value=numerize.numerize(df['Center'].nunique()),
        help='Number of transplant centers',
    )
    color_by = col2.selectbox("Color by", [c for c in cols if c != group_by], index=1)


st.altair_chart(
    summary_chart(df, group_by, color_by),
    use_container_width=True,
)

# """
# * same identifiers
# * worse status
# * same status and longer waiting time
# * same status and same waiting time but younger
# """
