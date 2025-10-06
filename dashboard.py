"""
Dashboard для отображения агрегированных данных о недвижимости в Португалии.
"""
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Настройка страницы Streamlit
st.set_page_config(page_title="Real Estate Listings in Portugal", layout="wide")
# Установка заголовка страницы
st.title("Португальская недвижимость (потоковая аналитика)")

# Автообновление каждые 30 секунд
st_autorefresh(interval=10 * 1000, key="data_refresh")

try:
    df = pd.read_csv("./result/aggregated_results.csv")

    # Установка подзаголовка
    st.subheader("Агрегированные данные (по типу и району)")
    # Вывод данных в виде таблицы
    st.dataframe(df)

    # Установка подзаголовка для бар чарта
    st.subheader("Средняя цена по типам недвижимости")
    # Создание и отображение бар чарта
    chart = df.groupby("Type")["avg_price"].mean().reset_index()
    st.bar_chart(chart.set_index("Type"))

    # Установка подзаголовка для линейного чарта
    st.subheader("Средняя площадь по типам недвижимости")
    # Создание и отображение линейной диаграммы
    chart_area = df.groupby("Type")["avg_area"].mean().reset_index()
    st.line_chart(chart_area.set_index("Type"))

except Exception as e:
    st.warning(f"Нет данных: {e}")
