# -*- coding: utf-8 -*-
"""
Streamlit-приложение для сбора отзывов из Google Play Store
Запуск: streamlit run app.py
"""

import streamlit as st
import time
import pandas as pd
from datetime import datetime, date
from google_play_scraper import reviews, Sort, exceptions
from langdetect import detect, LangDetectException
import random
import sys
import os
from io import BytesIO

# ============================================================================
# 1. КОНФИГУРАЦИЯ СТРАНИЦЫ
# ============================================================================

st.set_page_config(
    page_title="Google Play Reviews Scraper",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# 2. ФУНКЦИИ СБОРА И ФИЛЬТРАЦИИ
# ============================================================================

@st.cache_data(ttl=3600)
def extract_package_name(url: str) -> str:
    """Извлекает package name из URL"""
    url = url.strip()
    if 'id=' in url:
        return url.split('id=')[-1].split('&')[0].strip()
    raise ValueError(f"Не удалось извлечь package name из ссылки")

def collect_reviews_streamlit(package_name: str, target_count: int, delay_base: float, progress_bar, status_text):
    """Сбор отзывов с интеграцией в интерфейс Streamlit"""
    all_reviews = []
    continuation_token = None
    page = 0
    errors_in_row = 0
    max_errors = 4
    
    while len(all_reviews) < target_count and errors_in_row < max_errors:
        try:
            # Задержка между запросами
            if page > 0:
                delay = delay_base + random.uniform(0.5, 1.2)
                time.sleep(delay)
            
            # Запрос отзывов
            result, continuation_token = reviews(
                package_name,
                lang='ru',
                country='ru',
                sort=Sort.NEWEST,
                count=min(200, target_count - len(all_reviews)),
                continuation_token=continuation_token
            )
            
            if not result:
                if page == 0:
                    raise ValueError(f"Приложение '{package_name}' не найдено")
                break
            
            # Убираем дубликаты
            existing_ids = {r['reviewId'] for r in all_reviews}
            new_reviews = [r for r in result if r['reviewId'] not in existing_ids]
            
            all_reviews.extend(new_reviews)
            page += 1
            errors_in_row = 0
            
            # Обновляем прогресс
            progress = min(len(all_reviews) / target_count, 1.0)
            progress_bar.progress(progress)
            status_text.text(f"Страница {page} | Собрано: {len(all_reviews)}/{target_count}")
            
            if continuation_token is None or len(all_reviews) >= target_count:
                break
                
        except (exceptions.NotFoundError, exceptions.ExtraHTTPError) as e:
            error_msg = str(e).lower()
            if '404' in error_msg or 'not found' in error_msg:
                raise ValueError(f"Приложение '{package_name}' не существует в Google Play Store")
            elif '429' in error_msg or '403' in error_msg:
                errors_in_row += 1
                wait = min(30, 5 * (2 ** errors_in_row))
                status_text.warning(f"⚠️ Ограничение скорости. Ожидание {wait} сек...")
                time.sleep(wait)
                continue
            else:
                errors_in_row += 1
                status_text.warning(f"⚠️ Ошибка API. Попытка {errors_in_row}/{max_errors}")
                time.sleep(4)
                continue
        except Exception as e:
            errors_in_row += 1
            status_text.warning(f"⚠️ Ошибка: {type(e).__name__}")
            time.sleep(3)
            continue
    
    return all_reviews[:target_count]

def filter_reviews_streamlit(reviews_list: list, start_date: date, end_date: date, selected_langs: list):
    """Фильтрация отзывов с интеграцией в интерфейс Streamlit"""
    filtered = []
    stats = {
        'total': len(reviews_list),
        'by_date': 0,
        'by_language': 0,
        'empty_content': 0,
        'langdetect_error': 0
    }
    
    for r in reviews_list:
        # Обработка даты
        review_date = r['at'].date() if hasattr(r['at'], 'date') else r['at']
        if isinstance(review_date, datetime):
            review_date = review_date.date()
        elif isinstance(review_date, str):
            try:
                review_date = datetime.strptime(review_date[:10], "%Y-%m-%d").date()
            except:
                stats['by_date'] += 1
                continue
        
        # Фильтр по дате
        if not (start_date <= review_date <= end_date):
            stats['by_date'] += 1
            continue
        
        content = (r.get('content') or '').strip()
        if not content or len(content) < 5:
            stats['empty_content'] += 1
            continue
        
        # Проверка языка
        try:
            lang = detect(content[:300])
            if lang not in selected_langs:
                stats['by_language'] += 1
                continue
        except LangDetectException:
            stats['langdetect_error'] += 1
            continue
        
        date_str = review_date.strftime('%Y-%m-%d')
        
        filtered.append({
            'rating': int(r['score']),
            'title': '',
            'content': content,
            'date': date_str,
            'language': lang
        })
    
    return filtered, stats

def to_excel(df: pd.DataFrame) -> bytes:
    """Конвертирует DataFrame в Excel файл с форматированием"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Отзывы')
        
        # Автоширина колонок
        worksheet = writer.sheets['Отзывы']
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 50)
    
    return output.getvalue()

# ============================================================================
# 3. ИНТЕРФЕЙС ПРИЛОЖЕНИЯ
# ============================================================================

def main():
    # Заголовок
    st.title("📱 Google Play Reviews Scraper")
    st.markdown("### Сбор и анализ отзывов из магазина приложений Google Play")
    
    # Sidebar - настройки
    with st.sidebar:
        st.header("⚙️ Настройки сбора")
        
        # Ввод ссылки
        st.subheader("Ссылка на приложение")
        default_url = "https://play.google.com/store/apps/details?id=com.logistic.sdek"
        app_url = st.text_input(
            "URL приложения в Google Play",
            value=default_url,
            help="Пример: https://play.google.com/store/apps/details?id=com.example.app"
        )
        
        # Количество отзывов
        st.subheader("Параметры сбора")
        target_count = st.slider(
            "Количество отзывов",
            min_value=50,
            max_value=2000,
            value=500,
            step=50,
            help="Максимум обычно 1000-2000 из-за ограничений Google Play API"
        )
        
        # Задержка
        delay_base = st.slider(
            "Задержка между запросами (сек)",
            min_value=1.5,
            max_value=5.0,
            value=2.5,
            step=0.5,
            help="Больше задержка = меньше шанс блокировки, но дольше сбор"
        )
        
        # Фильтры
        st.subheader("Фильтры")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Начальная дата",
                value=date(2023, 1, 1),
                min_value=date(2020, 1, 1),
                max_value=date.today()
            )
        with col2:
            end_date = st.date_input(
                "Конечная дата",
                value=date.today(),
                min_value=date(2020, 1, 1),
                max_value=date.today()
            )
        
        languages = st.multiselect(
            "Языки отзывов",
            options=['ru', 'en', 'uk', 'be', 'kk', 'hy', 'az', 'ka'],
            default=['ru'],
            help="Выберите языки для анализа"
        )
        
        # Кнопка запуска
        st.divider()
        start_button = st.button("🚀 Запустить сбор", type="primary", use_container_width=True)
        
        # Информация
        st.divider()
        st.info("""
        **💡 Советы:**
        - Для массовых приложений (WhatsApp, Telegram) собирайте 500-1000 отзывов
        - Для нишевых приложений достаточно 100-200
        - При ошибке 429 подождите 5 минут и повторите
        - Максимум отзывов: 1000-2000 (ограничение Google)
        """)
    
    # Основная область
    if not start_button:
        # Приветственное сообщение
        st.info("👈 Настройте параметры в боковой панели и нажмите **'Запустить сбор'**")
        
        # Примеры популярных приложений
        st.subheader("🎯 Примеры приложений для анализа:")
        
        example_apps = {
            "СДЕК": "https://play.google.com/store/apps/details?id=com.logistic.sdek",
            "Почта России": "https://play.google.com/store/apps/details?id=ru.russianpost.postoffice",
            "Яндекс.Карты": "https://play.google.com/store/apps/details?id=ru.yandex.yandexmaps",
            "СберБанк": "https://play.google.com/store/apps/details?id=ru.sberbankmobile",
            "Тинькофф": "https://play.google.com/store/apps/details?id=ru.tinkoff.mobile",
            "ВКонтакте": "https://play.google.com/store/apps/details?id=com.vkontakte.android",
            "Telegram": "https://play.google.com/store/apps/details?id=org.telegram.messenger",
            "WhatsApp": "https://play.google.com/store/apps/details?id=com.whatsapp",
            "Duolingo": "https://play.google.com/store/apps/details?id=com.duolingo"
        }
        
        cols = st.columns(3)
        for i, (name, url) in enumerate(example_apps.items()):
            with cols[i % 3]:
                if st.button(f"📱 {name}", key=f"example_{i}", use_container_width=True):
                    st.session_state.app_url = url
                    st.rerun()
        
        return
    
    # ============================================================================
    # 4. ВЫПОЛНЕНИЕ СБОРА
    # ============================================================================
    
    try:
        # Валидация входных данных
        if not app_url or 'play.google.com' not in app_url:
            st.error("❌ Некорректная ссылка на приложение Google Play")
            st.stop()
        
        if start_date > end_date:
            st.error("❌ Начальная дата не может быть позже конечной")
            st.stop()
        
        if not languages:
            st.error("❌ Выберите хотя бы один язык для фильтрации")
            st.stop()
        
        # Извлечение package name
        with st.spinner("🔍 Извлечение package name..."):
            package_name = extract_package_name(app_url)
        
        st.success(f"✅ Package name: `{package_name}`")
        
        # Разделитель
        st.divider()
        
        # Индикаторы прогресса
        st.subheader("📊 Прогресс сбора")
        progress_bar = st.progress(0)
        status_text = st.empty()
        status_text.text("Подготовка к сбору...")
        
        # Сбор отзывов
        status_text.text("Начало сбора отзывов...")
        collected = collect_reviews_streamlit(
            package_name=package_name,
            target_count=target_count,
            delay_base=delay_base,
            progress_bar=progress_bar,
            status_text=status_text
        )
        
        if not collected:
            st.error("❌ Не удалось собрать отзывы. Проверьте ссылку или попробуйте позже.")
            st.stop()
        
        # Фильтрация
        status_text.text("Фильтрация отзывов...")
        filtered, stats = filter_reviews_streamlit(
            reviews_list=collected,
            start_date=start_date,
            end_date=end_date,
            selected_langs=languages
        )
        
        # Завершение прогресса
        progress_bar.progress(1.0)
        status_text.text("✅ Сбор и фильтрация завершены!")
        time.sleep(0.5)
        
        # ============================================================================
        # 5. ОТОБРАЖЕНИЕ РЕЗУЛЬТАТОВ
        # ============================================================================
        
        st.divider()
        st.subheader("📈 Результаты сбора")
        
        # Статистика в колонках
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Всего собрано", f"{stats['total']}")
        with col2:
            st.metric("После фильтрации", f"{len(filtered)}", 
                     delta=f"-{stats['total'] - len(filtered)}")
        with col3:
            st.metric("Период", f"{start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")
        with col4:
            st.metric("Языки", ", ".join(languages))
        
        # Детальная статистика
        with st.expander("📊 Подробная статистика фильтрации"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Отфильтровано по дате", stats['by_date'])
            with col2:
                st.metric("Отфильтровано по языку", stats['by_language'])
            with col3:
                st.metric("Пустой контент", stats['empty_content'])
            with col4:
                st.metric("Ошибки определения языка", stats['langdetect_error'])
        
        if not filtered:
            st.warning("⚠️ После фильтрации не осталось отзывов. Попробуйте:")
            st.markdown("""
            - Расширить диапазон дат
            - Добавить больше языков
            - Уменьшить количество запрашиваемых отзывов
            """)
            st.stop()
        
        # ============================================================================
        # 6. АНАЛИЗ ДАННЫХ
        # ============================================================================
        
        st.divider()
        st.subheader("⭐ Анализ отзывов")
        
        # Создание DataFrame
        df = pd.DataFrame(filtered)
        
        # Распределение оценок
        col1, col2 = st.columns([2, 1])
        
        with col1:
            rating_counts = df['rating'].value_counts().sort_index()
            rating_df = pd.DataFrame({
                'Оценка': rating_counts.index,
                'Количество': rating_counts.values
            })
            
            # Добавляем проценты
            rating_df['%'] = (rating_df['Количество'] / len(df) * 100).round(1)
            
            st.dataframe(
                rating_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Оценка": st.column_config.NumberColumn("⭐ Оценка", format="%d"),
                    "Количество": st.column_config.NumberColumn("📊 Количество", format="%d"),
                    "%": st.column_config.NumberColumn("📈 %", format="%.1f%%")
                }
            )
        
        with col2:
            avg_rating = df['rating'].mean()
            st.metric("Средняя оценка", f"{avg_rating:.2f}")
            
            pos_reviews = len(df[df['rating'] >= 4])
            neg_reviews = len(df[df['rating'] <= 2])
            
            st.metric("Положительные (4-5⭐)", f"{pos_reviews} ({pos_reviews/len(df)*100:.0f}%)")
            st.metric("Отрицательные (1-2⭐)", f"{neg_reviews} ({neg_reviews/len(df)*100:.0f}%)")
        
        # ============================================================================
        # 7. ПРОСМОТР ОТЗЫВОВ
        # ============================================================================
        
        st.divider()
        st.subheader("📝 Примеры отзывов")
        
        # Фильтр по оценке
        selected_rating = st.select_slider(
            "Фильтр по оценке",
            options=[1, 2, 3, 4, 5],
            value=(1, 5),
            format_func=lambda x: f"⭐ {x}"
        )
        
        # Фильтрация по оценке
        filtered_preview = df[
            (df['rating'] >= selected_rating[0]) & 
            (df['rating'] <= selected_rating[1])
        ].sort_values('date', ascending=False)
        
        # Пагинация
        page_size = 10
        total_pages = (len(filtered_preview) - 1) // page_size + 1
        page = st.number_input(
            "Страница",
            min_value=1,
            max_value=max(1, total_pages),
            value=1,
            step=1
        )
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_reviews = filtered_preview.iloc[start_idx:end_idx]
        
        st.caption(f"Показано {len(page_reviews)} из {len(filtered_preview)} отзывов")
        
        # Отображение отзывов
        for idx, row in page_reviews.iterrows():
            with st.container():
                col1, col2 = st.columns([1, 5])
                
                with col1:
                    rating_emoji = "⭐" * row['rating']
                    st.markdown(f"**{rating_emoji}**")
                    st.caption(f"{row['date']}")
                    if 'language' in row:
                        st.caption(f"🌐 {row['language']}")
                
                with col2:
                    content_preview = row['content'][:500] + '...' if len(row['content']) > 500 else row['content']
                    st.markdown(f"{content_preview}")
                
                st.divider()
        
        # ============================================================================
        # 8. СКАЧИВАНИЕ РЕЗУЛЬТАТОВ
        # ============================================================================
        
        st.divider()
        st.subheader("💾 Скачать результаты")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV
            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Скачать CSV (Excel)",
                data=csv,
                file_name=f"reviews_{package_name}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                type="primary",
                use_container_width=True
            )
            st.caption("Формат UTF-8 с BOM для корректного отображения кириллицы в Excel")
        
        with col2:
            # Excel
            excel_file = to_excel(df)
            st.download_button(
                label="📊 Скачать Excel (форматированный)",
                data=excel_file,
                file_name=f"reviews_{package_name}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary",
                use_container_width=True
            )
            st.caption("С автошириной колонок и форматированием")
        
        # Информация о файле
        st.info(f"""
        **📄 Информация о файле:**
        - Приложение: `{package_name}`
        - Количество отзывов: {len(df)}
        - Период: {df['date'].min()} — {df['date'].max()}
        - Языки: {', '.join(df['language'].unique()) if 'language' in df.columns else ', '.join(languages)}
        - Столбцы: rating, title, content, date{', language' if 'language' in df.columns else ''}
        """)
        
        # ============================================================================
        # 9. ПОДСКАЗКИ ПО АНАЛИЗУ
        # ============================================================================
        
        with st.expander("💡 Советы по анализу отзывов"):
            st.markdown("""
            ### Анализ отрицательных отзывов (1-2⭐)
            - **Ищите повторяющиеся проблемы**: курьеры, задержки, баги в приложении
            - **Группируйте по темам**: доставка, интерфейс, оплата, поддержка
            - **Обратите внимание на даты**: массовые жалобы в определенный период
            
            ### Анализ положительных отзывов (4-5⭐)
            - **Выявите сильные стороны**: что пользователи ценят больше всего
            - **Используйте цитаты** для маркетинговых материалов
            - **Анализируйте контекст**: почему поставили высокую оценку
            
            ### Рекомендации в Excel/Google Sheets:
            1. Отфильтруйте по колонке `rating` (1-2 для проблем, 4-5 для успехов)
            2. Используйте поиск по ключевым словам: "курьер", "задержка", "баг", "отлично"
            3. Создайте сводную таблицу по датам для анализа динамики
            4. Добавьте колонку "Категория" для ручной классификации отзывов
            """)
        
    except Exception as e:
        st.error(f"❌ Ошибка: {type(e).__name__}")
        st.code(str(e))
        
        st.info("""
        **💡 Возможные решения:**
        - Проверьте корректность ссылки на приложение
        - Убедитесь, что приложение существует в Google Play Store
        - Попробуйте уменьшить количество отзывов (200-300)
        - Подождите 5 минут при ошибке 429 (ограничение скорости)
        - Проверьте интернет-соединение
        """)

if __name__ == "__main__":
    main()