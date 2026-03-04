FROM apache/airflow:3.0.2

# Install additional Python packages required by DAGs in this repo.
# EDIT: Add any new packages here as new --find-links or on a new RUN line.
RUN pip install --no-cache-dir \
    praw \
    anthropic \
    openai
