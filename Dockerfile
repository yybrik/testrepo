FROM python:3.12-slim AS build
WORKDIR /build
COPY pyproject.toml ./
COPY cranelock_gate ./cranelock_gate
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels cranelock-gate \
    && rm -rf /wheels \
    && useradd --uid 10001 --create-home gate \
    && mkdir /data && chown gate:gate /data
USER 10001:10001
WORKDIR /app
EXPOSE 50051 9090
ENTRYPOINT ["python", "-m", "cranelock_gate.server"]
CMD ["serve", "--config", "/config/config.json", "--database", "/data/gate.sqlite", "--bind", "0.0.0.0:50051", "--allow-insecure-development"]
