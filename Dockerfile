# One image, two services, four entrypoints (api + worker each).
#
# In production these would be separate images built from separate contexts;
# for a case study a single image keeps the build honest and the compose file
# readable, while the *processes* stay independent -- which is what actually
# matters for scaling and failure isolation.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY libs ./libs
RUN pip install --no-cache-dir -e ./libs/common

COPY services ./services
COPY tests ./tests
COPY scripts ./scripts
COPY pytest.ini ruff.toml ./

RUN useradd --create-home --uid 10001 app && chown -R app:app /srv
USER app

EXPOSE 8000
