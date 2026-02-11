# -----------------
# Builder Stage
# -----------------
FROM python:3.13-alpine AS builder

# Install only the necessary build dependencies
RUN apk add --no-cache gcc musl-dev libffi-dev python3-dev build-base curl curl-dev openssl-dev fuse3-dev pkgconf fuse3

# Install uv (fast package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Install dependencies with uv (no dev in builder)
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/root/.cache/pip \
    uv venv .venv && uv sync --no-dev --frozen

# -----------------
# Final Stage
# -----------------
FROM python:3.13-alpine
LABEL name="Riven" \
      description="Riven Media Server" \
      url="https://github.com/rivenmedia/riven"

# Install only runtime dependencies
RUN apk add --no-cache curl libcurl shadow unzip ffmpeg libpq fuse3 libcap libcap-utils postgresql17-client

# Ensure libfuse3.so.3 symlink exists (Alpine ships .so.4 but pyfuse3 wheels expect .so.3)
RUN ln -sf /usr/lib/libfuse3.so.3.* /usr/lib/libfuse3.so.3 2>/dev/null || true

# Configure FUSE
RUN sed -i 's/^#\s*user_allow_other/user_allow_other/' /etc/fuse.conf || \
    echo 'user_allow_other' >> /etc/fuse.conf

WORKDIR /riven

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /riven/.venv

# Grant the necessary capabilities to the Python binary
RUN setcap cap_sys_admin+ep /usr/local/bin/python3.13

# Activate the virtual environment by adding it to the PATH
ENV PATH="/riven/.venv/bin:$PATH"

# Copy application code and entrypoint
COPY src/ ./src
COPY pyproject.toml uv.lock* ./
COPY entrypoint.sh ./

RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
