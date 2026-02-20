# Stage 1: Get the Deno binary
FROM denoland/deno:bin AS deno_bin

# Stage 2: Get the GLIBC libraries (Debian-based)
FROM gcr.io/distroless/cc AS cc

# Stage 3: Final Image
FROM python:3.13-alpine

WORKDIR /app

# 1. Install Alpine-native tools (FFmpeg and Python use these)
RUN apk add --no-cache ffmpeg

# 2. Copy GLIBC libraries for Deno (from Stage 2)
# We put them in a dedicated folder to avoid "poisoning" the system
COPY --from=cc /lib/*-linux-gnu/* /usr/glibc/lib/
COPY --from=cc /lib/ld-linux-* /lib/

# 3. Set up the dynamic loader symlink (Deno requires this)
RUN mkdir /lib64 && ln -s /usr/glibc/lib/ld-linux-* /lib64/

# 4. Copy the Deno binary
COPY --from=deno_bin /deno /usr/local/bin/deno-raw

# 5. CREATE THE WRAPPER (This is the secret sauce)
# This script runs Deno WITH the special libraries, while keeping the rest of the system clean.
RUN echo '#!/bin/sh' > /usr/local/bin/deno && \
    echo 'LD_LIBRARY_PATH=/usr/glibc/lib exec /usr/local/bin/deno-raw "$@"' >> /usr/local/bin/deno && \
    chmod +x /usr/local/bin/deno

# 6. Install Python dependencies and apply patches
COPY . .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install -U --no-cache-dir --pre yt-dlp[default]

#RUN sed -i "s/socs.value.startswith('CAA')/str(socs).startswith('CAA')/g" /usr/local/lib/python*/site-packages/chat_downloader/sites/youtube.py

RUN mkdir -p /app/temp /app/downloads && chmod +x *.py

WORKDIR /app/downloads

# 7. Verify all three tools
RUN python --version && deno --version && ffmpeg -version

CMD ["python", "/app/runner.py", \
     "--threads", "4", \
     "--dash", "--m3u8", \
     "--write-thumbnail", \
     "--embed-thumbnail", \
     "--wait-for-video", "60:600", \
     "--clean-info-json", \
     "--remove-ip-from-json", \
     "--live-chat", \
     "--resolution", "best", \
     "--write-info-json", \
     "--log-level", "INFO"]