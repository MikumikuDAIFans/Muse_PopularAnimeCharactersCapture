FROM python:3.12-slim

WORKDIR /app

# 依赖（构建上下文为 repo 根目录，Dockerfile 在 backend/ 子目录）
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 代码（将 backend/ 子目录内容复制到 /app，保持目录结构）
COPY . .

# 数据目录
RUN mkdir -p output/metadata output/images

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]