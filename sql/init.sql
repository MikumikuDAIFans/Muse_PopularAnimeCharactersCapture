-- Muse DataLoad 数据库初始化脚本
-- SQLite3: sqlite3 muse_dataload.db < sql/init.sql

-- ========== 项目表 ==========
CREATE TABLE IF NOT EXISTS project (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========== 任务表 ==========
CREATE TABLE IF NOT EXISTS task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER REFERENCES project(id),
    name TEXT NOT NULL,
    task_type TEXT NOT NULL CHECK (task_type IN ('posts', 'tags', 'characters')),
    params TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','paused','completed','failed','cancelled')),
    progress REAL NOT NULL DEFAULT 0.0,
    processed_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_task_status ON task(status);
CREATE INDEX IF NOT EXISTS ix_task_project_id ON task(project_id);

-- ========== 帖子元数据表 ==========
CREATE TABLE IF NOT EXISTS post (
    id INTEGER PRIMARY KEY,
    task_id INTEGER REFERENCES task(id),
    md5 TEXT,
    file_url TEXT,
    preview_url TEXT,
    sample_url TEXT,
    source TEXT,
    uploader_id INTEGER,
    uploader_name TEXT,
    tag_string TEXT,
    tag_count INTEGER DEFAULT 0,
    file_ext TEXT,
    file_size INTEGER,
    image_width INTEGER,
    image_height INTEGER,
    score INTEGER DEFAULT 0,
    fav_count INTEGER DEFAULT 0,
    rating TEXT,
    sources TEXT,  -- JSON数组
    has_children BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    is_flagged BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    file_path TEXT,
    file_verified BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_post_task_id ON post(task_id);
CREATE INDEX IF NOT EXISTS ix_post_fetched_at ON post(fetched_at);
CREATE INDEX IF NOT EXISTS ix_post_md5 ON post(md5);

-- ========== 标签表 ==========
CREATE TABLE IF NOT EXISTS tag (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL CHECK (category IN ('character','copyright','artist','general','meta','style')),
    post_count INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_tag_category ON tag(category);
CREATE INDEX IF NOT EXISTS ix_tag_name ON tag(name);

CREATE TABLE IF NOT EXISTS tag_alias (
    id INTEGER PRIMARY KEY,
    antecedent_name TEXT NOT NULL,
    consequent_name TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_tag_alias_antecedent ON tag_alias(antecedent_name);
CREATE INDEX IF NOT EXISTS ix_tag_alias_consequent ON tag_alias(consequent_name);

CREATE TABLE IF NOT EXISTS tag_implication (
    id INTEGER PRIMARY KEY,
    antecedent_name TEXT NOT NULL,
    consequent_name TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_tag_implication_antecedent ON tag_implication(antecedent_name);
CREATE INDEX IF NOT EXISTS ix_tag_implication_consequent ON tag_implication(consequent_name);

CREATE TABLE IF NOT EXISTS sync_checkpoint (
    name TEXT PRIMARY KEY,
    checkpoint TEXT DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========== 帖子-标签关联表 ==========
CREATE TABLE IF NOT EXISTS post_tag (
    post_id INTEGER REFERENCES post(id),
    tag_id INTEGER REFERENCES tag(id),
    PRIMARY KEY (post_id, tag_id)
);

-- ========== 角色表 ==========
CREATE TABLE IF NOT EXISTS character (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER UNIQUE REFERENCES tag(id),
    total_post_count INTEGER DEFAULT 0,
    recent_post_count INTEGER DEFAULT 0,
    popularity_score REAL DEFAULT 0.0,
    stat_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_character_score ON character(popularity_score DESC);
CREATE INDEX IF NOT EXISTS ix_character_total ON character(total_post_count DESC);

-- ========== 作品表 ==========
CREATE TABLE IF NOT EXISTS copyright (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER UNIQUE REFERENCES tag(id),
    post_count INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ========== 角色-作品关联表 ==========
CREATE TABLE IF NOT EXISTS character_copyright (
    character_tag_id INTEGER REFERENCES character(tag_id),
    copyright_tag_id INTEGER REFERENCES copyright(tag_id),
    PRIMARY KEY (character_tag_id, copyright_tag_id)
);

-- ========== 任务日志表 ==========
CREATE TABLE IF NOT EXISTS task_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER REFERENCES task(id),
    level TEXT NOT NULL CHECK (level IN ('INFO','WARN','ERROR')),
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_task_log_task_id ON task_log(task_id);
CREATE INDEX IF NOT EXISTS ix_task_log_level ON task_log(level);

CREATE TABLE IF NOT EXISTS download_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL,
    character_tag TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success','skipped','failed')),
    stage TEXT DEFAULT 'download',
    file_path TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_download_log_post_id ON download_log(post_id);
CREATE INDEX IF NOT EXISTS ix_download_log_character ON download_log(character_tag);
CREATE INDEX IF NOT EXISTS ix_download_log_status ON download_log(status);

-- ========== 系统统计表 ==========
CREATE TABLE IF NOT EXISTS system_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_posts INTEGER DEFAULT 0,
    total_tasks INTEGER DEFAULT 0,
    completed_tasks INTEGER DEFAULT 0,
    failed_tasks INTEGER DEFAULT 0,
    total_download_bytes INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 初始化系统统计
INSERT OR IGNORE INTO system_stats (id) VALUES (1);

-- 启用 WAL 模式（提升并发性能）
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-64000;
PRAGMA foreign_keys=ON;
