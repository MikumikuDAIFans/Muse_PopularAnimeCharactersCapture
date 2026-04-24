# Muse Intern Task v0.1

## Danbooru 热门角色识别与训练数据标准化管线

## 背景

`CK_Muse` 成立初期的核心任务，不是先做宏大的排行榜，而是先解决真实存在的训练数据问题。

以 `CKN` 为例，目前训练集中仍然存在 JPEG 压缩伪影、低质量样本混入、美学分布不稳定等问题。同时，包括 `CKN`、`anima` 在内的很多二次元模型，对热门角色的支持度仍然不够强。

这个课题的目标，是围绕热门角色建立一套可以复用的数据工程流程，为后续角色支持增强、审美模型、质量模型和 `Muse Score` 做准备。

## 课题目标

本课题分为两个阶段，但**第一阶段完成并验收通过后，才进入第二阶段**。

### 第一阶段

1. 产出一份仅基于 `Danbooru` 内部信息的角色名单表

### 第二阶段

1. 为榜单中的角色抓取可直接用于训练的数据，并落盘为标准格式

第一版只做 `Danbooru` 内部角色名单整理，不混入其他站点、社媒或人工主观排名。数据抓取任务作为后续进阶任务，在第一阶段结果稳定后再下发。

## 第一阶段：Danbooru 角色名单表

### 目标

建立一套可复现的角色筛选方法，并输出一份结构化角色名单表。

### 范围

- 仅使用 `Danbooru` 中 `category = character` 的 tag
- 角色名单中至少包含 `character tag`、对应 `copyright`、当前图片数量
- 第一阶段主目标为最近半年热门角色 `Top 200`
- “角色热度”及热门判定阈值研究作为附加题，不作为第一阶段的必做主项
- 第一版不做跨站融合，不做复杂人工修正

### 第一阶段核心字段

第一阶段产出的每条角色记录，至少应包含：

- `character_tag`
- `copyrights`
- `post_count`

推荐格式示例：

```json
{
  "character_tag": "frieren",
  "copyrights": ["frieren_beyond_journey's_end"],
  "post_count": 12345
}
```

其中：

- `character_tag`：角色主 tag
- `copyrights`：该角色关联的作品 tag，允许为一个或多个
- `post_count`：截止当前统计时点，该角色在 `Danbooru` 上的图片数量

在 `Danbooru` 中，大多数角色相关帖子本身就带有 `copyright` tag。因此第一阶段在多数情况下不需要“猜测角色属于哪个作品”，而是需要基于站内现有 tag 结构完成抽取、汇总和聚合。

对于少量缺失、歧义或多作品归属情况，可以：

- 保留为多值 `copyrights`
- 在说明文档中记录处理规则
- 对极少数异常角色标记为待人工确认

### 附加题：角色热度

“角色热度”在第一阶段中作为附加题存在，可以做，但不是必须完成项。

如果实习生完成基础名单后仍有余力，可以尝试增加：

- `popularity_score`
- `recent_post_count`
- `rank`

同时需要尝试回答以下研究问题：

- 一个 `character` tag 的 `post_count` 至少达到多少，可以进入热门角色候选池
- 最近半年新增图片数量至少达到多少，可以被视为当前仍然活跃
- 应该采用固定阈值、分层阈值，还是综合评分方式来判定“热门角色”

### 推荐规则（附加题）

第一版建议使用以下热度分数：

```text
popularity_score =
0.7 * normalized_total_post_count +
0.3 * normalized_recent_post_count
```

其中：

- `total_post_count`：角色 tag 的历史总作品量
- `recent_post_count`：近 6 个月或近 12 个月新增作品量

### 最低要求

- 只保留 `character` tag
- 去掉明显非角色实体 tag
- 去掉明显歧义或错误 tag
- 设置最低样本量门槛
- 尽量处理 alias 或同角色多写法
- 尽量补齐该角色对应的 `copyright` 信息
- 输出的 `post_count` 需注明统计时点或生成时间
- 对少量缺失或歧义 `copyright` 的角色给出备注策略

### 交付物

- `character_list_recent_6m_top_200.json`
- `character_list_recent_6m_top_200.csv`
- 名单生成脚本
- 一份简短说明文档，解释字段含义与筛选逻辑
- 如果完成附加题，可额外提交热门判定阈值分析与热度计算说明

`JSON` 作为正式数据源，`CSV` 作为人工查看和轻量分析格式。两者核心字段应保持一致；对于 `copyrights` 这类多值字段，`JSON` 使用数组，`CSV` 使用 `|` 连接。

### 验收标准

- 同样输入条件下可复现
- 字段完整，至少包含 `character_tag`、`copyrights`、`post_count`
- Top 结果整体上符合常识，不出现大量明显错误 tag
- `copyright` 关联结果整体合理，可支持后续按作品维度筛选角色
- 最近半年 `Top 200` 名单完整输出，且 `JSON` / `CSV` 内容一致

## 第一阶段验收通过条件

只有在满足以下条件后，才进入第二阶段：

- 榜单生成脚本可重复运行
- 最近半年 `Top 200` 结果整体符合常识
- 角色 tag 清洗、歧义过滤和 alias 处理已有初版方案
- 输出字段完整，至少能直接驱动后续按角色维度的数据抓取
- 角色与 `copyright` 的映射关系已有可用初版

## 第二阶段：热门角色训练数据抓取

### 目标

为热门角色抓取一批可用于后续训练、评估和清洗的数据，输出标准化目录。

### 每条样本必须包含

- 原图
- `txt` caption 文件
- `json` 元数据文件

### 推荐目录结构

```text
dataset/
  character_name/
    12345678.jpg
    12345678.txt
    12345678.json
```

这个结构优先服务后续 `kohya` 等训练脚本的直接使用，也方便后续二次清洗。

## Caption 规范 v0.1

`txt` 中的标签顺序必须统一，第一版固定为：

1. 主体标签
2. Character
3. Copyright
4. Artist
5. General tag
6. Other

即：

```text
subject -> character -> copyright -> artist -> general -> other
```

### 各分组说明

#### 1. Subject

放在最前面，表示主体人数与基础构图。

例如：

- `1girl`
- `1boy`
- `2girls`
- `2boys`
- `solo`
- `multiple_girls`
- `multiple_boys`

#### 2. Character

角色标签，放在主体标签之后。

例如：

- `hatsune_miku`
- `asuna_(blue_archive)`
- `frieren`

#### 3. Copyright

作品标签。

例如：

- `vocaloid`
- `blue_archive`
- `frieren_beyond_journey's_end`

#### 4. Artist

画师标签。

第一版 `txt` 中保留，后续训练时可以做可选开关；`json` 中必须完整保留。

#### 5. General tag

一般属性标签，是训练 caption 的主要内容。

例如：

- 发型
- 发色
- 服装
- 表情
- 动作
- 场景
- 镜头

#### 6. Other

暂时保留但优先级较低的其他标签。

## Caption 示例

```text
1girl, hatsune_miku, vocaloid, ixy, long_hair, twintails, blue_hair, sleeveless, necktie, smile, outdoors
```

## JSON 元数据要求

`json` 必须同时保留原始信息、分组结果和最终 caption。

建议至少包含这些字段：

```json
{
  "post_id": 12345678,
  "file_url": "...",
  "source_url": "...",
  "width": 1024,
  "height": 1536,
  "file_ext": "jpg",
  "rating": "s",
  "score": 120,
  "fav_count": 340,
  "created_at": "...",
  "raw_tag_string": "...",
  "tag_groups": {
    "subject": ["1girl", "solo"],
    "character": ["hatsune_miku"],
    "copyright": ["vocaloid"],
    "artist": ["ixy"],
    "general": ["long_hair", "twintails", "blue_hair", "smile"],
    "other": ["highres"]
  },
  "caption_v1": "1girl, hatsune_miku, vocaloid, ixy, long_hair, twintails, blue_hair, smile, highres"
}
```

## 技术要求

- 下载流程需要支持断点续跑
- 文件命名需要稳定，建议基于 `post_id`
- 下载失败、图片失效、无权限样本要有日志
- `txt` 要使用清洗后的 tag
- `json` 必须保留原始 tag 和最终输出 caption
- 代码应尽量模块化，方便后续接入质量过滤、审美筛选和去重流程

## 课题价值

这不是一个单纯的“爬图任务”。

它的真正价值在于为 `CK_Muse` 建立以下基础能力：

- 热门角色定义能力
- Danbooru tag 分组与标准化能力
- 训练前数据工程流程
- 后续质量筛选与审美筛选的输入标准

未来这些成果可以直接服务：

- `CKN` 和其他模型的热门角色支持增强
- 角色一致性评估
- 数据集质量治理
- `Muse Score` 的角色相关子指标

## 第一版建议节奏

### 第 1 周

- 调研 `Danbooru` tag 结构
- 输出最近半年 `Top 200` 的初版候选名单
- 观察 `post_count` 与近半年新增量的分布

### 第 2 周

- 完成热门角色榜单脚本
- 完成 alias、歧义 tag、最低样本量等基础过滤
- 形成最近半年 `Top 200` 的稳定输出
- 输出 `JSON + CSV` 双格式结果

### 第 3 周

- 对第一阶段结果做人工抽检和修正
- 补说明文档与验收材料
- 如果有余力，补充热门阈值或热度评分的分析结论
- 通过验收后，再启动第二阶段的数据抓取

## 第二阶段建议节奏（在第一阶段通过后启动）

### 第 1 周

- 完成图片、`txt`、`json` 的基础抓取与落盘
- 确认目录结构与字段规范

### 第 2 周

- 优化 caption 分组
- 增加日志、失败重试、断点续跑

### 第 3 周

- 产出 1 到 3 个角色的完整示例数据集

## 备注

当前阶段以数据工程和研究验证为目标。抓取、存储和后续使用需要遵守目标站点规则及适用法律政策。
