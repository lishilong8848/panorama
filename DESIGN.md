# DESIGN.md

## 0. 项目范围
本文件定义 `全景平台月报自动定时上传` 的前端设计约束。

这是一个中文业务控制台，不是营销官网，也不是展示型 SaaS 落地页。

本文件适用于以下前端入口和其衍生模块：
- `web/frontend/src/style.css`
- `web/frontend/src/app_dashboard_template.js`
- `web/frontend/src/app_status_template.js`
- `web/frontend/src/app_config_template.js`
- `web/frontend/src/handover_review_app.js`
- 以及所有通过 view-model 驱动渲染的状态卡、配置卡、审核页、共享文件卡

后续所有前端改动默认先遵守本文件。新增模块、重构旧模块、修文案、补状态页，都不应绕过这份约束。

## 1. Visual Theme & Atmosphere
### 1.1 总体气质
整体气质应接近“工程化、克制、可审计”的控制台：
- 明亮桌面优先
- 蓝灰色控制面板氛围
- 强状态感、低噪声
- 信息层级清晰，优先服务操作判断

参考方向只限于结构和节奏，不直接复刻品牌风格：
- `Linear`：信息层级清晰、结构克制
- `Vercel`：工程感强、排版干净

最终视觉必须回到本项目现有的中文业务控制台语义，不允许做成：
- 营销页
- 炫技型仪表盘
- 深色优先 hacker UI
- 每个模块各用一套配色的“拼贴式后台”

### 1.2 设计目标
页面必须优先回答这几个问题：
1. 当前发生了什么
2. 影响范围是什么
3. 何时开始、最近一次结果是什么
4. 操作人下一步应该做什么

状态优先于装饰，操作优先于品牌表达。

## 2. Color Palette & Roles
颜色必须以 `web/frontend/src/style.css` 为唯一真相源，不允许随功能新增私有 accent 色。

### 2.1 基础表面色
- `--bg-app: #e9eff3`
- `--bg-work: #f5f8fb`
- `--surface-1: #ffffff`
- `--surface-2: #f3f7fa`
- `--surface-3: #e9eef4`

### 2.2 边框
- `--border-soft: #d7e0e8`
- `--border-strong: #aebdca`

### 2.3 文本
- `--text-1: #14212b`
- `--text-2: #405261`
- `--text-3: #6a7b88`
- `--text-inverse: #f3f7fb`

### 2.4 语义色
- `--tone-primary: #0067d1`
- `--tone-info: #007ea7`
- `--tone-success: #1c8c4b`
- `--tone-warning: #c77700`
- `--tone-danger: #bf3d30`
- `--tone-neutral: #607282`

### 2.5 语义背景
- `--tone-primary-bg: #e8f1ff`
- `--tone-info-bg: #e6f6fb`
- `--tone-success-bg: #e8f7ee`
- `--tone-warning-bg: #fff4e5`
- `--tone-danger-bg: #fdeceb`
- `--tone-neutral-bg: #eef2f5`

### 2.6 Shell / Chrome
- `--chrome-bg: #102635`
- `--chrome-bg-2: #16354c`
- `--chrome-line: rgba(255, 255, 255, 0.12)`

### 2.7 使用规则
- 语义色只用于状态、CTA 强调、焦点和重要反馈
- `warning` 和 `danger` 必须在 badge 和卡片语义上可明显区分
- 同一种状态在不同模块中必须保持同一颜色语言
- 不允许为了单个功能发明新的“品牌色”

## 3. Typography Rules
### 3.1 字体
使用现有字体栈，不新增展示型字体：
- `"Source Han Sans SC"`
- `"Noto Sans SC"`
- `"PingFang SC"`
- `"Microsoft YaHei"`
- `sans-serif`

### 3.2 层级
- 模块标题：约 `26px`，强标题，优先单行
- 卡片标题：约 `18px` 到 `20px`
- 小节标题 / kicker：约 `12px` 到 `14px`，高字重
- 正文：约 `13px` 到 `14px`
- hint / meta / 路径 / 时间：约 `12px` 到 `13px`
- badge：约 `12px`，高字重

### 3.3 文字行为
- 长路径、长错误、长说明必须换行
- 状态短语必须短、稳、可扫描
- hint 用于解释状态，不重复标题
- 中文文案必须直接、业务化、可执行，不要营销腔

## 4. Language & Copy Rules
### 4.1 硬规则
前端所有**用户可见固定文案**默认必须使用中文。

这条规则是强约束，不是建议。后续 AI 或工程改前端时，默认先检查是否满足“全部中文”。

### 4.2 必须中文的范围
以下内容必须是中文：
- 页面标题
- 按钮文案
- 菜单名称
- badge / 状态文字
- hint / banner / toast / 弹窗
- 表单标签
- 输入占位
- 空状态
- 校验提示
- 审核页固定说明
- 冲突提示
- 步骤说明
- 状态总览和业务控制台中的卡片说明

### 4.3 不允许出现的情况
- 新增英文 UI 文案
- 半中半英文案
- 乱码文案
- 开发者内部短语直接暴露给用户

### 4.4 内部技术标识不受影响
以下内容可以继续保持技术命名，不需要翻译：
- 变量名
- 函数名
- API 路径
- action key
- feature key
- CSS class
- 文件名模式
- 数据库字段名

### 4.5 动态技术值的展示规则
如果页面必须展示技术原文，例如：
- 文件路径
- URL
- 文件名
- 任务 ID
- 后端原始错误详情

则允许值本身原样显示，但外围标签和解释必须是中文，例如：
- `文件：`
- `路径：`
- `任务编号：`
- `错误详情：`

### 4.6 文案风格
中文文案必须满足：
- 短
- 直接
- 可操作
- 不煽动
- 不口语化过度

优先使用这类短语：
- `已就绪`
- `补采中`
- `等待中`
- `等待恢复`
- `待确认`
- `已确认`
- `失败`
- `最近异常`
- `建议动作`

## 5. Component Stylings
### 5.1 Status Badge
权威类名：
- `status-badge`
- `status-badge-soft`
- `status-badge-solid`
- `status-badge-outline`

规则：
- badge 使用完整圆角 pill
- `soft` 用于行级 / 卡片内状态
- `solid` 用于模块级摘要
- `outline` 只少量使用
- badge 必须与标题顶部对齐
- badge 文案保持短句，不承载长解释

### 5.2 核心卡片
核心类名：
- `status-card`
- `task-block`
- `content-card`
- `source-cache-family-card`
- `source-cache-building-card`
- `review-matrix-item`

规则：
- 卡片必须像运营面板，不像营销 tile
- 优先白底或浅表面色
- 软边框、软阴影，不做重质感玻璃拟物
- 一张卡只承载一个主要语义

### 5.3 Dashboard Menu
类名：
- `dashboard-menu`
- `dashboard-menu-group`
- `dashboard-menu-button`

规则：
- 左侧菜单是控制导轨，不是装饰导航
- 菜单项展示标题和说明，不做 icon-only
- 激活态使用当前既有蓝色强调和清晰反差

### 5.4 Module Hero
类名：
- `module-hero`
- `module-kicker`
- `module-title`
- `module-hero-metric`

规则：
- hero 是信息引导，不是视觉秀场
- 用于说明模块作用域、当前重点和关键指标
- 文案保持短，不写大段宣讲

### 5.5 Shared Source Cache Cards
权威类名：
- `source-cache-family-card`
- `source-cache-building-card`

规则：
- 内外网共享文件状态卡都必须使用这组专用结构
- 不允许用 `internal-download-slot` 替代外网 source-cache 卡
- family card 头部只放标题和 badge
- building card 头部只放楼栋名和 badge
- 路径、bucket、参考日期、补采说明、错误详情全部放到 hint 行

### 5.6 Review Matrix
类名：
- `review-matrix`
- `review-matrix-item`
- `review-matrix-head`

规则：
- 每个楼栋是一个独立审批 tile
- 确认状态与云表同步状态是并列主状态
- 链接、路径、说明必须可换行

### 5.7 Buttons
规则：
- 按钮是功能动作，不是装饰物
- 强主按钮只能给真正核心动作
- 一个小区域不要堆多个同强度主按钮
- 下载、重试、补采等局部动作优先用次级或 ghost 风格

## 6. Layout Principles
### 6.1 桌面优先
当前项目默认是桌面优先：
- 宽屏主壳布局
- 左侧模块菜单 + 右侧主内容
- 状态和执行卡优先多列展示
- 5 个楼栋状态优先横向可比

### 6.2 间距
使用现有 8px 系节奏：
- 常用间距：`8 / 10 / 12 / 14 / 16 / 18`
- 卡片内必须留出阅读呼吸感
- 不允许把卡片压成高密度表格碎片

### 6.3 卡头规则
所有业务卡统一遵守：
- 第一行只放：标题 + badge
- 第二行开始才放 summary 和 hint
- 路径、错误、时间、bucket 不得和 badge 混在同一行

### 6.4 信息优先级
展示顺序优先为：
1. 当前状态
2. 作用范围
3. 时间 / bucket / 最近结果
4. 最近异常
5. 路径 / 文件细节

## 7. Depth & Elevation
阴影和层级以现有 token 为准：
- `--shadow-card: 0 10px 30px rgba(16, 38, 53, 0.06)`
- `--shadow-hover: 0 18px 36px rgba(16, 38, 53, 0.12)`
- `--shadow-chrome: 0 20px 44px rgba(8, 25, 37, 0.22)`

规则：
- 普通卡片使用软层级
- hover 只做轻微增强，不做夸张跳变
- 壳层、抽屉、overlay 可以使用更重的 chrome shadow
- 层级用于表达关系，不用于制造炫技感

## 8. Do's and Don'ts
### 8.1 Do
- 优先复用现有 token
- 优先扩展现有组件，而不是发明新结构
- 让卡片先回答状态，再展示细节
- 把长说明拆到 hint 行
- 保持 5 个楼栋之间的可比性
- 保持前端固定文案全部中文
- 修改 `src` 时同步考虑 `dist` 是否需要一致

### 8.2 Don't
- 不要混用多个互相冲突的设计风格
- 不要为某个模块单独发明 accent 色
- 不要把 badge、路径、时间、错误塞进同一头部行
- 不要让一个局部类名临时承载完全不同的语义
- 不要为了“看起来高级”牺牲状态可读性
- 不要新增英文 UI 文案、半中半英文案或乱码文案

## 9. Responsive Behavior
当前断点以代码为准：
- `max-width: 1320px`
- `max-width: 960px`

规则：
- `1320px` 以下，密集多列卡片可以逐步折叠
- `960px` 以下，dashboard menu 可切抽屉
- 移动端保证能访问，但复杂操作以桌面体验为主
- 任何组件都不能依赖 hover 作为唯一语义来源

## 10. Project-Specific Guardrails For Agents
后续 AI 或工程改 UI 时，默认遵守以下规则：

1. 先看 `style.css`，不要凭空造 token
2. 先找现有卡片结构，再决定是否新增组件
3. 状态页是诊断页，不是视觉展示页
4. source-cache 卡必须走专用 family/building card 结构
5. 内外网可以共用视觉原语，但不能共用误导性业务语义
6. 所有用户可见固定文案默认必须中文
7. 如果页面展示技术原文，外围标签和解释必须中文
8. 如果运行环境可能加载 `dist`，修改 `src` 后必须同步检查 `dist`

## 11. Agent Prompt Guide
后续给 AI 的提示可以直接使用这类短句：
- “使用项目根目录 DESIGN.md，不要发明新主题。”
- “保持当前中文业务控制台风格，不要做成营销页。”
- “先复用现有 source-cache family/building card 结构。”
- “标题和 badge 放头部，所有长说明放 hint 行。”
- “所有前端固定显示文案必须是中文。”
- “动态路径和原始错误可以保留原文，但标签必须中文。”

## 12. Initial Reference Direction
外部参考只允许作为结构参考，不是最终风格真相源。

若需要 mood reference，优先参考：
- `Linear`
- `Vercel`

但最终必须回到：
- 当前项目现有 token
- 当前业务控制台结构
- 当前中文操作语义

任何外部参考都不能覆盖本文件。
