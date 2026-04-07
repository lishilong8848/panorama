function cloneItems(items = []) {
  return items.map((item) => ({ ...item }));
}

const AUTO_GROUP = {
  id: 'auto',
  title: '自动流程',
  items: [
    {
      id: 'auto_flow',
      title: '每日用电明细自动流程',
      desc: '单次执行整条月报主流程，并保留断点续传入口。',
    },
    {
      id: 'multi_date',
      title: '多日用电明细自动流程',
      desc: '按日期区间批量补跑，适合回补连续日期。',
    },
  ],
};

const MANUAL_GROUP = {
  id: 'manual',
  title: '人工处理',
  items: [
    {
      id: 'manual_upload',
      title: '手动补传（月报）',
      desc: '使用已有文件补传单个楼栋，不重新执行内网下载。',
    },
    {
      id: 'sheet_import',
      title: '5Sheet 导入',
      desc: '清空后重建 5 个工作表，用于手工修复或覆盖。',
    },
  ],
};

const SPECIAL_GROUP = {
  id: 'special',
  title: '专项任务',
  items: [
    {
      id: 'handover_log',
      title: '交接班日志',
      desc: '优先读取共享文件生成交接班，并带审核与后续上传。',
    },
    {
      id: 'day_metric_upload',
      title: '12项独立上传',
      desc: '按日期读取共享文件并重写 12 项，不依赖交接班审核链路。',
    },
    {
      id: 'wet_bulb_collection',
      title: '湿球温度定时采集',
      desc: '复用交接班日志源文件提取湿球温度和冷源运行模式，并写入多维表。',
    },
    {
      id: 'monthly_event_report',
      title: '体系月度统计表',
      desc: '读取上一个自然月的新事件处理数据，按楼栋生成事件月度统计表到本地目录。',
    },
    {
      id: 'alarm_event_upload',
      title: '告警信息上传',
      desc: '读取 08/16 共享告警文件，筛选 60 天内记录并写入多维表。',
    },
  ],
};

const MONITOR_GROUP = {
  id: 'monitor',
  title: '系统监控',
  items: [
    {
      id: 'runtime_logs',
      title: '运行日志',
      desc: '查看任务执行、共享桥接推进和系统日志。',
    },
  ],
};

const ROLE_MENU_GROUPS = {
  external: [AUTO_GROUP, MANUAL_GROUP, SPECIAL_GROUP, MONITOR_GROUP],
  internal: [],
};

export const DASHBOARD_MENU_GROUPS = ROLE_MENU_GROUPS.external.map((group) => ({
  ...group,
  items: cloneItems(group.items),
}));

export function getDashboardMenuGroupsForRole(roleMode) {
  const normalized = String(roleMode || '').trim().toLowerCase();
  const key = ['internal', 'external'].includes(normalized) ? normalized : 'external';
  return (ROLE_MENU_GROUPS[key] || ROLE_MENU_GROUPS.external).map((group) => ({
    ...group,
    items: cloneItems(group.items),
  }));
}

