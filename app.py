#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3D 打印文件进度工作台 —— 本地版（零云端 / 零令牌）
====================================================
单文件后端，同时干两件事:
  1) 订阅多台拓竹(Bambu Lab)打印机局域网 MQTT(端口 8883, TLS)，实时解析每台状态:
        空闲 / 打印中 / 暂停 / 已完成 / 失败 + 当前文件名 / 进度% / 剩余分钟 / 喷嘴温度
  2) 起一个本地 HTTP 服务，把仪表盘网页也托管出来，并提供 /api 接口读写"文件进度"。

数据全部留在本地:
  * 打印机实时状态 -> 内存(被 MQTT 推送实时刷新)
  * 文件进度(7 类状态、人工维护) -> data/files.json

完全不依赖 WorkBuddy 云端、不需要任何令牌、可断网运行。

运行（建议 7x24 常开的那台局域网电脑）:
    pip install paho-mqtt
    python app.py
  浏览器打开: http://<这台电脑的局域网IP>:8000
  （同局域网下的手机/平板/其他电脑也能用这个地址打开）

可选参数:
    python app.py --config config.json --port 8000

重要:
  * 拓竹每台打印机同时只允许 1 个 MQTT 客户端连接。脚本运行期间，
    请勿用 Bambu Studio / 手机 App 连同一台打印机，否则会互相踢线。
  * 空闲的打印机可能只发心跳包，因此脚本会周期性(默认 60s)向每台重发一次
    pushall 指令，确保状态持续刷新。
"""

import argparse
import json
import ssl
import time
import datetime
import threading
import socket
import re
import sys
import os
import urllib.parse
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.stderr.write("【错误】缺少依赖 paho-mqtt，请先运行: pip install paho-mqtt\n")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
DATA_DIR = ROOT / "data"
FILES_PATH = DATA_DIR / "files.json"

OFFLINE_AFTER = 90          # 秒: 超过该时间没收到消息视为掉线 -> "未连接"
RECONNECT_BASE = 5          # 秒: 重连基础间隔
PUSHALL_INTERVAL = 60       # 秒: 周期性重新请求一次完整状态

# 允许「操作文件」的客户端 IP 白名单（仅这些 IP 可增删改/清空/导入）。
# 其余 IP 仅可查看，任何写操作返回 403。可在 config.json 的 "allowed_ips" 覆盖。
ALLOWED_IPS = ["192.168.1.26"]

# 拓竹 gcode_state -> 中文 (注意真实值多为 FINISH 而非 FINISHED)
STATE_MAP = {
    "IDLE": "空闲",
    "RUNNING": "打印中",
    "PREPARE": "打印中",
    "PAUSED": "暂停",
    "FINISH": "已完成",
    "FINISHED": "已完成",
    "FAILED": "失败",
    "OFFLINE": "离线",
}

# 拓竹「错误代码」权威字典（来源：Bambu Lab Wiki 错误代码页，全量内置）。
# key 用真实细粒度整型码；描述沿用 wiki 中文原文。
# 同一含义的多个 AMS 槽位变体（0700/0701/0702/0703/07FF/1200/1201/1202/1203/12FF 等）均收录。
# 未收录的细码会通过「大类高位兜底」回退到 EXCEPTION_CODE_MAP。
# 格式说明：wiki 形如 "0300-800A (0300800A)"，0300800A 即本字典的整型 key。
ERROR_CODE_MAP = {
    0x00000000: "无错误",
    # ===== 工具头 / 打印头（0300）=====
    0x03004000: "Z轴回零失败，打印已停止",
    0x03004002: "热床调平失败，打印已停止",
    0x03004005: "喷嘴风扇转速异常",
    0x03004006: "喷嘴堵头",
    0x03004008: "AMS换料失败",
    0x03004009: "XY回零失败",
    0x0300400A: "振动扫频识别失败",
    0x0300400B: "内部通讯异常",
    0x0300400C: "打印已取消",
    0x0300400D: "断电续打失败",
    0x03004001: "工具头降温超时",
    0x03008000: "未知原因暂停（可点击继续打印恢复）",
    0x03008001: "用户暂停（可点击继续打印恢复）",
    0x03008002: "激光雷达检测到首层缺陷，请检查打印件状况后决定是否继续",
    0x03008003: "AMS助力电机过载，可能因为耗材缠住或料盘卡住",
    0x03008004: "打印耗材用尽，请加载新料",
    0x03008005: "工具头前盖掉落，请把前盖重新装上并检查打印件是否完好",
    0x03008006: "未检测到打印板定位标识，请检查打印板是否放置且四角对齐并保证标识清晰无磨损",
    0x03008007: "检测到断电时有未完成的打印任务（若模型仍粘接底板可尝试继续打印）",
    0x03008008: "喷嘴温控故障，打印已停止",
    0x03008009: "热床温控故障",
    0x0300800A: "AI打印监控检测到垃圾桶处废料堆积，请及时清理",
    0x0300800B: "切刀已卡住，请确保切刀刀柄弹回",
    0x0300800C: "检测到丢步并自动恢复，请继续打印并观察是否有错层现象",
    0x0300800D: "检测到有打印件倒塌或喷嘴没有正常挤出（可接受则继续打印恢复）",
    0x0300800E: "检测到打印文件不可用，请确认存储介质是否被移除",
    0x0300800F: "检测到门已打开，暂停打印",
    0x03008010: "热端风扇转速异常，打印已停止",
    0x03008011: "检测到的打印板类型与切片G代码中不一致，请修改切片参数或使用匹配打印板",
    0x03008013: "检测到挤出机裹料或构建板放歪（可清理/调整或继续打印）",
    0x03008014: "检测到挤出机裹料或构建板放歪，请根据实际状态取消打印或继续打印",
    0x03008015: "打印耗材用尽，请在“耗材”页面加载新料",
    0x03008016: "检测到挤出机无法挤出料（可清理挤出机或继续打印）",
    0x03008017: "检测到热床上有异物，请检查并清理后继续打印",
    0x03008018: "腔体温控故障",
    0x03008019: "没有放置打印板",
    # ===== AMS 助力电机过载（07xx/12xx 8003/8010）=====
    0x07008003: "AMS助力电机过载，可能是因为耗材缠住或料盘卡住",
    0x07018003: "AMS助力电机过载，可能是因为耗材缠住或料盘卡住",
    0x07FF8010: "AMS助力电机过载，请检查料盘或料线是否卡住，排除异常后点击重试",
    0x07008010: "AMS助力电机过载，可能是因为耗材缠住或料盘卡住",
    0x07018010: "AMS助力电机过载，可能是因为耗材缠住或料盘卡住",
    0x07028010: "AMS助力电机过载，请检查料盘或料线是否卡住，排除异常后点击重试",
    0x07038010: "AMS助力电机过载，请检查料盘或料线是否卡住，排除异常后点击重试",
    0x12008010: "耗材或者料盘可能卡住",
    0x12018010: "请检查料盘或料线是否卡住，排除异常后点击重试",
    0x12028010: "请检查料盘或料线是否卡住，排除异常后点击重试",
    0x12038010: "请检查料盘或料线是否卡住，排除异常后点击重试",
    0x12FF8010: "请检查耗材或者料盘是否卡住",
    # ===== AMS 耗材用尽（07xx/12xx/07FF 8011）=====
    0x07008011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x07018011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x07028011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x07038011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x12008011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x07FF8011: "AMS耗材用尽，请把耗材放入同一个AMS槽位后继续",
    0x12FF8011: "检测到AMS材料耗尽，请放新料线后点击重试",
    0x12018011: "检测到AMS材料耗尽，请放新料线后点击重试",
    0x12028011: "检测到AMS材料耗尽，请放新料线后点击重试",
    0x12038011: "检测到AMS材料耗尽，请放新料线后点击重试",
    # ===== AMS 映射表获取失败（8012）=====
    0x07FF8012: "多次获取AMS映射表失败，请点击重试继续",
    0x12FF8012: "多次获取AMS映射表失败，请点击重试继续",
    0x07008012: "多次获取AMS映射表失败，请点击重试继续",
    0x07018012: "多次获取AMS映射表失败，请点击重试继续",
    0x07028012: "多次获取AMS映射表失败，请点击重试继续",
    0x07038012: "多次获取AMS映射表失败，请点击重试继续",
    0x12008012: "多次获取AMS映射表失败，请点击重试继续",
    0x12018012: "多次获取AMS映射表失败，请点击重试继续",
    0x12028012: "多次获取AMS映射表失败，请点击重试继续",
    0x12038012: "多次获取AMS映射表失败，请点击重试继续",
    # ===== 工具头内部料线位置检查失败（8014）=====
    0x12018014: "工具头内部料线位置检查失败，请查看HMS帮助排除异常后点击重试",
    0x12028014: "工具头内部料线位置检查失败，请查看HMS帮助排除异常后点击重试",
    0x12038014: "工具头内部料线位置检查失败，请查看HMS帮助排除异常后点击重试",
    0x12008014: "无法检查工具头内部的耗材位置，可查看助手了解具体原因和解决方法",
    # ===== 切刀卡住（8002）=====
    0x07FF8002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x12FF8002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x07008002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x07018002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x07028002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x07038002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x12008002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x12018002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x12028002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    0x12038002: "检测到切刀卡住，请拨出刀柄后点击重试按钮",
    # ===== 切料失败（8001）=====
    0x07FF8001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x12FF8001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x07008001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x07018001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x07028001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x07038001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x12018001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x12028001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    0x12038001: "检测到切料失败，请检查切刀排除异常后点击重试按钮",
    # ===== 冲刷旧料超时（8013）=====
    0x12FF8013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x12008013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x12018013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x12028013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x12038013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x07FF8013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x07008013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x07018013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x07028013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    0x07038013: "检测到冲刷旧料超时，请检查料线是否卡住或挤出机堵头排除异常后点击重试",
    # ===== 料线退回 AMS 失败（8004）=====
    0x07FF8004: "料线从工具头退回AMS失败，请检查料线或料盘是否卡住排除异常后点击重试",
    0x07028004: "料线从工具头退回AMS失败，请检查料线或料盘是否卡住排除异常后点击重试",
    0x07038004: "料线从工具头退回AMS失败，请检查料线或料盘是否卡住排除异常后点击重试",
    0x12FF8004: "料线从工具头退回失败，请检查料线是否卡住排除异常后点击重试",
    0x12008004: "料线从工具头退回失败，请检查料线是否卡住排除异常后点击重试",
    0x12018004: "料线从工具头退回失败，请检查料线是否卡住排除异常后点击重试",
    0x12028004: "料线从工具头退回失败，请检查料线是否卡住排除异常后点击重试",
    0x12038004: "料线从工具头退回失败，请检查料线是否卡住排除异常后点击重试",
    0x12018015: "料线从工具头退回失败，请检查料线是否卡住或断在挤出机内部排除异常后点击重试",
    0x12028015: "料线从工具头退回失败，请检查料线是否卡住或断在挤出机内部排除异常后点击重试",
    0x12038015: "料线从工具头退回失败，请检查料线是否卡住或断在挤出机内部排除异常后点击重试",
    0x07008004: "耗材从工具头退回AMS失败，可能是因为料盘卡住或者耗材的端头卡在路径中",
    0x07018004: "耗材从工具头退回AMS失败，可能是因为料盘卡住或者耗材的端头卡在路径中",
    # ===== 料线挤出失败/异常（8007）=====
    0x07028007: "料线挤出失败，请检查挤出机是否堵塞排除异常后点击重试",
    0x07038007: "料线挤出失败，请检查挤出机是否堵塞排除异常后点击重试",
    0x07018007: "料线挤出异常，请检查挤出机是否堵塞排除异常后点击重试",
    0x12018007: "料线挤出异常，可能堵头或缠料请查看HMS帮助排除异常后点击重试",
    0x12028007: "料线挤出异常，可能堵头或缠料请查看HMS帮助排除异常后点击重试",
    0x12038007: "料线挤出异常，可能堵头或缠料请查看HMS帮助排除异常后点击重试",
    0x12008007: "耗材挤出异常，可能由堵头或耗材卡住导致",
    0x07008007: "耗材挤出失败，请检查挤出机是否堵塞",
    # ===== 料线退出挤出机失败（8003）=====
    0x12008003: "料线退出挤出机失败，请检查挤出机是否堵塞或料线是否断在挤出机内部",
    0x12018003: "料线退出挤出机失败，请检查挤出机是否堵塞或料线是否断在挤出机内部",
    0x12028003: "料线退出挤出机失败，请检查挤出机是否堵塞或料线是否断在挤出机内部",
    0x12038003: "料线退出挤出机失败，请检查挤出机是否堵塞或料线是否断在挤出机内部",
    0x07028003: "料线退出挤出机失败，请检查是否发生堵头或者料线是否断在挤出机内部",
    0x07038003: "料线退出挤出机失败，请检查是否发生堵头或者料线是否断在挤出机内部",
    # ===== 料线送出 AMS 失败（8005）=====
    0x07FF8005: "料线送出AMS失败，请剪平料头检查料盘是否卡住后点击重试",
    0x07028005: "料线送出AMS失败，请剪平料头检查料盘是否卡住后点击重试",
    0x07038005: "料线送出AMS失败，请剪平料头检查料盘是否卡住后点击重试",
    0x07008005: "耗材无法从AMS送往打印机，请取出耗材后剪平料头重新插入并检查特氟龙管",
    0x07018005: "耗材无法从AMS送往打印机，请取出耗材后剪平料头重新插入并检查特氟龙管",
    0x12FF8005: "请插入料线后点击重试按钮",
    0x12008005: "请插入料线后点击重试按钮",
    0x12018005: "请插入料线后点击重试按钮",
    0x12028005: "请插入料线后点击重试按钮",
    0x12038005: "请插入料线后点击重试按钮",
    # ===== 料线送往挤出机失败（8006）=====
    0x07028006: "料线送往挤出机失败，请检查料线或料盘是否卡住排除异常后点击重试",
    0x07038006: "料线送往挤出机失败，请检查料线或料盘是否卡住排除异常后点击重试",
    0x12008006: "料线送往挤出机失败，请检查料线是否卡住排除异常后点击重试",
    0x12018006: "料线送往挤出机失败，请检查料线是否卡住排除异常后点击重试",
    0x12028006: "料线送往挤出机失败，请检查料线是否卡住排除异常后点击重试",
    0x12038006: "料线送往挤出机失败，请检查料线是否卡住排除异常后点击重试",
    0x07008006: "耗材送往挤出机失败，可能是因为耗材缠住或料盘卡住也可能是AMS特氟龙管未接上",
    0x07018006: "耗材送往挤出机失败，可能是因为耗材缠住或料盘卡住",
    # ===== 挤出机无法挤出料（8016）=====
    0x12008016: "检测到挤出机无法挤出料，请查看HMS帮助排除异常后若缺陷可接受请点击重试继续",
    0x12018016: "检测到挤出机无法挤出料，请查看HMS帮助排除异常后若缺陷可接受请点击重试继续",
    0x12028016: "检测到挤出机无法挤出料，请查看HMS帮助排除异常后若缺陷可接受请点击重试继续",
    0x12038016: "检测到挤出机无法挤出料，请查看HMS帮助排除异常后若缺陷可接受请点击重试继续",
    0x03008016: "检测到挤出机无法挤出料，请根据实际状态取消本次打印并清理挤出机或点击继续打印恢复",
    # ===== 禁用 AMS 时仍从 AMS 加载料（4001）=====
    0x12FF4001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x07004001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x07014001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x07024001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x07034001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x12004001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x12014001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x12024001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x12034001: "检测到禁用AMS时仍从AMS加载料，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    0x07FF4001: "检测到禁用AMS时仍从AMS加载耗材，请将耗材退回AMS并从料盘支架加载新料后重新发起打印",
    # ===== 外挂料盘相关（8003/8006/C003/C006）=====
    0x07FF8003: "请将外挂料盘的耗材拉出，若重复出现请检查是否有耗材断在工具头（如用AMS请连接特氟龙管）",
    0x07FFC003: "请将外挂料盘的耗材拉出，若重复出现请检查是否有耗材断在工具头（如用AMS请连接特氟龙管）",
    0x12FF8003: "请将外挂料盘的耗材拉出，若重复出现请检查是否有耗材断在工具头（如用AMS请连接特氟龙管）",
    0x12FFC003: "请将外挂料盘的耗材拉出，若重复出现请检查是否有耗材断在工具头（如用AMS请连接特氟龙管）",
    0x07FF8006: "请把耗材插入进料口，直到推不动为止",
    0x07FFC006: "请把耗材插入进料口，直到推不动为止",
    0x12FF8006: "请把耗材插入进料口，直到推不动为止",
    0x12FFC006: "请把耗材插入进料口，直到推不动为止",
    0x07FF8007: "请观察喷嘴，若耗材已被挤出点击完成；若未被挤出请往前轻推料线后点击重试",
    0x12FF8007: "请观察喷嘴，若耗材已被挤出点击完成；若未被挤出请往前轻推料线后点击重试",
    # ===== SD 卡 / 系统 / 网络（05xx）=====
    0x05004002: "不支持的打印文件路径或名称，请重新发起打印",
    0x05004003: "解析gcode.3mf遇到问题，打印已停止，请重新发送打印",
    0x05004004: "打印过程中无法启动新的打印，请当前打印任务结束后再发送新的打印",
    0x05004005: "不允许在固件更新过程中发送打印任务",
    0x05004006: "内部存储空间不足，可恢复出厂设置以释放所有可用空间",
    0x05004007: "需要修复升级和强制升级，不允许打印",
    0x05004008: "启动打印失败，请重启机器并重新发送打印",
    0x0500400A: "文件名不支持，请修改文件名并重新发送打印",
    0x0500400B: "文件下载错误，请检查网络并重新发送打印",
    0x0500400C: "请插入MicroSD卡，重新发送打印",
    0x0500400D: "请运行自检后重新启动打印作业",
    0x0500400E: "打印已取消",
    0x05004015: "设备存储空间不足以打印任务，请格式化或清理SD卡以释放可用空间",
    0x05004016: "MicroSD卡进入写保护，请更换MicroSD卡",
    0x0500401A: "云端访问失败，可能因网络干扰/无internet/防火墙限制，请移动打印机或检查路由器",
    0x0500401B: "云端访问返回值非法，多次失败请联系客服",
    0x0500401C: "云端访问被拒绝，多次失败请联系客服",
    0x0500401D: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x0500401E: "云端访问返回值非法，多次失败请联系客服",
    0x05004021: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x05004022: "云端访问返回值非法，多次失败请联系客服",
    0x05004024: "云端访问失败，可能因网络干扰/无internet/防火墙限制，请移动打印机或检查路由器",
    0x05004025: "云端访问返回值非法，多次失败请联系客服",
    0x05004026: "云端访问被拒绝，多次失败请联系客服",
    0x05004027: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x05004028: "云端访问返回值非法，多次失败请联系客服",
    0x05004029: "云端访问被拒绝，多次失败请联系客服",
    0x0500402A: "连接路由器失败，可能是无线环境干扰或距离较远，可重试或移动打印机到路由器附近",
    0x0500402B: "连接路由器密码错误，请检查密码后重试",
    0x0500402C: "未成功获取IP地址，可能无线干扰或路由器DHCP地址池满，可移动打印机或检查DHCP",
    0x0500402D: "系统发生异常",
    0x0500402E: "系统不支持SD卡当前文件系统，请更换SD卡或格式化为FAT32",
    0x0500402F: "SD卡扇区数据发生损坏，请用修复工具修复或格式化，仍无法识别请更换SD卡",
    0x0500C010: "SD卡读写异常，请重新插入或更换SD卡",
    0x05004014: "云端切片发生错误，请检查切片参数并重新发起打印",
    0x0500401F: "云端绑定授权超时，请检查手机或PC具备访问internet能力并保证绑定时Studio/Handy在前台",
    0x0501401F: "云端绑定授权超时，请检查手机或PC具备访问internet能力并保证绑定时Studio/Handy在前台",
    0x0500401D: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x0501401D: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x05014021: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x05014027: "云端访问失败，可能因网络干扰，请移动打印机到路由器附近重试",
    0x05014024: "云端访问失败，可能因网络干扰/无internet/防火墙限制，请移动打印机或检查路由器",
    0x0501401A: "云端访问失败，可能因网络干扰/无internet/防火墙限制，请移动打印机或检查路由器",
    0x0501401C: "云端访问被拒绝，多次失败请联系客服",
    # ===== 绑定 / 设备发现（0501/0514）=====
    0x05014032: "二维码绑定执行中，无法进行设备发现绑定，可扫码或退出二维码页尝试发现绑定",
    0x05014031: "设备发现绑定执行中，屏幕无法展现二维码，可等待局域网绑定结束或中止后重试",
    0x05014033: "打印区区域限制，请在对应区域商城下载APP注册后重试绑定",
    0x05014034: "切片进度长时间没有更新，打印任务退出，请确认参数并重新发起打印",
    0x05014035: "设备正在进行绑定中，无法响应绑定请求",
    0x05014038: "区域设置与打印机不匹配，请检查打印机的区域设置",
    0x05004019: "打印机已经绑定，请解除绑定然后重试",
    0x05014019: "打印机已经绑定，请解除绑定然后重试",
    0x05004017: "绑定操作失败，可重试或重启打印机后重试",
    0x05014017: "绑定操作失败，可重试或重启打印机后重试",
    0x05004018: "绑定配置信息解析失败，请重试",
    0x05014018: "绑定配置信息解析失败，请重试",
    0x05144039: "设备登录过期，请尝试重新绑定",
    # ===== AI 监控 / 视觉（0Cxx）=====
    0x0C008005: "检测到垃圾桶废料堆积，可能导致工具头撞料",
    0x0C008002: "检测到打印炒面问题",
    0x0C00800A: "检测到的打印板类型与切片选择的打印板类型不一致",
    0x0C00C003: "检测到首层可能存在打印瑕疵",
    0x0C00C004: "检测到可能发生了打印炒面失败问题",
    0x0C00C006: "检测到可能发生垃圾桶堆料问题",
    0x0C008001: "检测到首层打印的缺陷（可接受则继续打印恢复）",
    # ===== 热床/喷嘴材质提示（1000）=====
    0x1000C001: "热床温度较高，可能造成耗材软化堵头，请注意打印机开放通风",
    0x1000C002: "用不锈钢喷嘴打印碳纤维料可能造成喷嘴损坏",
    0x1000C003: "开启传统模式的延时摄影可能产生表面瑕疵，请根据需要启用",
    # ===== 切片参数 / 延时摄影（1001）=====
    0x1001C001: "切片参数中开启了旋转花瓶，无法支持延时摄影",
    0x1001C002: "切片参数中开启了逐件打印，无法支持延时摄影",
    # ===== 打印板定位（0C00 8009 / 0300 8006 已在前）=====
    0x05008036: "切片文件中预设机型与当前打印机型号不一致，是否继续",
    0x05004037: "切片文件中预设机型与当前打印机型号不兼容，不能进行打印",
    0x05004038: "切片文件中的喷嘴直径与当前设置不符，不能进行打印",
    0x0500403A: "当前环境温度过低，打印/轴移动等操作将被禁用，请移到10度以上环境",
}

# 大类高位兜底：当细码未在 ERROR_CODE_MAP 命中时，用高 16 位（分类）匹配，
# 给出一个粗略类别描述，避免直接裸显十六进制。
EXCEPTION_CODE_MAP = {
    0x0300: "工具头/打印头相关异常",
    0x0700: "AMS（耗材系统）相关异常",
    0x1200: "外部耗材/挤出相关异常",
    0x0500: "SD卡/系统/网络相关异常",
    0x0C00: "AI监控/视觉相关异常",
    0x1000: "热床/喷嘴材质相关提示",
    0x0514: "设备登录相关异常",
}


def _norm_code(code):
    """把各种输入格式归一化为整型错误码。支持:
       int(0x0300800A) / "0300800A" / "0300-800A" / "0300_800A" / "0x0300800A"。
       无法解析返回 None。"""
    if isinstance(code, (int, float)):
        return int(code)
    if not isinstance(code, str):
        return None
    s = code.strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    s = s.replace("-", "").replace("_", "")
    if len(s) == 8 and all(c in "0123456789abcdef" for c in s):
        return int(s, 16)
    return None


def error_desc(code, raw=None):
    """把 print_error / print_exception 转为可读中文。
    查找顺序：1) 真实细码精确命中；2) 大类高位兜底；3) 未知码原样显示十六进制。
    raw 为可选的原始字符串（如 "0300-800A"），用于补充归一化尝试。"""
    c = _norm_code(code)
    if c is None and raw:
        c = _norm_code(raw)
    if c is None:
        return None
    if c == 0:
        return None
    if c in ERROR_CODE_MAP:
        return ERROR_CODE_MAP[c]
    # 大类高位兜底（取高 16 位）
    cls = (c >> 16) & 0xFFFF
    if cls in EXCEPTION_CODE_MAP:
        return EXCEPTION_CODE_MAP[cls]
    return f"错误码 0x{c:08X}"
PAHO_V2 = hasattr(mqtt, "CallbackAPIVersion")

# 文件进度的状态流转（一键推进用）
NEXT = {
    "待打印": "打印中",
    "打印中": "待摆盘填表",
    "待摆盘填表": "已完成",
    "需改进重打": "打印中",
    "已完成": "归档",
}
VALID_STATUS = ["待打印", "打印中", "待摆盘填表", "需改进重打", "已完成", "已放弃", "归档"]

# "即将完成"提醒的防抖：打印必须已实际运行至少这么久(分钟)，
# 才信任"剩余<=10分钟"的读数。用于跳过刚下发/刚启动时报文的脏剩余时间(常被误报为1分钟)。
REMIND_GRACE_MIN = 3

state_lock = threading.Lock()
machine_state = {}          # name -> dict(状态字段)
jobs_lock = threading.Lock()
jobs = []                   # list[dict]
MACHINES = []              # 机器名顺序列表

cfg = {"host": "0.0.0.0", "port": 8000}


# 日志文件路径（按天滚动）：ROOT/logs/printsync_YYYY-MM-DD.log
LOG_DIR = ROOT / "logs"
_log_fh = None  # 当前打开的日志文件句柄（main 启动时初始化）


def ensure_log_file():
    """确保当天日志文件已打开；按天切换。返回文件句柄或 None。"""
    global _log_fh
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        path = LOG_DIR / f"printsync_{today}.log"
        if _log_fh is None or getattr(_log_fh, "_log_date", "") != today:
            if _log_fh is not None:
                try:
                    _log_fh.close()
                except Exception:
                    pass
            _log_fh = open(path, "a", encoding="utf-8")
            _log_fh._log_date = today
    except Exception as e:
        sys.stderr.write(f"【警告】日志文件初始化失败: {e}\n")
        _log_fh = None
    return _log_fh


def log(*a):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] " + " ".join(str(x) for x in a)
    print(line, flush=True)
    fh = ensure_log_file()
    if fh is not None:
        try:
            fh.write(line + "\n")
            fh.flush()
        except Exception:
            pass


# 北京时间 (UTC+8)
CST = datetime.timezone(datetime.timedelta(hours=8))


def now_iso():
    """当前时间，北京时间 (UTC+8)，格式 YYYY-MM-DDTHH:MM:SS+08:00。"""
    return datetime.datetime.now(CST).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def now_display():
    """当前北京时间的易读格式: YYYY-MM-DD HH:MM:SS。"""
    return datetime.datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def finish_est_display(remain_min):
    """根据剩余分钟，计算预计完成时间(北京时间)的易读字符串; 无数据返回 None。

    容错：剩余 < 1 分钟视为未知(避免显示"0分钟/过去时间")；
    若因上报延迟算出的预计完成早于当前时间，则按"即将完成"兜底，不显示过去的时间。
    """
    if not isinstance(remain_min, (int, float)) or remain_min <= 0:
        return None
    now = datetime.datetime.now(CST)
    eta = now + datetime.timedelta(minutes=float(remain_min))
    if remain_min < 1:
        return None
    # 防止脏数据(刚下发时剩余被误报为极小值)导致预计完成早于现在
    if eta <= now:
        return "即将完成（<1分钟）"
    return eta.strftime("%Y-%m-%d %H:%M:%S")


def today_iso():
    d = datetime.datetime.now(CST)
    return f"{d.year}-{d.month:02d}-{d.day:02d}"


def extract_state(payload):
    """从 MQTT report 报文解析出我们关心的字段。返回 dict 或 None。

    返回字段说明：
      raw_state : 报文里真实的 gcode_state 原文（可能为 None）
      state     : 推断后的中文状态（已做增量保持，不会把"打印中"降级成"在线"）
      其余为文件名/进度/温度等遥测。
    """
    try:
        obj = json.loads(payload)
    except Exception:
        return None
    pr = obj.get("print") or {}
    state_raw = pr.get("gcode_state") or obj.get("system_status")
    # 文件名多字段兜底：拓竹打印中常用 subtask_name，gcode_file 可能为路径或空
    gcode = (pr.get("subtask_name")
             or pr.get("gcode_file")
             or pr.get("task_name")
             or pr.get("file_name") or "")
    if gcode and gcode.lower().endswith((".gcode", ".3mf")):
        gcode = gcode.rsplit("/", 1)[-1]
    progress = pr.get("mc_percent") or pr.get("progress") or pr.get("print_percent")
    remain = pr.get("mc_remaining_time") or pr.get("remaining_time")
    temp = pr.get("nozzle_temper", pr.get("temper", pr.get("nozzle")))
    # ---- 错误/异常解析 ----
    # 拓竹出错(如前盖脱落)会自动暂停，并带 print_error(非0)/print_exception 字段。
    # 这两个字段是"事件型"的: 出错时上报一次，后续增量心跳可能不再携带(或带0表示已清除)。
    # 设计三态：
    #   err_code = 非0整数  -> 明确出错(带描述)
    #   err_code = 0        -> 报文"显式"带 print_error=0，表示打印机已清除错误(恢复)
    #   err_code = None     -> 报文完全没带错误字段(增量心跳常见)，交由 merge_state 保持上一状态
    raw_err = pr.get("print_error")
    raw_exc = pr.get("print_exception")
    err_code = None
    err_explicit_zero = False   # 是否"显式携带了" print_error/print_exception 且为 0
    if isinstance(raw_err, (int, float)):
        if int(raw_err) != 0:
            err_code = int(raw_err)
        else:
            err_explicit_zero = True
    elif isinstance(raw_exc, (int, float)):
        if int(raw_exc) != 0:
            err_code = int(raw_exc)
        else:
            err_explicit_zero = True
    err_desc = error_desc(err_code) if err_code is not None else None
    # ---- AMS 料盘解析 ----
    # 结构: print.ams.ams[] -> 每个 AMS 单元有 tray[] 数组; tray_now/tray_tar 指示当前在用料盘(十进制槽位)
    ams_obj = pr.get("ams") or {}
    ams_units = ams_obj.get("ams") if isinstance(ams_obj, dict) else None
    tray_now = ams_obj.get("tray_now") if isinstance(ams_obj, dict) else None
    ams_list = []
    if ams_units:
        for unit in ams_units:
            if not isinstance(unit, dict):
                continue
            unit_id = unit.get("id", "?")
            trays = unit.get("tray") or []
            for tr in trays:
                if not isinstance(tr, dict):
                    continue
                # tray_color 形如 "DCF478FF"(RRGGBBAA), 取前6位
                raw_color = str(tr.get("tray_color") or "000000FF")
                hex_color = raw_color[:6] if len(raw_color) >= 6 else "000000"
                total_len = tr.get("total_len")
                rem_len = tr.get("remain")
                # remain = -1 表示未称重/未知; 优先用 remain, 否则标 None
                if isinstance(rem_len, (int, float)) and rem_len >= 0:
                    rem_disp = int(rem_len)
                else:
                    rem_disp = None
                total_disp = int(total_len) if isinstance(total_len, (int, float)) else None
                # 当前在用料盘: tray_now 是十进制字符串(如 "3" 表示第4槽), tray.id 是 "0".."3"
                is_now = (tray_now is not None and str(tr.get("id")) == str(int(tray_now) % 4)
                          and str(unit_id) == str(int(tray_now) // 4))
                ams_list.append({
                    "槽位": f"{int(unit_id)+1}-{int(tr.get('id', 0))+1}",
                    "颜色": "#" + hex_color,
                    "类型": tr.get("tray_type") or "未知",
                    "剩余米": round(rem_disp / 1000, 1) if rem_disp is not None else None,
                    "总长米": round(total_disp / 1000, 1) if total_disp is not None else None,
                    "当前": bool(is_now),
                })
    # 明确状态：报文带 gcode_state 且能识别
    if state_raw in STATE_MAP:
        state = STATE_MAP[state_raw]
    elif state_raw:
        state = "在线"
    else:
        # gcode_state 取空（打印机增量心跳只发温度/进度，不带状态）
        # 此时不能直接兜底成"在线"——需要结合上一状态做保持（见 merge_state）
        if gcode or (isinstance(progress, (int, float)) and progress > 0):
            state = "打印中"          # 有文件名或有进度，基本可判定在打印
        else:
            state = None             # 无法判定，交由 merge_state 保持上一状态
    # 错误优先：只要本次报文明确带错误码(非0)，就判定为"出错"（即便 gcode_state 是 PAUSED/RUNNING）。
    # 注意：仅当 err_code 非 None(明确非0)才覆盖；err_explicit_zero(显式带0)或 None(未携带)
    # 都不在此处改状态，交由 merge_state 处理粘性/清除。
    if err_desc is not None:
        state = "出错"
    return {
        "raw_state": state_raw,
        "状态": state,
        "当前文件": gcode or "",
        # 关键: 字段缺失时返回 None（而非 0），这样 merge_state 才能跳过、保留上一状态的真实遥测值，
        # 否则增量心跳（只带温度不带 mc_percent）会把进度/剩余清零。
        "进度": int(progress) if isinstance(progress, (int, float)) else None,
        "剩余分钟": int(remain) if isinstance(remain, (int, float)) else None,
        "喷嘴温度": int(temp) if isinstance(temp, (int, float)) else None,
        "ams": ams_list,
        "预计完成": finish_est_display(int(remain)) if isinstance(remain, (int, float)) and remain > 0 else None,
        "错误": err_desc,            # 出错原因中文(无错误为 None)
        "错误码": err_code,          # 原始整数码: 非0=出错, 0=显式清除, None=未携带
        "错误已清除": err_explicit_zero,  # True=报文显式带 print_error=0，表示已恢复
    }


# 这些状态属于"明确结论"，一旦进入就不会被模糊增量心跳降级
LOCKED_STATES = {"打印中", "已完成", "失败", "暂停", "空闲", "离线", "未连接", "未配置", "出错"}


def merge_state(prev, cur):
    """把增量报文解析结果合并进上一状态，保持状态粘性。

    - cur["状态"] 明确（非 None）→ 直接采用（除了"在线"不能覆盖已锁定的打印状态）。
    - cur["状态"] 为 None（增量心跳无状态字段）→ 沿用 prev 的状态，仅更新遥测。
    - 只有收到明确的 OFFLINE / FINISH / FAILED / IDLE / 连接断开 才允许改变打印中状态。
    """
    merged = dict(prev) if prev else {}
    cur_state = cur.get("状态")
    prev_state = merged.get("state")
    if cur_state is not None:
        # 关键：已处于"打印中/已完成/失败/暂停"等锁定的状态，除非新状态是更明确的结论，
        # 否则不被模糊的"在线"覆盖。但 RUNNING 再次确认、或 FINISH/FAILED/PAUSED/IDLE 都可切换。
        if prev_state in LOCKED_STATES and cur_state == "在线":
            # 已有明确状态，模糊增量不应降级为在线；仅更新遥测
            pass
        elif prev_state == "出错" and cur_state != "出错":
            # 处于"出错"状态时，除非收到"显式清除"(print_error=0)带来的明确恢复状态，
            # 否则不允许被模糊增量心跳(只带进度/温度)降级回打印中。这样前盖脱落的错误
            # 不会在后续心跳里悄悄消失，直到打印机真正发出 print_error=0 的恢复报文。
            # cur["错误已清除"] 为 True 时，下面会清空错误并允许状态切换。
            if cur.get("错误已清除"):
                merged["state"] = cur_state
            # 否则保持"出错"不变，仅更新遥测字段
        else:
            merged["state"] = cur_state
    # 其余遥测字段：增量里提供了就用新的，没提供就保留旧的
    if cur.get("当前文件"):
        merged["file"] = cur["当前文件"]
    if cur.get("进度") is not None:
        merged["percent"] = cur["进度"]
    if cur.get("剩余分钟") is not None:
        merged["remain"] = cur["剩余分钟"]
        # 剩余时间变化 -> 重算预计完成(北京时间)
        merged["finish_est"] = finish_est_display(cur["剩余分钟"])
    if cur.get("喷嘴温度") is not None:
        merged["temp"] = cur["喷嘴温度"]
    # AMS 料盘：增量心跳通常不含 ams，缺失时保留上一状态；有则更新
    if cur.get("ams"):
        merged["ams"] = cur["ams"]
    # 错误字段粘性处理（三态）：
    #   cur["错误码"] 非0            -> 明确出错，更新 error/error_code
    #   cur["错误已清除"] 为 True     -> 打印机显式 print_error=0，清空错误(恢复)
    #   cur 未携带错误字段(都为 None) -> 保持上一状态(粘性)，不清空也不更新
    if cur.get("错误码") is not None and cur.get("错误码") != 0:
        merged["error"] = cur.get("错误")
        merged["error_code"] = cur.get("错误码")
    elif cur.get("错误已清除"):
        # 显式清除：错误归于无，状态由上面的 state 切换逻辑接管
        merged["error"] = None
        merged["error_code"] = None
    # 其余情况(None/未携带)保持上一状态，不清空
    # ---- 打印开始时间追踪(用于"即将完成"提醒防抖) ----
    # 进入"打印中"且上一状态不是"打印中" -> 记录开始时间；
    # 仍在"打印中" -> 沿用已有开始时间；离开"打印中" -> 清空。
    final_state = merged.get("state")
    if final_state == "打印中":
        if prev_state != "打印中":
            merged["print_start_ts"] = time.time()
        # 否则保留 merged 中已有的 print_start_ts(来自 prev)
    else:
        merged["print_start_ts"] = None
    return merged


# ---------------- 文件进度 JSON 持久化 ----------------
def load_jobs():
    global jobs
    DATA_DIR.mkdir(exist_ok=True)
    if FILES_PATH.exists():
        try:
            with FILES_PATH.open(encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                jobs = data
                # 兼容迁移：已下线/废弃的"已打印"状态统一归并为"待摆盘填表"，
                # 避免旧数据因状态不在 STATUSES 中而从看板消失。
                migrated = 0
                for j in jobs:
                    if isinstance(j, dict) and j.get("status") == "已打印":
                        j["status"] = "待摆盘填表"
                        migrated += 1
                if migrated:
                    log(f"【迁移】{migrated} 条旧\"已打印\"任务已归并为\"待摆盘填表\"")
                    save_jobs()
        except Exception as e:
            log(f"【警告】读取 {FILES_PATH.name} 失败: {e}，将以空数据启动")
            jobs = []
    else:
        jobs = []


# 写盘前保留的历史版本数量（防止误删/覆盖导致永久丢失，可一键回退）
BACKUP_KEEP = 3


def save_jobs():
    DATA_DIR.mkdir(exist_ok=True)
    # 历史备份：保存前把当前 files.json 轮转为 files.json.bak1~bakN，
    # 这样即使发生误删/覆盖（如旧版覆盖式 reorder），也能回退到删前状态。
    if FILES_PATH.exists():
        try:
            for i in range(BACKUP_KEEP - 1, 0, -1):
                src = FILES_PATH.with_name("files.json.bak%d" % i)
                if src.exists():
                    src.replace(FILES_PATH.with_name("files.json.bak%d" % (i + 1)))
            FILES_PATH.replace(FILES_PATH.with_name("files.json.bak1"))
        except Exception:
            pass
    tmp = FILES_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    os.replace(tmp, FILES_PATH)


def reorder_jobs(ordered_ids):
    """按前端拖拽后的 id 顺序重排 jobs 列表并持久化。
    仅接受已知 id，列表外的 job 一律原样保留（追加到末尾），绝不删除——防止拖拽误删文件。
    """
    if not isinstance(ordered_ids, list) or len(ordered_ids) == 0:
        return False
    with jobs_lock:
        by_id = {j.get("id"): j for j in jobs}
        new_list = []
        seen = set()
        for jid in ordered_ids:
            j = by_id.get(jid)
            if j is not None and jid not in seen:
                new_list.append(j)
                seen.add(jid)
        # 补齐未在 ordered_ids 中出现的（极端情况下）
        for j in jobs:
            if j.get("id") not in seen:
                new_list.append(j)
        jobs[:] = new_list
        save_jobs()
    return True


# ---------------- 企业微信推送 ----------------
import urllib.request

WEBHOOK_URL = ""   # 由 config.json 的 webhook_url 填充（main 启动时）


def qywechat_push(text):
    """向企业微信群机器人 webhook 推送一条 markdown 消息。失败仅记录，不抛异常。"""
    if not WEBHOOK_URL:
        return
    payload = json.dumps({
        "msgtype": "markdown",
        "markdown": {"content": text}
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", "ignore")
            try:
                rc = json.loads(body).get("errcode")
            except Exception:
                rc = None
            if rc not in (0, None):
                log(f"【企业微信推送】返回异常: {body}")
            else:
                log("【企业微信推送】已发送")
    except Exception as e:
        log(f"【企业微信推送】失败: {e}")


def first_free_machine(joblist):
    used = {}
    for j in joblist:
        if j.get("status") == "打印中" and j.get("machine"):
            used[j["machine"]] = True
    for m in MACHINES:
        if m not in used:
            return m
    return ""


def new_id():
    return "j" + str(int(time.time() * 1000)) + str(int(time.time() % 1 * 1000))


def create_job(body):
    job = {
        "id": new_id(),
        "name": str(body.get("name", "") or ""),
        "status": body.get("status") or "待打印",
        "machine": body.get("machine") or "",
        "qty": int(body.get("qty") or 0),
        "note": body.get("note") or "",
        "addedAt": body.get("addedAt") or today_iso(),
    }
    if job["status"] not in VALID_STATUS:
        job["status"] = "待打印"
    with jobs_lock:
        jobs.append(job)
        save_jobs()
    return job


def update_job(jid, body):
    with jobs_lock:
        for j in jobs:
            if j["id"] == jid:
                if "name" in body:
                    j["name"] = str(body["name"] or "")
                if "status" in body:
                    j["status"] = body["status"] if body["status"] in VALID_STATUS else j["status"]
                if "machine" in body:
                    j["machine"] = body["machine"] or ""
                if "qty" in body:
                    j["qty"] = int(body["qty"] or 0)
                if "note" in body:
                    j["note"] = body["note"] or ""
                save_jobs()
                return dict(j)
        return None


def delete_job(jid):
    with jobs_lock:
        before = len(jobs)
        jobs[:] = [j for j in jobs if j["id"] != jid]
        if len(jobs) != before:
            save_jobs()
            return True
        return False


def clear_jobs():
    with jobs_lock:
        jobs[:] = []
        save_jobs()


def advance_job(jid):
    with jobs_lock:
        for j in jobs:
            if j["id"] == jid:
                ns = NEXT.get(j["status"])
                if ns:
                    j["status"] = ns
                    if ns == "打印中" and not j.get("machine"):
                        j["machine"] = first_free_machine(jobs)
                    save_jobs()
                    return dict(j)
                return dict(j)
        return None


# ---------------- 打印机 MQTT ----------------
class Printer:
    def __init__(self, cfg_item):
        self.name = cfg_item.get("name", "?")
        self.ip = cfg_item.get("ip", "")
        self.serial = (cfg_item.get("serial") or "").upper()
        self.access_code = cfg_item.get("access_code", "")
        self.last_msg_ts = 0.0
        self.last_pushall_ts = 0.0
        self.connected = False
        self.reconnect_at = 0.0
        self.reconnect_gap = RECONNECT_BASE
        self.notify_done = False   # 已完成推送去重
        self.notify_10m = False    # 完成前10分钟推送去重
        self.notify_error = False  # 出错告警推送去重
        self.client = self._build()
        with state_lock:
            machine_state[self.name] = {
                "state": "未连接", "file": "", "percent": 0,
                "remain": 0, "temp": 0, "updated": "", "connected": False,
                "finish_est": None, "error": None, "error_code": None,
            }

    def _build(self):
        cid = f"sync-{self.serial or self.name}"
        if PAHO_V2:
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid,
                            clean_session=True, protocol=mqtt.MQTTv311)
        else:
            c = mqtt.Client(client_id=cid, clean_session=True, protocol=mqtt.MQTTv311)
        c.username_pw_set("bblp", self.access_code)
        c.tls_set(cert_reqs=ssl.CERT_NONE)
        c.tls_insecure_set(True)
        c.on_connect = self._on_connect
        c.on_message = self._on_message
        c.on_disconnect = self._on_disconnect
        return c

    def _topic(self):
        return f"device/{self.serial}/report"

    def _req_topic(self):
        return f"device/{self.serial}/request"

    def _pushall(self):
        try:
            self.client.publish(self._req_topic(),
                                json.dumps({"pushing": {"command": "pushall"}}), qos=1)
        except Exception:
            pass

    def _on_connect(self, *args):
        self.connected = True
        self.reconnect_gap = RECONNECT_BASE
        self.client.subscribe(self._topic())
        self._pushall()
        log(f"{self.name}: MQTT 已连接 ({self.ip})")

    def _on_message(self, client, userdata, msg):
        st = extract_state(msg.payload)
        if not st:
            return
        with state_lock:
            prev = machine_state.get(self.name)
            # 首次收到：prev 为空则用解析结果直接建；否则合并保持状态粘性
            if prev is None:
                merged = {
                    "state": st["状态"] or "在线",
                    "file": st["当前文件"], "percent": st["进度"],
                    "remain": st["剩余分钟"], "temp": st["喷嘴温度"],
                    "updated": now_iso(), "connected": True,
                    "finish_est": st.get("预计完成"),
                    "error": st.get("错误"), "error_code": st.get("错误码"),
                    # 记录打印开始时间(仅"打印中"时有效)，用于"即将完成"提醒防抖
                    "print_start_ts": time.time() if (st["状态"] == "打印中") else None,
                }
            else:
                merged = merge_state(prev, st)
                merged["updated"] = now_iso()
                merged["connected"] = True
            machine_state[self.name] = merged
        # 推送检测（企业微信）：在锁外执行，避免阻塞 MQTT 线程
        self._notify_check(merged)
        self.last_msg_ts = time.time()

    def _notify_check(self, st):
        """根据当前状态/进度触发企业微信推送（带去重）：
           - 进入"已完成" -> 推送一次完成通知
           - 剩余时间 <= 10 分钟且仍在打印 -> 推送一次"即将完成"提醒
        """
        if not WEBHOOK_URL:
            return
        state = st.get("state")
        remain = st.get("remain")
        percent = st.get("percent")
        fname = st.get("file") or "（未知文件）"
        finish = st.get("finish_est") or "未知"

        # 1) 已完成推送
        if state == "已完成":
            if not self.notify_done:
                self.notify_done = True
                qywechat_push(
                    f"## ✅ 打印完成通知\n"
                    f"> **机器**：{self.name}\n"
                    f"> **文件**：{fname}\n"
                    f"> **完成时间**：{now_display()}（北京时间）\n"
                    f"> **最终进度**：{percent if isinstance(percent, int) else 100}%\n\n"
                    f"> 该任务已打印完成，请前往取件。"
                )
        else:
            # 非已完成状态，重置标记（下一轮打印可再次提醒）
            self.notify_done = False

        # 2) 完成前 10 分钟提醒（仅"打印中"且剩余 0<remain<=10，且已实际运行 >= REMIND_GRACE_MIN 分钟）
        #    加 grace 防抖：刚下发/刚启动的报文剩余时间常是脏值(如误报1分钟)，
        #    必须打印真正跑了一段时间后才信任"临近完成"读数，避免误推。
        run_sec = (time.time() - (st.get("print_start_ts") or 0)) if st.get("print_start_ts") else 0
        if (state == "打印中" and isinstance(remain, int) and 0 < remain <= 10
                and run_sec >= REMIND_GRACE_MIN * 60):
            if not self.notify_10m:
                self.notify_10m = True
                qywechat_push(
                    f"## ⏰ 打印即将完成提醒\n"
                    f"> **机器**：{self.name}\n"
                    f"> **文件**：{fname}\n"
                    f"> **当前进度**：{percent if isinstance(percent, int) else '?'}%\n"
                    f"> **预计完成**：{finish}（北京时间）\n"
                    f"> **剩余时间**：约 {remain} 分钟\n\n"
                    f"> 请留意，10 分钟内将完成。"
                )
        else:
            # 剩余 > 10 或非打印中，重置标记（下次进入窗口可再提醒）
            self.notify_10m = False

        # 3) 出错告警：状态为"出错"且本次有明确错误原因 -> 推送一次（去重）
        err = st.get("error")
        if state == "出错" and err:
            if not self.notify_error:
                self.notify_error = True
                qywechat_push(
                    f"## ⚠️ 打印出错告警\n"
                    f"> **机器**：{self.name}\n"
                    f"> **文件**：{fname}\n"
                    f"> **错误原因**：{err}\n"
                    f"> **发现时间**：{now_display()}（北京时间）\n"
                    f"> **当前进度**：{percent if isinstance(percent, int) else '?'}%\n\n"
                    f"> 打印机已自动暂停，请尽快检查处理（如前盖是否脱落、是否断料等）。"
                )
        else:
            # 非出错状态，重置标记（下次再出错可再提醒）
            self.notify_error = False

    def _on_disconnect(self, *args):
        self.connected = False
        # paho v2: on_disconnect(client, userdata, disconnect_flags, reason_code, properties)
        # reason_code != 0 表示非正常断开(常被其他客户端踢掉); v1 则只有 (client, userdata, rc)
        rc = None
        if len(args) >= 4:
            rc = args[3]
        elif len(args) >= 3:
            rc = args[2]
        with state_lock:
            if self.name in machine_state:
                machine_state[self.name]["connected"] = False
        if rc not in (0, None):
            rc_val = getattr(rc, "value", rc)
            # MQTT 原因码 0x8E/142 = "会话被接管"(另一客户端以同 client_id 接入会把旧的踢掉)
            if rc_val in (0x8E, 142, "session taken over"):
                log(f"{self.name}: MQTT 被踢下线(原因码 {rc_val}) —— 可能有其他客户端接入占用了此机，请检查是否开了重复进程/Bambu Studio")
            else:
                log(f"{self.name}: MQTT 异常断开(原因码 {rc_val})，将自动重连")
        else:
            log(f"{self.name}: MQTT 断开")

    def connect(self, now):
        if now < self.reconnect_at:
            return
        if not (self.ip and self.serial and self.access_code):
            with state_lock:
                if self.name in machine_state:
                    machine_state[self.name]["state"] = "未配置"
            return
        try:
            self.client.connect(self.ip, 8883, keepalive=15)
            self.client.loop_start()
        except Exception as e:
            msg = str(e)
            # 拓竹每台打印机只允许 1 个 MQTT 客户端。若被远程 reset/拒绝，
            # 几乎可以肯定是别的客户端(旧的 printsync 进程 / Bambu Studio / Handy App / 测试脚本)
            # 正占用这台机的连接，应明确提示而不是笼统报 timed out。
            if "ConnectionReset" in msg or "reset" in msg.lower() or "10054" in msg:
                log(f"{self.name}: 连接被打印机拒绝({msg}) —— 可能已有其他客户端(旧进程/Bambu Studio/手机App/测试脚本)占用此机连接，请先关闭它们再重启本程序")
            else:
                log(f"{self.name}: 连接失败 {e}")
            self.reconnect_gap = min(self.reconnect_gap * 2, 60)
            self.reconnect_at = time.time() + self.reconnect_gap

    def tick(self, now):
        # 周期性重新请求完整状态（空闲机可能只发心跳）
        if self.connected and (now - self.last_pushall_ts) > PUSHALL_INTERVAL:
            self._pushall()
            self.last_pushall_ts = now
        # 掉线判定
        if self.last_msg_ts and (now - self.last_msg_ts) > OFFLINE_AFTER:
            with state_lock:
                if self.name in machine_state:
                    machine_state[self.name]["state"] = "未连接"
                    machine_state[self.name]["connected"] = False
            self.last_msg_ts = now  # 防止刷屏


def printer_loop(printers):
    try:
        while True:
            now = time.time()
            for p in printers:
                if not p.connected:
                    p.connect(now)
                p.tick(now)
            time.sleep(5)
    except KeyboardInterrupt:
        pass


# ---------------- HTTP 服务 ----------------
class Handler(BaseHTTPRequestHandler):
    server_version = "PrintSync/1.0"

    def log_message(self, fmt, *args):
        return  # 安静

    def _send(self, code, body, content_type):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False), "application/json; charset=utf-8")

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            return {}

    def client_ip(self):
        # 优先级：X-Real-IP (Nginx $remote_addr 透传, 不可伪造) → X-Forwarded-For → 直连地址
        real = self.headers.get("X-Real-IP")
        if real:
            return real.strip()
        xff = self.headers.get("X-Forwarded-For") or ""
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0]

    def is_editor(self):
        return self.client_ip() in ALLOWED_IPS

    def _forbidden(self):
        return self._json({"error": "只读模式：仅 %s 可操作，当前 %s 只能查看" % (",".join(ALLOWED_IPS), self.client_ip())}, 403)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path in ("/", "/index.html"):
            return self.serve_dashboard()
        if path == "/api/data":
            with state_lock:
                ms = {k: dict(v) for k, v in machine_state.items()}
            with jobs_lock:
                js = list(jobs)
            return self._json({"machines": ms, "jobs": js, "serverTime": now_iso(), "canEdit": self.is_editor()})
        if path == "/api/jobs":
            with jobs_lock:
                return self._json(list(jobs))
        if path == "/api/machines":
            with state_lock:
                return self._json({k: dict(v) for k, v in machine_state.items()})
        if path == "/api/ping":
            return self._json({"ok": True})
        self._json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_body()
        if not self.is_editor():
            return self._forbidden()
        if path == "/api/jobs":
            if not str(body.get("name", "")).strip():
                return self._json({"error": "name required"}, 400)
            job = create_job(body)
            return self._json(job, 201)
        m = re.match(r"^/api/jobs/([^/]+)/next$", path)
        if m:
            job = advance_job(urllib.parse.unquote(m.group(1)))
            return self._json(job, 200) if job else self._json({"error": "not found"}, 404)
        if path == "/api/jobs/reorder":
            ok = reorder_jobs(body.get("ordered_ids") or [])
            return self._json({"ok": True}, 200) if ok else self._json({"error": "invalid order"}, 400)
        self._json({"error": "not found"}, 404)

    def do_PUT(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if not self.is_editor():
            return self._forbidden()
        m = re.match(r"^/api/jobs/([^/]+)$", path)
        if m:
            job = update_job(urllib.parse.unquote(m.group(1)), self._read_body())
            return self._json(job, 200) if job else self._json({"error": "not found"}, 404)
        self._json({"error": "not found"}, 404)

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if not self.is_editor():
            return self._forbidden()
        if path == "/api/jobs":
            clear_jobs()
            return self._json({"ok": True})
        m = re.match(r"^/api/jobs/([^/]+)$", path)
        if m:
            ok = delete_job(urllib.parse.unquote(m.group(1)))
            return self._json({"ok": True}, 200) if ok else self._json({"error": "not found"}, 404)
        self._json({"error": "not found"}, 404)

    def serve_dashboard(self):
        p = ROOT / "dashboard.html"
        try:
            data = p.read_bytes()
        except Exception:
            return self._send(500, b"dashboard.html missing", "text/plain; charset=utf-8")
        self._send(200, data, "text/html; charset=utf-8")


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def main():
    global CONFIG_PATH, MACHINES
    ap = argparse.ArgumentParser(description="3D 打印本地工作台")
    ap.add_argument("--config", default=str(CONFIG_PATH), help="配置文件路径")
    ap.add_argument("--port", type=int, default=None, help="覆盖监听端口")
    args = ap.parse_args()

    # 启动即初始化日志文件（建立 logs 目录 + 当天日志句柄）
    ensure_log_file()

    cp = Path(args.config)
    if not cp.exists():
        sys.stderr.write(f"【错误】找不到配置文件 {cp}\n")
        sys.exit(1)
    c = json.loads(cp.read_text(encoding="utf-8"))
    cfg["host"] = c.get("host", "0.0.0.0")
    cfg["port"] = args.port or c.get("port", 8000)
    allowed = c.get("allowed_ips")
    if isinstance(allowed, list) and allowed:
        global ALLOWED_IPS
        ALLOWED_IPS = [str(x).strip() for x in allowed]
        log("操作白名单已覆盖为: " + ", ".join(ALLOWED_IPS))
    global WEBHOOK_URL
    WEBHOOK_URL = (c.get("webhook_url") or "").strip()
    if WEBHOOK_URL:
        log("企业微信推送已启用")
    else:
        log("未配置 webhook_url，企业微信推送关闭")
    machines = c.get("machines") or []

    load_jobs()
    MACHINES = [m.get("name", f"机器{i+1}") for i, m in enumerate(machines)]

    printers = [Printer(m) for m in machines]
    log(f"启动本地工作台，监听 {len(printers)} 台打印机 …")

    t = threading.Thread(target=printer_loop, args=(printers,), daemon=True)
    t.start()

    server = ThreadingHTTPServer((cfg["host"], cfg["port"]), Handler)
    ip = lan_ip()
    log("=" * 48)
    log(f"仪表盘已启动！在本机或同局域网设备浏览器打开:")
    log(f"  本机:    http://127.0.0.1:{cfg['port']}")
    log(f"  局域网:  http://{ip}:{cfg['port']}")
    log("=" * 48)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("收到中断，退出")
        server.shutdown()
    finally:
        global _log_fh
        if _log_fh is not None:
            try:
                _log_fh.close()
            except Exception:
                pass
            _log_fh = None


if __name__ == "__main__":
    main()
