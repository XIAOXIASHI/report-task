import requests
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import datetime
import json
import os
import logging
import pdfkit
from dotenv import load_dotenv


# 加载环境变量
load_dotenv()


class DataFetcher:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.api_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        self.api_params = {
            "sortColumns": "SCGGRQ",
            "sortTypes": -1,
            "pageSize": 50,
            "pageNumber": 1,
            "columns": "ALL",
            "source": "WEB",
            "token": "894050c76af8597a853f5b408b759f5d",
            "reportName": "RPTA_WEB_BGCZMX"
        }

    def fetch(self):
        try:
            response = requests.get(self.api_url, params=self.api_params, timeout=10)
            response.raise_for_status()
            return response.json()['result']['data']
        except Exception as e:
            self.logger.error(f"数据抓取失败: {str(e)}")
            return []


class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.today = datetime.datetime.now().strftime('%Y-%m-%d')
        self.field_mapping = {
            'SCODE': '股票代码',
            'SNAME': '股票简称',
            'OBJTYPE': '相关',
            'H_COMNAME': '交易标的',
            'G_GOMNAME': '卖方',
            'S_COMNAME': '买方',
            'JYJE': '交易金额 (万)',
            'BZNAME': '币种',
            'ZRFS': '并购方式',
            'ANNOUNDATE': '最新公告日'
        }
        self.gd_keywords = ['广东', '粤', '广州', '深圳', '珠海', '佛山', '东莞', '中山', '惠州',
                            '江门', '肇庆', '汕头', '潮州', '揭阳', '汕尾', '韶关', '清远',
                            '梅州', '河源', '阳江', '茂名', '湛江','岭南']
        self.guangdong_cases = []

    def is_guangdong_company(self, company_name):
        if company_name == '-':
            return False
        for kw in self.gd_keywords:
            if kw in company_name:
                return True
        return False

    def process(self, raw_data):
        if not raw_data:
            return []

        processed_data = []
        serial_number = 1
        for item in raw_data:
            item_date = item.get('SCGGRQ', '')[:10]
            if item_date != self.today:
                continue

            row = {'序号': serial_number}
            serial_number += 1
            stock_code = item.get('SCODE', '')

            row['交易金额 (万)'] = '-'  # 确保交易金额列存在
            
            for api_field, report_field in self.field_mapping.items():
                value = item.get(api_field, '-')

                if value is None or str(value).strip() == '' or str(value).lower() == 'none':
                    value = '-'

                if api_field == 'JYJE':
                    try:
                        if value != '-':
                            original_amount = float(value)
                            row[report_field] = f"{original_amount:,.2f}"
                    except:
                        row[report_field] = "-"
                elif api_field == 'OBJTYPE':
                    if stock_code and value != '-':
                        notice_url = f"https://data.eastmoney.com/notices/stock/{stock_code}.html"
                        detail_url = f"https://data.eastmoney.com/bgcz/detail/{stock_code}.html"
                        # 生成两种格式的链接
                        # 1. 带分隔符的链接（用于第二部分案例展示）
                        row['相关_带分隔符'] = f'<a href="{notice_url}" target="_blank">公告</a> | <a href="{detail_url}" target="_blank">详细</a>'
                        # 2. 带换行的链接（用于第三部分列表展示）
                        row['相关_换行'] = f'<a href="{notice_url}" target="_blank">公告</a><br><a href="{detail_url}" target="_blank">详细</a>'
                    else:
                        row['相关_带分隔符'] = "-"
                        row['相关_换行'] = "-"
                elif api_field in ['ANNOUNDATE']:
                    row[report_field] = value[:10] if value != '-' else "-"
                else:
                    row[report_field] = value

            processed_data.append(row)

            # 检查广东案例，使用带分隔符的链接
            seller = row.get('卖方', '')
            buyer = row.get('买方', '')
            if self.is_guangdong_company(seller) or self.is_guangdong_company(buyer):
                # 创建包含带分隔符链接的案例数据
                case_data = row.copy()
                case_data['相关'] = case_data['相关_带分隔符']  # 替换为带分隔符的链接
                self.guangdong_cases.append(case_data)

        return processed_data

    def analyze(self, processed_data):
        if not processed_data:
            return {
                'overview': '今日无并购重组数据公告',
                'basic_overview': '今日无并购重组数据公告',
                'cases': [],
                'guangdong_cases': [],
                'guangdong_count': 0,
                'total_count': 0,
                'method_distribution': {}
            }

        df = pd.DataFrame(processed_data)
        total_count = len(df)
        method_distribution = df['并购方式'].value_counts().to_dict()

        return {
            'overview': f"今日共获取{total_count}条并购重组数据",
            'basic_overview': f"今日共获取{total_count}条并购重组数据",
            'cases': [],
            'guangdong_cases': self.guangdong_cases,  # 已包含带分隔符链接的案例数据
            'guangdong_count': len(self.guangdong_cases),
            'total_count': total_count,
            'method_distribution': method_distribution
        }


class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.template_dir = '.'
        self.env = Environment(loader=FileSystemLoader(self.template_dir))

    def generate_html(self, data, analysis, date_str):
        if not data:
            return None

        html_filename = f"并购重组日报_{date_str}.html"
        generate_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        try:
            template = self.env.get_template('report_template.html')
            html_content = template.render(
                title="并购重组日报",
                data=data,
                analysis=analysis,
                date=current_date,
                generate_time=generate_time
            )

            with open(html_filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"HTML报告生成成功: {html_filename}")
            return html_filename

        except Exception as e:
            self.logger.error(f"生成HTML报告失败: {str(e)}")
            return None

    def generate_pdf(self, html_path):
        if not html_path or not os.path.exists(html_path):
            self.logger.error(f"HTML文件不存在: {html_path}")
            return None

        pdf_filename = os.path.splitext(html_path)[0] + '.pdf'

        try:
            options = {
                'page-size': 'A4',
                'orientation': 'Landscape',
                'margin-top': '0.3in',
                'margin-right': '0.3in',
                'margin-bottom': '0.3in',
                'margin-left': '0.3in',
                'encoding': "UTF-8",
                'no-outline': None,
                'quiet': '',
                'enable-local-file-access': None,
                'disable-smart-shrinking': None,
                'footer-right': '[page]/[topage]'
            }

            pdfkit.from_file(html_path, pdf_filename, options=options)
            self.logger.info(f"PDF报告生成成功: {pdf_filename}")
            return pdf_filename

        except Exception as e:
            self.logger.error(f"HTML转PDF失败: {str(e)}")
            return None

    def generate(self, data, analysis, date_str):
        html_path = self.generate_html(data, analysis, date_str)
        if not html_path:
            return []

        pdf_path = self.generate_pdf(html_path)
        if not pdf_path:
            return [html_path]

        return [html_path, pdf_path]


class WechatSender:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)

    def send_file(self, file_path):
        if not os.path.exists(file_path):
            self.logger.error(f"文件不存在: {file_path}")
            return False

        try:
            upload_url = self.webhook_url.replace('/send', '/upload_media')
            files = {'media': open(file_path, 'rb')}
            params = {'type': 'file'}

            upload_response = requests.post(upload_url, params=params, files=files)
            upload_result = upload_response.json()

            if upload_result.get('errcode') != 0:
                self.logger.error(f"文件上传失败: {upload_result.get('errmsg')}")
                return False

            media_id = upload_result.get('media_id')
            send_data = {"msgtype": "file", "file": {"media_id": media_id}}
            send_response = requests.post(self.webhook_url, json=send_data)
            send_result = send_response.json()

            if send_result.get('errcode') != 0:
                self.logger.error(f"文件发送失败: {send_result.get('errmsg')}")
                return False

            self.logger.info(f"文件发送成功: {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"发送文件出错: {str(e)}")
            return False

    def send_status(self, status, message):
        try:
            send_data = {
                "msgtype": "text",
                "text": {
                    "content": f"【并购重组报告定时任务】\n状态: {status}\n时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n详情: {message}"
                }
            }

            send_response = requests.post(self.webhook_url, json=send_data)
            send_result = send_response.json()

            if send_result.get('errcode') != 0:
                self.logger.error(f"状态通知发送失败: {send_result.get('errmsg')}")
                return False

            self.logger.info("状态通知发送成功")
            return True

        except Exception as e:
            self.logger.error(f"发送状态通知出错: {str(e)}")
            return False


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=591c96ad-d26f-475d-935e-1e838061b6f7"

    class Config:
        def get_log_level(self):
            return logging.INFO

    config = Config()
    sender = WechatSender(WEBHOOK_URL)

    try:
        logger.info("===== 立即执行今日定时任务：生成并发送并购重组报告 =====\n")

        # 1. 抓取数据
        fetcher = DataFetcher(config)
        raw_data = fetcher.fetch()

        if not raw_data:
            sender.send_status("成功", "未获取到原始数据，任务正常结束")
            return

        # 2. 处理数据
        processor = DataProcessor(config)
        processed_data = processor.process(raw_data)

        if not processed_data:
            sender.send_status("成功", "今日无相关数据，任务正常结束")
            return

        # 3. 分析数据
        analysis = processor.analyze(processed_data)

        # 4. 生成报告
        generator = ReportGenerator(config)
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        report_paths = generator.generate(processed_data, analysis, today_str)

        if not report_paths:
            sender.send_status("失败", "报告生成失败")
            return

        # 5. 发送所有文件
        for path in report_paths:
            if not sender.send_file(path):
                sender.send_status("失败", f"文件发送失败: {path}")
                return

        sender.send_status("成功", f"报告生成并发送成功，共发送{len(report_paths)}个文件")
        logger.info("===== 今日定时任务执行完成 =====\n")
        return report_paths

    except Exception as e:
        sender.send_status("失败", f"任务执行出错: {str(e)}")
        logger.error(f"工具运行出错: {str(e)}", exc_info=True)
        return []


if __name__ == "__main__":
    main()
