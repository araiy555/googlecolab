#!/usr/bin/env python3
"""
株探データを銘柄別に整理するシステム（日次版）
毎日当月と前月のデータのみを処理・更新
"""

import json
import boto3
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import logging
import re
from typing import Dict, List, Any, Optional
import time
import requests

class StockBasedDataOrganizer:
    def __init__(self):
        """銘柄別データ整理システム初期化"""
        # S3設定（環境変数から認証情報を取得）
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if aws_access_key and aws_secret_key:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name="ap-northeast-1"
            )
        else:
            self.s3 = boto3.client('s3', region_name="ap-northeast-1")

        self.bucket_name = "m-s3storage"
        self.source_prefix = "japan-stocks-5years-chart/monthly-disclosures/"
        self.output_prefix = "japan-stocks-5years-chart/stock-based-disclosures/"

        # ログ設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # 統計
        self.stats = {
            'total_disclosures': 0,
            'unique_stocks': 0,
            'processed_months': 0,
            'created_files': 0,
            'errors': 0,
            'start_time': None
        }

    def get_target_months(self):
        """対象月を取得（当月と前月）"""
        today = datetime.now()
        current_year = today.year
        current_month = today.month

        months = []

        # 前月
        if current_month == 1:
            prev_year = current_year - 1
            prev_month = 12
        else:
            prev_year = current_year
            prev_month = current_month - 1

        months.append((prev_year, prev_month))
        months.append((current_year, current_month))

        return months

    def load_target_monthly_data(self, target_months) -> Dict[str, List[Dict]]:
        """対象月のデータのみ読み込み、銘柄別に整理"""
        stock_data = defaultdict(list)

        try:
            self.logger.info(f"対象月: {target_months}")

            for year, month in target_months:
                file_key = f"{self.source_prefix}{year}-{month:02d}.json"
                self.logger.info(f"読み込み: {file_key}")

                try:
                    response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                    file_content = response['Body'].read().decode('utf-8')
                    data = json.loads(file_content)

                    # データ構造の確認と開示情報の抽出
                    disclosures = []

                    if isinstance(data, dict) and 'disclosures' in data:
                        disclosures = data['disclosures']
                    elif isinstance(data, list):
                        disclosures = data
                    elif isinstance(data, dict) and 'stock_code' in data:
                        disclosures = [data]

                    # 銘柄別に分類
                    processed_count = 0
                    for disclosure in disclosures:
                        if not isinstance(disclosure, dict):
                            continue

                        stock_code = disclosure.get('stock_code')
                        if stock_code and re.match(r'^\d{4}$', str(stock_code).strip()):
                            stock_code = str(stock_code).strip()
                            enhanced_disclosure = self.enhance_disclosure_data(disclosure)
                            stock_data[stock_code].append(enhanced_disclosure)
                            processed_count += 1
                            self.stats['total_disclosures'] += 1

                    self.logger.info(f"  処理完了: {processed_count}件")
                    self.stats['processed_months'] += 1

                except Exception as e:
                    self.logger.error(f"ファイル処理エラー: {file_key} - {e}")
                    self.stats['errors'] += 1
                    continue

            self.stats['unique_stocks'] = len(stock_data)
            self.logger.info(f"対象月データ読み込み完了: {len(stock_data)}銘柄")

            return dict(stock_data)

        except Exception as e:
            self.logger.error(f"データ読み込みエラー: {e}")
            return {}

    def enhance_disclosure_data(self, disclosure: Dict) -> Dict:
        """開示データの拡張・強化"""
        enhanced = disclosure.copy()

        stock_code = enhanced.get('stock_code', '').strip()
        enhanced['stock_code'] = stock_code

        company_name = enhanced.get('company_name', '').strip()
        if company_name and company_name not in ['抽出中', '不明', '']:
            enhanced['company_name_cleaned'] = self.normalize_company_name(company_name)
        else:
            enhanced['company_name_cleaned'] = f"銘柄{stock_code}"

        date_str = enhanced.get('date', '')
        if date_str:
            enhanced['date_normalized'] = self.normalize_date(date_str)
            enhanced['year'] = int(enhanced['date_normalized'][:4]) if enhanced['date_normalized'] else None
            enhanced['month'] = int(enhanced['date_normalized'][5:7]) if enhanced['date_normalized'] else None
            enhanced['quarter'] = self.get_quarter(enhanced['month']) if enhanced['month'] else None

        enhanced['category_detailed'] = self.detailed_categorization(
            enhanced.get('title', ''),
            enhanced.get('company_name', '')
        )

        enhanced['importance_score'] = self.calculate_importance_score(enhanced)
        enhanced['disclosure_type'] = self.classify_disclosure_type(enhanced)
        enhanced['processed_at'] = datetime.now().isoformat()

        return enhanced

    def normalize_company_name(self, company_name: str) -> str:
        """会社名の正規化"""
        patterns_to_remove = [
            r'\s*\d{2}/\d{2}/\d{2}\s*\d{2}:\d{2}$',
            r'\s*第\d+期.*$',
            r'\s*Notice.*$',
            r'\s*\d+$',
        ]

        normalized = company_name
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized)

        normalized = normalized.replace('(株)', '').replace('㈱', '')

        return normalized.strip()

    def normalize_date(self, date_str: str) -> str:
        """日付の正規化"""
        if not date_str:
            return ''

        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        patterns = [
            (r'^(\d{4})/(\d{1,2})/(\d{1,2})$', lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
            (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', lambda m: f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"),
            (r'^(\d{4})(\d{2})(\d{2})$', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
        ]

        for pattern, formatter in patterns:
            match = re.match(pattern, date_str)
            if match:
                try:
                    return formatter(match)
                except ValueError:
                    continue

        return date_str

    def get_quarter(self, month: int) -> int:
        """月から四半期を取得"""
        if month in [1, 2, 3]:
            return 1
        elif month in [4, 5, 6]:
            return 2
        elif month in [7, 8, 9]:
            return 3
        else:
            return 4

    def detailed_categorization(self, title: str, company_name: str = '') -> str:
        """詳細カテゴリ分類"""
        text = f"{title} {company_name}".lower()

        detailed_categories = {
            '決算短信': ['決算短信', '四半期決算短信'],
            '決算説明会': ['決算説明会', '決算briefing', '業績説明会'],
            '業績予想修正': ['業績予想', '業績見通し修正', '業績修正'],
            '配当金': ['配当金', '期末配当', '中間配当', '特別配当'],
            '株主優待': ['株主優待', '優待制度'],
            '自己株式取得': ['自己株式取得', '自社株買い'],
            '株式分割': ['株式分割', '株式併合'],
            '新株予約権': ['新株予約権', 'ストックオプション'],
            '第三者割当増資': ['第三者割当', '第三者割当増資'],
            '代表取締役': ['代表取締役', 'CEO', '社長'],
            '役員人事': ['取締役', '監査役', '執行役員'],
            'M&A買収': ['買収', '株式取得', '子会社化'],
            '業務提携': ['業務提携', '資本提携'],
            '新規事業': ['新規事業', '事業参入'],
            '新製品発表': ['新製品', '新商品'],
            '設備投資': ['設備投資', '工場建設'],
            '適時開示訂正': ['訂正', '修正', '取消'],
            '有価証券報告書': ['有価証券報告書', '四半期報告書'],
            '定時株主総会': ['定時株主総会'],
            '臨時株主総会': ['臨時株主総会'],
        }

        for category, keywords in detailed_categories.items():
            if any(keyword in text for keyword in keywords):
                return category

        return 'その他'

    def calculate_importance_score(self, disclosure: Dict) -> float:
        """重要度スコアの計算"""
        score = 0.0
        title = disclosure.get('title', '').lower()
        category = disclosure.get('category_detailed', '')

        high_importance_categories = [
            '決算短信', '業績予想修正', '代表取締役', 'M&A買収', '第三者割当増資'
        ]

        medium_importance_categories = [
            '決算説明会', '配当金', '株式分割', '自己株式取得', '新規事業', '業務提携', '設備投資'
        ]

        if category in high_importance_categories:
            score += 0.5
        elif category in medium_importance_categories:
            score += 0.3
        else:
            score += 0.1

        high_impact_keywords = [
            '業績予想修正', '赤字', '黒字転換', '増配', '減配', '無配', '買収', '合併'
        ]

        for keyword in high_impact_keywords:
            if keyword in title:
                score += 0.2
                break

        if re.search(r'\d+億円|\d+百万円|\d+千万円', title):
            score += 0.1

        if len(disclosure.get('title', '')) > 50:
            score += 0.1

        return min(score, 1.0)

    def classify_disclosure_type(self, disclosure: Dict) -> str:
        """開示タイプの分類"""
        category = disclosure.get('category_detailed', '')
        title = disclosure.get('title', '').lower()

        if any(keyword in title for keyword in ['有価証券報告書', '四半期報告書', '決算短信']):
            return '法定開示'
        elif any(keyword in category for keyword in ['業績予想', 'M&A', '人事', '配当']):
            return '適時開示'
        elif any(keyword in title for keyword in ['説明資料', 'プレゼン', '説明会']):
            return 'IR資料'
        else:
            return 'その他'

    def create_stock_summary(self, stock_code: str, disclosures: List[Dict]) -> Dict:
        """銘柄サマリーの作成"""
        if not disclosures:
            return {}

        total_disclosures = len(disclosures)
        date_range = self.get_date_range(disclosures)

        category_stats = Counter(d.get('category_detailed', 'その他') for d in disclosures)
        yearly_stats = Counter(d.get('year') for d in disclosures if d.get('year'))

        quarterly_stats = defaultdict(lambda: defaultdict(int))
        for d in disclosures:
            year = d.get('year')
            quarter = d.get('quarter')
            if year and quarter:
                quarterly_stats[year][f"Q{quarter}"] += 1

        importance_distribution = {
            'high': len([d for d in disclosures if d.get('importance_score', 0) >= 0.7]),
            'medium': len([d for d in disclosures if 0.4 <= d.get('importance_score', 0) < 0.7]),
            'low': len([d for d in disclosures if d.get('importance_score', 0) < 0.4])
        }

        company_names = [d.get('company_name_cleaned') or d.get('company_name', '')
                        for d in disclosures
                        if d.get('company_name_cleaned') or d.get('company_name', '')]
        company_names = [name for name in company_names if name and name != f"銘柄{stock_code}"]

        if company_names:
            name_counter = Counter(company_names)
            most_common_name = name_counter.most_common(1)[0][0]
        else:
            most_common_name = f"銘柄{stock_code}"

        company_info = {
            'stock_code': stock_code,
            'company_name': most_common_name,
            'company_name_variations': list(set(company_names[:5])),
            'data_source': '開示データから抽出'
        }

        major_disclosures = sorted(
            [d for d in disclosures if d.get('importance_score', 0) >= 0.6],
            key=lambda x: (x.get('date_normalized', ''), x.get('importance_score', 0)),
            reverse=True
        )[:20]

        return {
            'company_info': company_info,
            'summary_stats': {
                'total_disclosures': total_disclosures,
                'date_range': date_range,
                'analysis_period_years': len(yearly_stats),
                'average_disclosures_per_year': total_disclosures / max(len(yearly_stats), 1)
            },
            'category_distribution': dict(category_stats.most_common()),
            'yearly_trend': dict(yearly_stats),
            'quarterly_trend': dict(quarterly_stats),
            'importance_distribution': importance_distribution,
            'major_disclosures': major_disclosures
        }

    def get_date_range(self, disclosures: List[Dict]) -> Dict[str, str]:
        """日付範囲の取得"""
        dates = [d.get('date_normalized') for d in disclosures if d.get('date_normalized')]
        dates = [d for d in dates if d and re.match(r'^\d{4}-\d{2}-\d{2}$', d)]

        if dates:
            return {
                'start_date': min(dates),
                'end_date': max(dates)
            }
        return {'start_date': '', 'end_date': ''}

    def save_stock_data_to_s3(self, stock_code: str, disclosures: List[Dict]) -> bool:
        """銘柄別データをS3に保存"""
        try:
            sorted_disclosures = sorted(disclosures, key=lambda x: x.get('date_normalized', ''))
            summary = self.create_stock_summary(stock_code, sorted_disclosures)

            stock_data = {
                'metadata': {
                    'stock_code': stock_code,
                    'created_at': datetime.now().isoformat(),
                    'data_version': '1.0',
                    'source': '株探5年分データ'
                },
                'summary': summary,
                'disclosures': sorted_disclosures
            }

            key = f"{self.output_prefix}{stock_code}.json"
            json_data = json.dumps(stock_data, ensure_ascii=False, indent=2)

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )

            file_size_kb = len(json_data) / 1024
            self.logger.info(f"保存完了: {stock_code} ({len(sorted_disclosures)}件, {file_size_kb:.1f}KB)")
            self.stats['created_files'] += 1

            return True

        except Exception as e:
            self.logger.error(f"保存エラー ({stock_code}): {e}")
            self.stats['errors'] += 1
            return False

    def create_master_index(self, stock_data: Dict[str, List[Dict]]):
        """銘柄インデックスファイルの作成"""
        try:
            index_data = {
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'total_stocks': len(stock_data),
                    'total_disclosures': sum(len(disclosures) for disclosures in stock_data.values()),
                    'data_source': '株探5年分データ'
                },
                'stocks': {}
            }

            for stock_code, disclosures in stock_data.items():
                if disclosures:
                    latest = max(disclosures, key=lambda x: x.get('date_normalized', ''), default={})
                    date_range = self.get_date_range(disclosures)

                    index_data['stocks'][stock_code] = {
                        'company_name': latest.get('company_name_cleaned') or latest.get('company_name', '不明'),
                        'total_disclosures': len(disclosures),
                        'date_range': date_range,
                        'file_path': f"{self.output_prefix}{stock_code}.json"
                    }

            index_key = f"{self.output_prefix}index.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=json.dumps(index_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json'
            )

            self.logger.info(f"インデックスファイル作成完了: {index_key}")

        except Exception as e:
            self.logger.error(f"インデックスファイル作成エラー: {e}")

    def process_stocks_in_batches(self, stock_data: Dict[str, List[Dict]]):
        """銘柄データをバッチ処理"""
        stock_codes = list(stock_data.keys())
        total_stocks = len(stock_codes)

        self.logger.info(f"銘柄データ処理開始: {total_stocks}銘柄")

        for i, stock_code in enumerate(stock_codes, 1):
            try:
                self.logger.info(f"[{i}/{total_stocks}] 処理中: {stock_code}")
                success = self.save_stock_data_to_s3(stock_code, stock_data[stock_code])

                if not success:
                    self.logger.warning(f"保存失敗: {stock_code}")

                if i % 100 == 0:
                    elapsed = time.time() - self.stats['start_time']
                    progress = i / total_stocks * 100
                    self.logger.info(f"進捗: {progress:.1f}% ({i}/{total_stocks}) - 経過時間: {elapsed/60:.1f}分")

                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"処理エラー ({stock_code}): {e}")
                self.stats['errors'] += 1

        self.logger.info("銘柄データ処理完了")

    def run_stock_based_organization(self):
        """銘柄別データ整理の実行（当月と前月のみ）"""
        print("=" * 80)
        print("株探データ銘柄別整理システム（日次版）")
        print("=" * 80)

        self.stats['start_time'] = time.time()

        try:
            # 対象月を決定
            target_months = self.get_target_months()
            print(f"対象月: {target_months[0][0]}年{target_months[0][1]}月 と {target_months[1][0]}年{target_months[1][1]}月")

            # 対象月のデータのみ読み込み
            self.logger.info("Step 1: 対象月データ読み込み開始")
            stock_data = self.load_target_monthly_data(target_months)

            if not stock_data:
                self.logger.error("データが読み込めませんでした")
                return False

            # 銘柄別ファイル作成
            self.logger.info("Step 2: 銘柄別ファイル作成開始")
            self.process_stocks_in_batches(stock_data)

            # インデックス作成
            self.logger.info("Step 3: マスターインデックス作成")
            self.create_master_index(stock_data)

            # 完了レポート
            elapsed_time = time.time() - self.stats['start_time']

            print("\n" + "=" * 80)
            print("銘柄別データ整理完了レポート")
            print("=" * 80)
            print(f"処理時間: {elapsed_time/60:.1f}分")
            print(f"処理月数: {self.stats['processed_months']}")
            print(f"対象銘柄数: {self.stats['unique_stocks']:,}")
            print(f"総開示件数: {self.stats['total_disclosures']:,}")
            print(f"作成ファイル数: {self.stats['created_files']:,}")
            print(f"エラー件数: {self.stats['errors']}")

            if self.stats['created_files'] > 0:
                print(f"平均処理時間/銘柄: {elapsed_time/self.stats['created_files']:.2f}秒")

            print(f"\n保存先: s3://{self.bucket_name}/{self.output_prefix}")
            print("=" * 80)

            return True

        except Exception as e:
            self.logger.error(f"実行エラー: {e}")
            return False

def notify_slack(status, message):
    """Slackに通知を送る"""
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not slack_webhook_url:
        print("警告: SLACK_WEBHOOK_URLが設定されていません")
        return
    
    color = "good" if status == "success" else "danger"
    emoji = "✅" if status == "success" else "❌"
    
    payload = {
        "attachments": [{
            "color": color,
            "title": f"{emoji} 銘柄別データ整理",
            "text": message,
            "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    }
    
    try:
        requests.post(slack_webhook_url, json=payload)
    except Exception as e:
        print(f"Slack通知エラー: {e}")


def main():
    """メイン実行関数"""
    try:
        organizer = StockBasedDataOrganizer()
        success = organizer.run_stock_based_organization()
        
        if success:
            print("銘柄別データ整理が正常に完了しました")
            notify_slack("success", "銘柄別データ整理が正常に完了しました")
        else:
            print("処理中にエラーが発生しました")
            notify_slack("failure", "処理中にエラーが発生しました")
    except Exception as e:
        print(f"実行エラー: {e}")
        notify_slack("failure", f"実行エラー: {str(e)}")

if __name__ == "__main__":
    main()
